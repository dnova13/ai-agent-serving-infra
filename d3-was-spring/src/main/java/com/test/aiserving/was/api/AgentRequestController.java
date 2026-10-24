package com.test.aiserving.was.api;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import com.test.aiserving.was.messaging.AgentRequestMessage;
import com.test.aiserving.was.messaging.AgentRequestProducer;
import com.test.aiserving.was.messaging.PublishFailedException;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 에이전트 요청 접수. LLM 을 기다리지 않고 Kafka 에 넣은 뒤 202 Accepted 로 바로 응답한다.
 * 처리 결과 조회는 Day 17 이후에 requestId 로 한다.
 */
@RestController
@RequestMapping("/api/agent")
public class AgentRequestController {

	private static final Logger log = LoggerFactory.getLogger(AgentRequestController.class);
	private static final String DEFAULT_PROVIDER = "gemini";

	private final AgentRequestProducer producer;

	public AgentRequestController(AgentRequestProducer producer) {
		this.producer = producer;
	}

	@PostMapping("/requests")
	public ResponseEntity<Map<String, Object>> accept(@Valid @RequestBody AgentRequestBody body) {
		// 흐름(Day 20): ① requestId 발급 → ② provider 기본값 → ③ sessionId 정리 후 메시지 만들기 → ④ Kafka 에 넣기 → ⑤ 202 응답
		// sessionId 는 FastAPI 에서 이전 대화를 찾는 키로만 쓰이고, 여기서는 검증(길이)과 정리(빈 값 → null)만 한다
		String requestId = UUID.randomUUID().toString();  // ① 요청 하나를 가리키는 id (결과 조회용). sessionId 와 다르다
		// ② provider 를 안 보내면 gemini
		String provider = (body.provider() == null || body.provider().isBlank())
				? DEFAULT_PROVIDER
				: body.provider();
		// Day 20: 비어 있는 sessionId 는 "없음"(null) 으로 본다
		String sessionId = (body.sessionId() == null || body.sessionId().isBlank()) ? null : body.sessionId();
		AgentRequestMessage message = new AgentRequestMessage(requestId, body.question(), provider, Instant.now(), sessionId);  // ③ 메시지 JSON 맨 끝에 sessionId 가 실린다

		try {
			producer.publish(message);  // ④ Kafka 토픽 agent.requests 로 전송 (key=requestId). LLM 처리를 기다리지 않는다
		} catch (PublishFailedException e) {
			log.error("접수 실패 requestId={}", requestId, e);
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
					"requestId", requestId,
					"status", "REJECTED",
					"error", "요청을 큐에 넣지 못했습니다. 잠시 후 다시 시도해 주세요."));
		}

		// ⑤ 처리 결과가 아니라 "접수됐다"만 바로 돌려준다 (결과는 requestId 로 FastAPI 에서 조회)
		return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
				"requestId", requestId,
				"status", "ACCEPTED"));
	}
}
