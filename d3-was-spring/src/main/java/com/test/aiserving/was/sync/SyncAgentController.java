package com.test.aiserving.was.sync;

import com.test.aiserving.was.api.AgentRequestBody;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 동기 구조(Day 26 실험용): 답이 나올 때까지 이 요청을 붙잡고 있다가 그대로 돌려준다.
 * 기존 POST /api/agent/requests(Kafka 에 넣고 202)와 같은 본문 {question, provider} 을 받는다.
 * 응답: FastAPI 가 만든 답(200), 시간 초과(504), FastAPI 접속 실패(502). 자세한 코드는 SyncAgentClient.
 */
@RestController
@RequestMapping("/api/agent")
public class SyncAgentController {

	private static final String DEFAULT_PROVIDER = "gemini";

	private final SyncAgentClient client;

	public SyncAgentController(SyncAgentClient client) {
		this.client = client;
	}

	@PostMapping("/sync")
	public ResponseEntity<String> sync(@Valid @RequestBody AgentRequestBody body) {
		String provider = (body.provider() == null || body.provider().isBlank())
				? DEFAULT_PROVIDER
				: body.provider();
		SyncAgentClient.Result result = client.call(body.question(), provider);
		return ResponseEntity.status(result.status())
				.contentType(MediaType.APPLICATION_JSON)
				.body(result.body());
	}
}
