package com.test.aiserving.was.messaging;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * 요청 메시지를 agent.requests 토픽에 넣는다.
 *
 * 브로커가 저장했다고 응답할 때까지(acks=all) 짧게 기다린 뒤에 반환한다. 그래야 "202 접수"가
 * 실제로 Kafka 에 들어갔다는 뜻이 된다. 이 대기는 LLM 호출(수 초~수십 초)과 달리 보통 수 ms 이다.
 * 대기 없이 바로 응답하는 방식과의 비교는 k6 실험(Day 25~)에서 다룰 수 있다.
 */
@Component
public class AgentRequestProducer {

	private static final Logger log = LoggerFactory.getLogger(AgentRequestProducer.class);

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topic;
	private final long sendTimeoutMs;

	public AgentRequestProducer(
			KafkaTemplate<String, String> kafkaTemplate,
			@Value("${app.kafka.topics.requests}") String topic,
			@Value("${app.kafka.send-timeout-ms:5000}") long sendTimeoutMs) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
		this.sendTimeoutMs = sendTimeoutMs;
	}

	// 주의: KafkaTemplate.send() 는 Future 를 돌려주기 전에 브로커 메타데이터를 받으려고 최대 max.block.ms(기본 60초) 동안
	// 호출 스레드를 막는다. 아래 get(sendTimeoutMs) 만으로는 이 대기를 줄일 수 없어서 application.properties 에서
	// max.block.ms 도 같이 줄였다.
	/** key 는 requestId. 파티션이 여러 개가 되면 같은 요청 id 는 같은 파티션으로 간다. */
	public SendResult<String, String> publish(AgentRequestMessage message) {
		try {
			SendResult<String, String> result = kafkaTemplate
					.send(topic, message.requestId(), message.toJson())
					.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
			log.info("published requestId={} partition={} offset={}",
					message.requestId(),
					result.getRecordMetadata().partition(),
					result.getRecordMetadata().offset());
			return result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new PublishFailedException("Kafka 전송 중 인터럽트", e);
		} catch (ExecutionException | TimeoutException e) {
			throw new PublishFailedException("Kafka 전송 실패 (topic=" + topic + ")", e);
		// send() 가 던지는 RuntimeException 도 같은 실패로 취급해 503 으로 돌려준다.
		//       (PublishFailedException 자신은 위에서 던지는 게 아니므로 여기서 다시 잡히지 않는다.)
		} catch (RuntimeException e) {
			throw new PublishFailedException("Kafka 전송 실패 (topic=" + topic + ")", e);
		}
	}
}
