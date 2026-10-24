package com.test.aiserving.was.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 요청 토픽을 애플리케이션 시작 시 보장한다.
 * 이미 있는 토픽이면 그대로 두고, 파티션 수만 설정값이 더 크면 늘린다 (Day 18, 34 에서 파티션 수를 바꿀 때 사용).
 * Day 13 에서 Python Producer 가 자동 생성한 agent.requests (파티션 1개)와 같은 토픽이다.
 */
@Configuration
public class KafkaTopicConfig {

	@Bean
	public NewTopic agentRequestsTopic(
			@Value("${app.kafka.topics.requests}") String name,
			@Value("${app.kafka.topics.requests-partitions:1}") int partitions) {
		return TopicBuilder.name(name)
				.partitions(partitions)
				.replicas(1) // 단일 브로커
				.build();
	}
}
