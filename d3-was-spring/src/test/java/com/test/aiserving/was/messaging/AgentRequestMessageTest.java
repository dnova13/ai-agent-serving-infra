package com.test.aiserving.was.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;

import org.junit.jupiter.api.Test;

/**
 * AgentRequestMessage 의 JSON 변환만 검증하는 단위 테스트.
 * 스프링, Kafka 를 띄우지 않고 순수 자바로 실행되므로 매우 빠르다(수 ms).
 * 확인하는 것: Kafka 로 들어갈 JSON 문자열이 Consumer 가 기대하는 모양(Day 13 스키마)인가.
 * 확인하지 않는 것: 실제 Kafka 전송, HTTP 요청 처리.
 */
class AgentRequestMessageTest {

	/**
	 * 케이스 1: 메시지 전체를 JSON 으로 바꿨을 때
	 * (a) 필드 순서가 requestId, question, provider, createdAt 인지
	 * (b) 따옴표(")와 줄바꿈(\n)이 들어 있는 질문이 깨지지 않고 이스케이프되는지
	 * (c) 한글은 유니코드 이스케이프(역슬래시+u+4자리)로 바뀌지 않고 그대로 남는지
	 * (d) 시각이 ISO-8601 문자열("...Z")로 나오는지
	 */
	// @Test: JUnit 이 이 메서드를 테스트로 인식해 실행한다. (없으면 그냥 메서드라 실행되지 않음)
	// 이름 규칙: "대상메서드_확인할내용". toJson() 의 "필드 순서"와 "이스케이프"를 본다는 뜻.
	//   한글 메서드명은 자바에서 허용되고, 실패 시 리포트에 이름이 그대로 떠서 무엇이 깨졌는지 바로 보인다.
	// void + public 아님: JUnit 5 는 테스트 메서드가 반환값 없이 같은 패키지에서 보이기만 하면 된다.
	//   예외 없이 끝까지 가면 통과, assertEquals 가 어긋나면 AssertionFailedError 로 실패.
	@Test
	void toJson_필드_순서와_이스케이프() {
		// 준비: 일부러 따옴표와 줄바꿈을 섞은 질문을 넣는다. (자바 문자열 리터럴이라 \" 와 \n 으로 적음)
		AgentRequestMessage message = new AgentRequestMessage(
				"id-1",
				"그는 \"안녕\"\n이라고 했다",
				"gemini",
				Instant.parse("2026-10-10T12:00:00Z"),
				null);  // Day 20: sessionId 없음

		System.out.println(message.toJson());

		// 기대값: 아래 문자열도 자바 리터럴이라 역슬래시가 한 번 더 붙어 있다.
		// 실제 JSON 으로 풀어 쓰면 다음과 같다.
		// {"requestId":"id-1","question":"그는 \"안녕\"\n이라고 했다","provider":"gemini","createdAt":"2026-10-10T12:00:00Z"}
		assertEquals(
				"{\"requestId\":\"id-1\",\"question\":\"그는 \\\"안녕\\\"\\n이라고 했다\","
						+ "\"provider\":\"gemini\",\"createdAt\":\"2026-10-10T12:00:00Z\",\"sessionId\":null}",
				message.toJson());
	}

	/**
	 * 케이스 3 (Day 20): sessionId 가 있으면 맨 끝에 문자열로 들어가는지.
	 */
	@Test
	void toJson_sessionId가_있으면_끝에_문자열로() {
		AgentRequestMessage message = new AgentRequestMessage(
				"id-2", "질문", "gemini", Instant.parse("2026-10-10T12:00:00Z"), "user-1");

		System.out.println(message.toJson());

		assertEquals(
				"{\"requestId\":\"id-2\",\"question\":\"질문\",\"provider\":\"gemini\","
						+ "\"createdAt\":\"2026-10-10T12:00:00Z\",\"sessionId\":\"user-1\"}",
				message.toJson());
	}

	/**
	 * 케이스 2: 눈에 보이지 않는 제어 문자(코드 0x01)가 역슬래시+u0001 형태로 이스케이프되는지.
	 * (주의: 자바는 주석 안의 역슬래시+u 도 해석하므로 주석에는 그 형태를 그대로 쓰지 않는다.)
	 * JSON 규격상 0x20 미만 문자는 그대로 넣으면 안 되므로, 빼먹으면 Consumer 의 JSON 파싱이 실패할 수 있다.
	 * quote() 는 같은 패키지에서만 보이는(package-private) 메서드라 테스트가 직접 호출한다.
	 */
	@Test
	void quote_제어문자는_유니코드_이스케이프() {
		// 입력: a, 제어문자 0x01, b  →  기대: 앞뒤 따옴표 포함, a 와 b 사이에 역슬래시+u0001 이 글자 그대로 들어간 문자열
		assertEquals("\"a\\u0001b\"", AgentRequestMessage.quote("a\u0001b"));
	}
}
