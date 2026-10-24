package com.test.aiserving.was.messaging;

import java.time.Instant;

/**
 * Kafka 로 보내는 에이전트 요청 메시지. Day 13 의 Python Producer 와 같은 스키마다 (docs/DAY13_GUIDE.md 3장).
 *
 * <pre>
 * {"requestId":"UUID","question":"...","provider":"gemini","createdAt":"2026-10-10T12:00:00Z","sessionId":"abc" 또는 null}
 * </pre>
 *
 * 필드를 바꾸면 FastAPI Consumer 도 같이 고친다.
 * sessionId(Day 20)는 대화 이어가기용이며 없으면 null 이다. 맨 끝에 붙여서 앞의 4개 필드 순서는 그대로다.
 * JSON 변환은 직접 한다. Spring Boot 4 의 JSON 라이브러리(Jackson 3)와 이전 버전의 패키지가 달라서,
 * 확인하기 전에는 특정 JSON 라이브러리에 의존하지 않기로 했다. 필드가 4개뿐이라 단순하다.
 */
public record AgentRequestMessage(String requestId, String question, String provider, Instant createdAt, String sessionId) {

	public String toJson() {
		return "{"
				+ "\"requestId\":" + quote(requestId) + ","
				+ "\"question\":" + quote(question) + ","
				+ "\"provider\":" + quote(provider) + ","
				+ "\"createdAt\":" + quote(createdAt.toString()) + ","
				+ "\"sessionId\":" + (sessionId == null ? "null" : quote(sessionId))
				+ "}";
	}

	/** JSON 문자열 리터럴로 변환한다. 따옴표, 역슬래시, 제어 문자를 이스케이프하고 한글은 그대로 둔다(UTF-8). */
	// Day 26: 동기 프록시(was/sync)가 FastAPI 로 보낼 JSON 을 만들 때도 쓰려고 public 으로 연다.
	public static String quote(String value) {
		StringBuilder sb = new StringBuilder(value.length() + 2);
		sb.append('"');
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			switch (c) {
				case '"' -> sb.append("\\\"");
				case '\\' -> sb.append("\\\\");
				case '\n' -> sb.append("\\n");
				case '\r' -> sb.append("\\r");
				case '\t' -> sb.append("\\t");
				case '\b' -> sb.append("\\b");
				case '\f' -> sb.append("\\f");
				default -> {
					if (c < 0x20) {
						sb.append(String.format("\\u%04x", (int) c));
					} else {
						sb.append(c);
					}
				}
			}
		}
		sb.append('"');
		return sb.toString();
	}
}
