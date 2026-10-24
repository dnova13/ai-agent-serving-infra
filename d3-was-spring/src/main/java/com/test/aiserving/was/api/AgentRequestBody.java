package com.test.aiserving.was.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** POST /api/agent/requests 요청 본문. provider 를 생략하면 gemini 로 처리한다. sessionId 가 같으면 이전 대화를 이어간다(생략하면 새 대화, Day 20). */
public record AgentRequestBody(
		@NotBlank(message = "question 은 비어 있을 수 없습니다")
		@Size(max = 2000, message = "question 은 2000자 이하여야 합니다")
		String question,

		@Size(max = 50, message = "provider 는 50자 이하여야 합니다")
		String provider,

		// Day 20: 대화 이어가기용 세션 식별자. 클라이언트가 정한 값을 그대로 쓴다.
		@Size(max = 100, message = "sessionId 는 100자 이하여야 합니다")
		String sessionId) {
}
