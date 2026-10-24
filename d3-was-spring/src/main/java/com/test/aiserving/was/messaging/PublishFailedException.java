package com.test.aiserving.was.messaging;

/** Kafka 에 메시지를 넣지 못했을 때(브로커 불가, 시간 초과 등). 컨트롤러가 503 으로 바꾼다. */
public class PublishFailedException extends RuntimeException {

	public PublishFailedException(String message, Throwable cause) {
		super(message, cause);
	}
}
