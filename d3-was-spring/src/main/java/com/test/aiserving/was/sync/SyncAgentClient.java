package com.test.aiserving.was.sync;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.test.aiserving.was.messaging.AgentRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 동기 구조(Day 26 실험용): FastAPI 의 POST /api/v1/agent/run 을 호출하고 답이 올 때까지 기다린다.
 *
 * Kafka 를 거치는 기존 경로(AgentRequestProducer)와 달리, 이 요청을 처리하는 Tomcat 스레드가 LLM 응답이 올 때까지 묶인다.
 * 그래서 요청이 몰리면 (1) FastAPI 가 못 따라가 응답이 늦어지고 (2) 기다리는 시간이 timeout-ms 를 넘으면 504, (3) 스레드가 모자라 접속 자체가 밀린다.
 * 이 클래스는 그 "기다리다 포기하는" 지점을 만든다. 실험이 끝나면 쓰지 않는다(기존 비동기 경로는 그대로 둔다).
 *
 * 결과 코드
 *   FastAPI 가 응답하면 그 상태 코드와 본문을 그대로 돌려준다(200 이면 {"answer", "provider"}).
 *   timeout-ms 안에 응답이 없으면 504, FastAPI 에 접속하지 못하면 502 (본문은 {"status","error"}).
 * JSON 라이브러리를 쓰지 않고 문자열로 만든다(AgentRequestMessage 와 같은 이유).
 */
@Component
public class SyncAgentClient {

	/** 호출 결과. body 는 JSON 문자열이다. */
	public record Result(int status, String body) {
	}

	private static final Logger log = LoggerFactory.getLogger(SyncAgentClient.class);

	private final HttpClient http;
	private final URI uri;
	private final Duration timeout;

	public SyncAgentClient(
			@Value("${app.sync.fastapi-url:http://localhost:8000}") String baseUrl,
			@Value("${app.sync.timeout-ms:30000}") long timeoutMs,
			@Value("${app.sync.connect-timeout-ms:2000}") long connectTimeoutMs) {
		String base = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
		this.uri = URI.create(base + "/api/v1/agent/run");
		this.timeout = Duration.ofMillis(timeoutMs);
		// uvicorn 은 HTTP/2 업그레이드(h2c)를 받지 않으므로 처음부터 HTTP/1.1 로 보낸다.
		this.http = HttpClient.newBuilder()
				.version(HttpClient.Version.HTTP_1_1)
				.connectTimeout(Duration.ofMillis(connectTimeoutMs))
				.build();
	}

	public Result call(String question, String provider) {
		String json = "{\"query\":" + AgentRequestMessage.quote(question)
				+ ",\"provider\":" + AgentRequestMessage.quote(provider) + "}";
		HttpRequest request = HttpRequest.newBuilder(uri)
				.timeout(timeout)
				.header("Content-Type", "application/json; charset=UTF-8")
				.POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
				.build();

		long start = System.nanoTime();
		try {
			HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
			log.info("sync upstream status={} elapsedMs={}", response.statusCode(), elapsedMs(start));
			return new Result(response.statusCode(), response.body());
		} catch (HttpConnectTimeoutException e) {
			// HttpConnectTimeoutException 은 HttpTimeoutException 의 하위 클래스라 먼저 잡는다(접속 실패는 502).
			log.warn("sync upstream connect timeout elapsedMs={}", elapsedMs(start));
			return failure(502, "BAD_GATEWAY", "FastAPI 접속 시간 초과");
		} catch (HttpTimeoutException e) {
			log.warn("sync upstream timeout elapsedMs={}", elapsedMs(start));
			return failure(504, "GATEWAY_TIMEOUT", timeout.toMillis() + "ms 안에 FastAPI 응답이 없음");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return failure(502, "BAD_GATEWAY", "FastAPI 호출 중 인터럽트");
		} catch (IOException e) {
			log.warn("sync upstream failed elapsedMs={} ({}: {})", elapsedMs(start), e.getClass().getSimpleName(), e.getMessage());
			return failure(502, "BAD_GATEWAY", "FastAPI 접속 실패: " + e.getClass().getSimpleName());
		}
	}

	private static long elapsedMs(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

	private static Result failure(int status, String code, String message) {
		return new Result(status, "{\"status\":" + AgentRequestMessage.quote(code)
				+ ",\"error\":" + AgentRequestMessage.quote(message) + "}");
	}
}
