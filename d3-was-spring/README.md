# d3-was-spring — Spring WAS

클라이언트 요청을 받아 Kafka 에 넣고 바로 `202 Accepted` 로 응답하는 Spring Boot 서버다. LLM 처리는 기다리지 않고 FastAPI 워커([d2-ai-python](../d2-ai-python/README.md))가 맡는다. 프로젝트 전체 설명은 루트 [README.md](../README.md) 를 본다.

## 사용하는 것

| 프레임워크·도구 | 하는 일 |
| --- | --- |
| Spring Boot 4.1 (Java 21, Gradle) | 서버 본체 |
| Spring Web MVC | REST 엔드포인트 |
| Spring for Apache Kafka | 요청을 `agent.requests` 토픽에 넣는 Producer |
| Bean Validation | 요청 본문 검사(질문 비어 있음, 길이 제한) |
| Actuator + Micrometer Prometheus | `/actuator/prometheus` 로 지표를 연다 |

## 폴더

```
src/main/java/com/test/aiserving/was/
├── WasApplication.java
├── api/         AgentRequestController(POST /api/agent/requests), AgentRequestBody
├── messaging/   AgentRequestProducer, AgentRequestMessage, PublishFailedException
├── config/      KafkaTopicConfig(토픽과 파티션 수를 만든다)
└── sync/        SyncAgentController, SyncAgentClient(비동기와 비교하려고 만든 동기 경로)
src/main/resources/application.properties
src/test/java/...   AgentRequestMessageTest, WasApplicationTests
```

## 실행

먼저 Kafka 가 떠 있어야 한다(루트 `docker-compose.yml`).

```bash
cd d3-was-spring
./gradlew bootRun        # 인자 없이. 포트 8080
./gradlew test           # 테스트
```

## 엔드포인트

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| POST | `/api/agent/requests` | 요청을 Kafka 에 넣고 `202 {requestId, status: ACCEPTED}` 를 돌려준다. Kafka 에 못 넣으면 `503 REJECTED` |
| POST | `/api/agent/sync` | FastAPI `/api/v1/agent/run` 을 기다렸다가 결과를 그대로 돌려준다(동기 비교 실험). 응답이 없으면 `504`, 연결 실패는 `502` |
| GET | `/actuator/health` · `/actuator/prometheus` | 상태, Prometheus 지표 |

요청 본문(`POST /api/agent/requests`):

```json
{ "question": "환불은 며칠 안에 신청해야 하나요?", "provider": "gemini", "sessionId": "user-1" }
```

- `question` 필수, 2000자 이하. `provider` 를 생략하면 `gemini`. `sessionId` 가 같으면 이전 대화를 이어간다(100자 이하, 생략하면 새 대화).
- 결과는 이 서버가 아니라 FastAPI 에서 `GET /api/v1/agent/results/{requestId}` 로 조회한다.

## 설정(`application.properties`)

| 키 | 값 | 뜻 |
| --- | --- | --- |
| `server.port` | `8080` | |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka 주소 |
| `spring.kafka.producer.acks` | `all` | 브로커 확인까지 기다림 |
| `spring.kafka.producer.properties.max.block.ms` / `app.kafka.send-timeout-ms` | `5000` | Kafka 가 꺼져 있을 때 기다리는 최대 시간 |
| `app.kafka.topics.requests` | `agent.requests` | 토픽 이름 |
| `app.kafka.topics.requests-partitions` | `3` | 파티션 수 |
| `app.sync.fastapi-url` | `http://localhost:8000` | 동기 경로가 부르는 FastAPI |
| `app.sync.timeout-ms` / `connect-timeout-ms` | `30000` / `2000` | 동기 경로의 응답, 연결 제한 시간 |
