# Day 14 가이드 — 메인 WAS(Spring Boot)에 Kafka Producer 연동

> 상태: 완료(2026-10-10), 실행 결과는 [DAY14_RESULT.md](DAY14_RESULT.md). 아래는 작성 당시 안내다(작성 당시에는 컴파일과 실행을 하지 못했다). 프로젝트 뼈대(`build.gradle`, wrapper, 시작 클래스)는 사용자가 Initializr 에서 직접 받은 zip 이고, 그 위에 소스를 얹었다. 작성 환경에서 Gradle, Java 21, Maven Central 접속이 안 되어 빌드해 보지 못했다. 실행 결과는 실행 후 `DAY14_RESULT.md` 에 적는다.

## 1. 오늘 목표

클라이언트 요청을 받아 Kafka 토픽에 넣고 `202 Accepted` 로 바로 응답하는 WAS 를 만든다. Day 13 에서 확인한 토픽(`agent.requests`)과 메시지 스키마를 그대로 쓴다. 에이전트 호출은 하지 않는다(Day 15~16, FastAPI Consumer).

```
[클라이언트] --POST /api/agent/requests--> [Spring WAS] --send(acks=all)--> [Kafka agent.requests]
                  <-- 202 {requestId} ----       (LLM 을 기다리지 않음)
```

## 2. 확정한 조건

| 항목 | 값 |
| --- | --- |
| 빌드 | Gradle (Groovy DSL) |
| 자바 | 21 (toolchain) |
| Spring Boot | **4.1.1** (정식. Initializr 의 `4.1.2 (SNAPSHOT)` 은 개발 중 빌드라 쓰지 않음) |
| 프로젝트 위치 | `ai-agent-serving-infra/d3-was-spring` |
| Group / Artifact | `com.test.aiserving` / `was` (패키지 `com.test.aiserving.was`) |
| 설정 파일 | `application.properties` (Initializr 기본 형식 유지) |
| 포트 | 8080 (kafka-ui 는 8090) |

## 3. 만든 파일

| 파일 | 역할 |
| --- | --- |
| `build.gradle`, `settings.gradle`, `gradlew`, `gradle/wrapper/` | Initializr 가 생성(Gradle 9.7.1 wrapper). 의존성은 webmvc, kafka, validation 세 starter + 각 test starter |
| `src/main/resources/application.properties` | Kafka 주소(`localhost:9092`), 토픽 이름, 전송 대기 시간 |
| `WasApplication.java` | 시작 클래스 (Initializr 생성) |
| `config/KafkaTopicConfig.java` | 시작 시 `agent.requests` 토픽을 보장. 파티션 수는 `app.kafka.topics.requests-partitions` |
| `api/AgentRequestBody.java` | 요청 본문(`question`, `provider`)과 검증 |
| `api/AgentRequestController.java` | `POST /api/agent/requests`. 접수하면 202, Kafka 실패면 503 |
| `messaging/AgentRequestMessage.java` | Kafka 로 보내는 메시지와 JSON 변환 |
| `messaging/AgentRequestProducer.java` | `KafkaTemplate` 으로 전송하고 브로커 응답까지 짧게 대기 |
| `messaging/PublishFailedException.java` | 전송 실패 예외 |
| `src/test/.../messaging/AgentRequestMessageTest.java` | JSON 변환 단위 테스트 |
| `src/test/.../WasApplicationTests.java` | Initializr 생성 `contextLoads`. 앱 전체를 띄우므로 Kafka 가 떠 있어야 하고 느릴 수 있음 |

## 4. API

### 요청

```
POST /api/agent/requests
Content-Type: application/json

{"question": "단순 변심으로 환불하면 배송비는 누가 내요?", "provider": "gemini"}
```

| 필드 | 필수 | 설명 |
| --- | --- | --- |
| `question` | 필수 | 비어 있으면 400. 2000자 이하 |
| `provider` | 선택 | 생략하면 `gemini`. 50자 이하 |

### 응답

| 상황 | 상태 | 본문 |
| --- | --- | --- |
| 접수 | 202 | `{"requestId": "<UUID>", "status": "ACCEPTED"}` |
| `question` 이 비어 있음 | 400 | 검증 오류 (Spring 기본 오류 응답) |
| Kafka 에 넣지 못함 | 503 | `{"requestId": "...", "status": "REJECTED", "error": "..."}` |

### Kafka 로 들어가는 메시지 (Day 13 스키마와 동일)

```json
{"requestId":"UUID","question":"...","provider":"gemini","createdAt":"2026-10-10T12:00:00Z"}
```

key 는 `requestId` 문자열, 토픽은 `agent.requests`.

## 5. 설계 판단

| 항목 | 선택 | 이유와 대가 |
| --- | --- | --- |
| 202 를 언제 돌려주나 | 브로커 저장 확인(`acks=all`) 후 | 202 가 "실제로 Kafka 에 들어갔다"를 뜻하게 하려고. 요청 스레드가 보통 수 ms 대기. 대기 없이 바로 응답하는 방식(fire-and-forget)은 유실 가능성이 있어 택하지 않음 |
| 전송 실패 시 | 503 | 접수하지 못했다는 것을 클라이언트가 알 수 있어야 하므로 |
| JSON 변환 | 직접 구현(필드 4개) | Spring Boot 4 의 JSON 라이브러리(Jackson 3)와 이전 버전의 패키지가 달라서, 확인 전에는 특정 라이브러리에 의존하지 않기로 함. 필드가 늘면 라이브러리로 교체 |
| key | `requestId` | 파티션이 여러 개가 되면 같은 요청 id 는 같은 파티션. 순서가 중요해지면 `userId` 같은 값을 고려 (Day 16~17 에서 결정) |
| 토픽 보장 | `NewTopic` 빈 | Day 13 의 자동 생성(파티션 1개)과 같은 이름. 파티션 수 변경 실험은 설정값만 바꾸면 됨 |

## 6. 빌드 준비

| 항목 | 상태 |
| --- | --- |
| Gradle wrapper | zip 에 포함되어 있음 (`gradlew`, `gradle/wrapper`). 별도 작업 없음 |
| 의존성 이름 | zip 의 `build.gradle` 에 이미 `spring-boot-starter-webmvc`, `-kafka`, `-validation` 이 들어 있어 확인됨. 소스는 이 이름을 그대로 따른다 |
| 컴파일 | **미확인.** import(`KafkaTemplate`, `NewTopic`, `TopicBuilder`, 검증 어노테이션)가 Boot 4.1.1 에서 맞는지는 첫 빌드에서 알 수 있다 |
| 실행 권한 | `gradlew` 에 실행 권한이 없다는 오류가 나면 `chmod +x gradlew` |

## 7. 실행과 확인

터미널 위치는 각 줄에 적었다. Kafka 컨테이너가 떠 있어야 한다(`ai-agent-serving-infra` 에서 `docker compose up -d kafka kafka-ui`).

### ① 단위 테스트 (`d3-was-spring`)

```bash
./gradlew test
```

기대: `AgentRequestMessageTest` 2개 통과. `WasApplicationTests` 는 앱 전체를 띄우므로 Kafka(`localhost:9092`)가 떠 있어야 한다. Kafka 를 켜지 않았을 때 실패/지연하면 그 사실을 결과에 그대로 기록한다.

### ② WAS 실행 (`d3-was-spring`)

```bash
./gradlew bootRun
```

기대: 로그에 Kafka 접속과 `Started WasApplication` 이 나오고 8080 에서 대기. 토픽 `agent.requests` 는 이미 있으므로 새로 만들어지지 않는다.

### ③ 요청 보내기 (다른 터미널)

```bash
curl -i -X POST http://localhost:8080/api/agent/requests \
  -H 'Content-Type: application/json' \
  -d '{"question":"단순 변심으로 환불하면 배송비는 누가 내요?","provider":"gemini"}'
```

| 기대 | 의미 |
| --- | --- |
| `HTTP/1.1 202` 와 `{"requestId":"...","status":"ACCEPTED"}` | 접수 완료 |
| WAS 로그 `published requestId=... partition=0 offset=...` | Kafka 에 저장됨 |

### ④ 검증 오류 확인

```bash
curl -i -X POST http://localhost:8080/api/agent/requests \
  -H 'Content-Type: application/json' \
  -d '{"question":""}'
```

기대: `400`. Kafka 에는 아무것도 들어가지 않는다.

### ⑤ Kafka 에 들어갔는지 확인 (`d2-ai-python`, 가상환경 켠 상태)

```bash
python -m app.checks.kafka_consumer_test --group day14 --max 100 --timeout 10
```

기대: 새 그룹이라 토픽의 처음부터 읽는다. Day 13 의 테스트 메시지들 사이에서, 마지막에 방금 보낸 메시지(`provider=gemini`, 질문 내용이 위 curl 의 것)가 나온다. kafka-ui(`http://localhost:8090`) Topics → Messages 에서도 같은 requestId 로 볼 수 있다.

### ⑥ Kafka 중단 상태에서 503 확인 (선택)

**순서 주의:** Kafka 를 켠 상태에서 WAS 를 먼저 기동하고, 기동이 끝난 뒤에 `docker compose stop kafka` 한다(Kafka 가 꺼진 채로 WAS 를 기동하면 시작 단계의 토픽 확인(`NewTopic`)이 브로커에 접속하려고 재시도하며 기동이 끝나지 않는 것을 실행 중에 확인했다. 대기 시간은 재지 않았다). 그 뒤 ③을 다시 보낸다. 기대: 약 5초 뒤 `503`, `status: REJECTED`. 확인이 끝나면 `docker compose start kafka`.

**수정 이력 (실행하며 발견):** 처음에는 `send(...).get(5000ms)` 만 믿고 "5초 뒤 503" 이라고 적었다. 그런데 `KafkaTemplate.send()` 는 Future 를 돌려주기 전에 브로커 메타데이터를 받으려고 `max.block.ms`(기본 60초, 실행 로그의 ProducerConfig 에서 확인) 동안 호출 스레드를 막는다. Kafka 를 끄고 curl 을 보냈을 때 응답이 5초를 훌쩍 넘겨 오래 걸렸다. 그래서 두 가지를 고쳤다. (1) `spring.kafka.producer.properties.max.block.ms=5000`, (2) `send()` 가 직접 던지는 `RuntimeException` 도 `PublishFailedException` 으로 감싸 503 으로 응답. 수정 후 결과는 `DAY14_RESULT.md` 에 적는다.

## 8. 알려진 한계와 주의

| 항목 | 내용 |
| --- | --- |
| 컴파일 미확인 | 의존성 이름은 Initializr 가 확인해 줬지만, 내가 쓴 소스는 빌드하지 못했다. 첫 `./gradlew test` 에서 import 오류가 나올 수 있다 |
| 4.1.2 | Initializr 에서 SNAPSHOT 이라 쓰지 않았다. 정식이 되면 올릴지 검토 |
| 응답 JSON | 202, 503 본문은 Spring 의 JSON 변환기가 만든다. Kafka 메시지의 JSON 은 직접 만든 것이라 두 경로가 다르다 |
| 상태 조회 | 결과 조회 API 는 없다. Day 17 이후에 `requestId` 로 조회 |
| 인증, 요청 제한 | 없음 |
| Kafka 가 꺼진 채 기동 | 시작 시 토픽 보장(`KafkaTopicConfig`)이 Kafka 접속을 재시도해 기동이 완료되지 않았다(로그에 `AdminClient ... Rebootstrapping` 반복). 기동 시 Kafka 필수. 필요하면 토픽 생성을 시작 단계에서 빼는 방법을 검토 |
| 파티션 1개 | 병렬 처리는 Day 18 에서 |

## 9. 체크리스트

- [x] Gradle wrapper 준비 (Initializr zip)
- [x] `build.gradle` 의존성 이름 확인 (Initializr zip)
- [x] `./gradlew test` 통과
- [x] `./gradlew bootRun` 으로 8080 기동
- [x] curl 로 202 와 `requestId` 응답
- [x] 빈 `question` 으로 400
- [x] Python Consumer 로 Kafka 에 들어간 메시지 확인
- [x] kafka-ui 에서 같은 requestId 확인
- [x] (선택) Kafka 중단 시 503
- [x] 결과를 `DAY14_RESULT.md` 에 기록
