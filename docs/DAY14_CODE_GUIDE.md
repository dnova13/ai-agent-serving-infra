# Day 14 코드 가이드 — Spring Boot WAS 가 요청을 Kafka 에 넣고 202 로 바로 응답하기

> 실행하는 방법과 확인 항목은 [DAY14_GUIDE.md](DAY14_GUIDE.md), 결과는 [DAY14_RESULT.md](DAY14_RESULT.md). 이 문서는 Spring 코드가 요청을 받아 Kafka 에 넣기까지의 순서를 설명한다.
> 파일 경로는 `d3-was-spring/src/main/java/com/test/aiserving/was/` 기준이며 아래 표에서는 `was/` 로 줄여 적었다. 줄 번호는 적지 않았다.
> 포트: **8080 = Spring WAS**, **9092 = Kafka**. 이 Day 에는 FastAPI(8000, 8001) 워커를 쓰지 않는다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행하거나 빌드해 확인한 것은 없다. 설명한 코드는 Day 14 커밋(`32d75e9`) 시점의 것이다.

**전체 목적:** WAS 가 LLM 을 기다리지 않고 Kafka 에 요청을 넣은 직후 `202 Accepted` 로 응답하는 구조가 코드에서 어떻게 만들어지는지, Kafka 가 안 될 때 어떻게 `503` 으로 돌려주는지 따라간다.


## 1. 전체 흐름

```
POST /api/agent/requests {"question","provider"}
   ▼ AgentRequestController.accept
   ① @Valid 검증 (question 비어 있음/2000자 초과 → 400)
   ② requestId = UUID, provider 기본값 "gemini"
   ③ AgentRequestMessage 생성
   ④ AgentRequestProducer.publish → KafkaTemplate.send(topic, key=requestId, value=JSON).get(5000ms)
   ▼
   성공: 202 {"requestId","status":"ACCEPTED"}
   실패: PublishFailedException → 503 {"requestId","status":"REJECTED","error":...}
```

| 단계 | 하는 일                    | 파일                                          | 클래스 / 메서드                          |
| ---- | -------------------------- | --------------------------------------------- | ---------------------------------------- |
| A    | 요청 본문과 검증           | `was/api/AgentRequestBody.java`               | `AgentRequestBody`(record)               |
| B    | 접수와 응답                | `was/api/AgentRequestController.java`         | `accept`                                 |
| C    | Kafka 로 보낼 메시지       | `was/messaging/AgentRequestMessage.java`      | `toJson`, `quote`                        |
| D    | Kafka 전송                 | `was/messaging/AgentRequestProducer.java`     | `publish`                                |
| E    | 실패 예외                  | `was/messaging/PublishFailedException.java`   | -                                        |
| F    | 토픽 보장                  | `was/config/KafkaTopicConfig.java`            | `agentRequestsTopic`                     |
| G    | 설정                       | `d3-was-spring/src/main/resources/application.properties`, `d3-was-spring/build.gradle` | - |
| H    | 단위 테스트                | `d3-was-spring/src/test/java/.../messaging/AgentRequestMessageTest.java` | `toJson_필드_순서와_이스케이프`, `quote_제어문자는_유니코드_이스케이프` |


## 2. 주요 코드

### 2-1. 접수 (`accept`)

| 부분                         | 설명                                                                                                                              |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `@Valid @RequestBody`        | `question` 은 `@NotBlank`, 2000자 이하. `provider` 는 50자 이하이고 생략 가능                                                       |
| `DEFAULT_PROVIDER = "gemini"`| `provider` 가 null 이거나 공백이면 gemini                                                                                         |
| `requestId`                  | WAS 가 `UUID.randomUUID()` 로 만든다. 응답에 담아 돌려주고, 나중에 결과 조회(Day 17)의 열쇠가 된다                                    |
| 성공 응답                    | `202 ACCEPTED`. 처리가 끝났다는 뜻이 아니라 "큐에 넣었다"는 뜻이다                                                                  |
| 실패 응답                    | `PublishFailedException` 을 잡아 `503` + `status: REJECTED`. 같은 `requestId` 를 응답에 담는다                                       |

### 2-2. 전송 (`publish`)

```java
kafkaTemplate.send(topic, message.requestId(), message.toJson()).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
```

| 부분                                   | 설명                                                                                                                                      |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| key = `requestId`                      | 파티션이 여러 개가 되면 같은 요청 id 는 같은 파티션으로 간다                                                                                 |
| `.get(5000ms)` 로 기다림               | 브로커가 저장했다고 응답할 때까지 기다린 뒤 반환한다(`acks=all`). 그래야 202 가 "Kafka 에 실제로 들어갔다"는 뜻이 된다. 이 대기는 LLM 호출과 달리 보통 수 ms 라고 코드 주석에 적혀 있다 |
| `max.block.ms=5000` 설정               | `send()` 는 Future 를 돌려주기 전에 브로커 메타데이터를 받으려고 최대 `max.block.ms`(기본 60초) 동안 스레드를 막는다. `.get(timeout)` 만으로는 이 대기를 줄일 수 없어서 properties 에서도 줄였다(코드 주석) |
| `catch (RuntimeException e)` 추가      | `[OLD]` 는 `ExecutionException`, `TimeoutException` 만 잡았다. `send()` 가 던지는 `RuntimeException` 은 잡히지 않아 503 대신 500 이 될 수 있었다. `[NEW]` 는 같은 실패로 취급해 503 으로 돌려준다 |
| `InterruptedException`                 | 인터럽트 표시를 되살린 뒤 `PublishFailedException` 으로 바꾼다                                                                              |

### 2-3. 메시지를 JSON 으로 (`AgentRequestMessage.toJson`)

| 부분                         | 설명                                                                                                                                      |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 직접 문자열을 만듦           | Spring Boot 4 의 JSON 라이브러리(Jackson 3)와 이전 버전의 패키지가 달라, 확인하기 전에는 특정 JSON 라이브러리에 의존하지 않기로 했다(코드 주석). 필드가 4개뿐이라 단순하다 |
| `quote`                      | 따옴표, 역슬래시, 줄바꿈 등 제어 문자를 이스케이프하고 한글은 그대로 둔다. 0x20 미만 문자는 `\uXXXX` 로 바꾼다                              |
| 필드 순서                    | `requestId`, `question`, `provider`, `createdAt`. Day 13 Python Producer 와 같은 스키마다                                                  |
| 단위 테스트                  | 순수 자바 테스트 2개: 따옴표와 줄바꿈이 섞인 질문의 JSON 문자열 비교, 제어 문자 0x01 이 `\u0001` 이 되는지. 스프링과 Kafka 를 띄우지 않는다  |

### 2-4. 토픽과 설정

| 부분                                    | 설명                                                                                                                              |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `KafkaTopicConfig.agentRequestsTopic`   | 시작할 때 토픽 `agent.requests` 를 보장한다. 이미 있으면 그대로 두고 파티션 수만 설정값이 더 크면 늘린다. 이 Day 의 `requests-partitions` 는 1이다 |
| `application.properties`                | `server.port=8080`, `spring.kafka.bootstrap-servers=localhost:9092`, `acks=all`, `max.block.ms=5000`, 값과 키는 `StringSerializer`, `app.kafka.send-timeout-ms=5000` |
| `build.gradle`                          | Spring Boot 4.1.1, Java 21, `spring-boot-starter-kafka`, `-validation`, `-webmvc`                                                 |


## 3. 결과를 읽는 순서

1. 정상일 때 `curl` 이 `202` 와 `requestId` 를 즉시 돌려주는지, 응답 시간이 LLM 시간(수 초)이 아니라 수십 ms 인지 본다.
2. kafka-ui(8090) 의 `agent.requests` 토픽에서 메시지가 Day 13 과 같은 4개 필드인지 본다.
3. Kafka 를 끈 채 요청하면 5초 안에 `503 REJECTED` 가 오는지 본다(`max.block.ms` 효과).
4. `question` 을 비우거나 2000자를 넘기면 `400` 인지 본다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 빌드, 테스트, 서버 실행을 하지 않았다. 응답 시간 같은 숫자는 DAY14_RESULT 의 기록을 따른다                              |
| 현재 코드와의 차이    | Day 14 이후 파티션 수가 3으로 바뀌는 등(Day 18) 설정이 달라졌다. 이 문서는 Day 14 커밋 시점의 파일을 읽은 것이다                        |
| 부하 상황             | `.get()` 으로 기다리는 방식이 동시 요청이 많을 때 WAS 스레드에 주는 영향은 확인하지 않았다(k6 실험에서 다룰 수 있다는 코드 주석뿐)        |
