# Day 26 코드 가이드 — 동기 프록시와 k6 sync 모드

**전체 목적:** Day 26 에서 추가한 코드가 어떤 순서로 동작하는지 설명한다. 실험 방법은 [DAY26_GUIDE.md](DAY26_GUIDE.md).
Spring 코드는 이 환경에서 빌드하지 못했으므로 컴파일 확인은 내 환경 몫이다.


## 1. 요청 흐름

| 순서 | 위치                       | 하는 일                                                                                       |
| ---- | -------------------------- | --------------------------------------------------------------------------------------------- |
| ①    | k6 → Spring `POST /api/agent/sync` | `{question, provider}` 를 보낸다 (`MODE=sync`)                                         |
| ②    | `SyncAgentController.sync` | provider 가 비면 `gemini`. `SyncAgentClient.call` 을 부르고 결과의 상태 코드와 본문을 그대로 돌려준다 |
| ③    | `SyncAgentClient.call`     | `{"query":..., "provider":...}` 를 만들어 FastAPI `/api/v1/agent/run` 에 POST 한다. 이 동안 Tomcat 스레드가 묶인다 |
| ④    | FastAPI `/api/v1/agent/run` | 막히는 `run_agent` 를 이벤트 루프에서 직접 실행한다. 끝나면 `{answer, provider}` 로 200            |
| ⑤    | `SyncAgentClient`          | 응답을 받으면 그대로 반환, `timeout-ms` 넘기면 504, 접속 실패면 502 로 바꾼다                      |


## 2. 파일별 설명

| 파일                      | 핵심                                                                                                                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SyncAgentClient`         | JDK `HttpClient` 를 쓴다(새 라이브러리 없음). HTTP/1.1 로 고정(uvicorn 이 h2c 업그레이드를 받지 않음). `HttpConnectTimeoutException` 이 `HttpTimeoutException` 의 하위라 먼저 잡아 502, 그 다음 504 |
| `SyncAgentController`     | 기존 `AgentRequestBody` 를 재사용해 검증(@NotBlank, @Size)을 같이 받는다. sessionId 는 쓰지 않는다                                                                                       |
| `AgentRequestMessage.quote` | 문자열을 JSON 으로 감싸는 기존 함수. 접근 범위만 public 으로 열었다                                                                                                                  |
| `application.properties`  | `app.sync.timeout-ms=30000`(504 기준), `connect-timeout-ms=2000`(502 기준), `fastapi-url`                                                                                              |
| `ramping.js`              | `MODE=sync` 는 Spring, `sync-direct` 는 FastAPI 를 호출한다. 두 모드 모두 응답 코드별 카운터를 올린다. 본문 필드는 Spring 이 `question`, FastAPI 가 `query` 로 다르다 |


## 3. 응답 코드 정리

| 코드  | 누가 만드나            | 의미                                      |
| ----- | ---------------------- | ----------------------------------------- |
| 200   | FastAPI → Spring 전달   | 답이 `timeout-ms` 안에 나옴               |
| 504   | `SyncAgentClient`      | FastAPI 가 `timeout-ms` 안에 답하지 못함   |
| 502   | `SyncAgentClient`      | FastAPI 에 접속하지 못함                  |
| 그 밖 | FastAPI → Spring 전달   | FastAPI 가 준 상태 코드를 그대로 전달     |
| 0     | k6                     | k6 자체 시간 제한 또는 접속 실패 (`sync_other`) |
