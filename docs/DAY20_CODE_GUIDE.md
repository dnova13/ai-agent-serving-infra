# Day 20 코드 가이드 — 대화 이어가기가 코드에서 어떻게 흐르는가

> 실행하는 방법과 확인 항목은 [DAY20_GUIDE.md](DAY20_GUIDE.md), 실행 결과는 [DAY20_RESULT.md](DAY20_RESULT.md), 설계 고찰은 [CONVERSATION_CONTEXT_NOTES.md](CONVERSATION_CONTEXT_NOTES.md). 이 문서는 **코드가 어떤 순서로 무엇을 하는지**를 파일 위치, 함수, 줄 단위 설명으로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 작성 시점(2026-10-13) 기준이라 코드를 고치면 밀릴 수 있으니, 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI**, **6379 = Redis**, 9092 = Kafka.
> 상태: 코드를 읽고 설명한 문서다. FastAPI 직접 호출 경로(`POST /api/v1/agent/run`)로 같은 세션 이어가기, 세션 없음, 세션 분리는 내 환경에서 실행해 확인했다(DAY20_RESULT 3번). Spring → Kafka → FastAPI 경로(가이드 ④)도 `d20-c` 로 실행해 두 질문이 Redis 에 순서대로 저장된 것을 확인했다(DAY20_RESULT 3-4). 7번 참고.

**목적:** Day 20에서 바뀐 코드가 "같은 `sessionId` 의 두 번째 질문이 앞의 대화를 알고 답한다"를 어떤 순서로 해내는지 따라간다. 요청이 Spring(8080)에서 들어와 Kafka 를 거쳐 FastAPI(8000)에서 Redis(6379)를 읽고 쓰기까지, 각 단계가 어느 파일의 어느 함수인지 찾을 수 있고, 각 함수가 줄 단위로 무엇을 하는지, 어디서 실패할 수 있는지까지 정리한다.


## 1. 전체 흐름

```
클라이언트
  │ POST /api/agent/requests  {"question": "...", "sessionId": "d20-c"}
  ▼
[Spring :8080]  AgentRequestController.accept
  │ requestId 발급, sessionId 정리 → Kafka 메시지(JSON) 생성 → 토픽 agent.requests 로 전송 → 202 응답
  ▼
[Kafka]  메시지: {"requestId":..., "question":..., "provider":..., "createdAt":..., "sessionId":"d20-c"}
  ▼
[FastAPI :8000]  KafkaWorker._handle 이 JSON 을 AgentRequestMessage 로 해석 → agent_handler 호출
  │ asyncio.to_thread 로 run_agent(question, provider, sessionId) 를 별도 스레드에서 실행
  ▼
run_agent  ① Redis 에서 이전 대화 읽기 (conversation_store.load_history)
           ② 이전 대화 + 이번 질문을 모델 입력으로 만들기
           ③ 그래프 실행 (LLM → 도구 → LLM, 20~30초)
           ④ 최종 답 꺼내기
           ⑤ 이번 질문+답을 Redis 에 저장 (conversation_store.append_turn)
  ▼
결과: 워커 메모리의 result_store 에 DONE/답 저장 (GET /api/v1/agent/results/{requestId})
대화: Redis 의 agent:conversation:d20-c  (GET /api/v1/agent/sessions/d20-c 로 확인)
```

Kafka 와 Spring 을 거치지 않는 경로도 있다. `POST /api/v1/agent/run`(`routers/agent.py`)이 `session_id` 를 받아 같은 `run_agent` 를 직접 부른다. 두 경로 모두 대화를 읽고 쓰는 곳은 `run_agent` 한 곳이다. DAY20_RESULT 의 실행은 이 직접 경로로 했다.

### 1-1. 단계별 파일 위치

| 단계 | 하는 일                                      | 파일                                                                                   | 함수 (줄)                                              |
| ---- | -------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| A    | 요청 접수, sessionId 정리, Kafka 로 전송      | `d3-was-spring/src/main/java/com/test/aiserving/was/api/AgentRequestController.java`   | `accept` (38)                                          |
| A'   | 요청 본문의 sessionId 길이 검증               | `d3-was-spring/.../api/AgentRequestBody.java`                                          | `sessionId` 필드 (16~17)                               |
| B    | Kafka 메시지 JSON 생성                        | `d3-was-spring/.../messaging/AgentRequestMessage.java`                                 | `toJson` (20)                                          |
| B'   | Kafka 로 실제 전송                            | `d3-was-spring/.../messaging/AgentRequestProducer.java`                                | `publish` (43)                                         |
| C    | 메시지를 읽어 파이썬 객체로 해석              | `d2-ai-python/app/services/kafka_worker.py`, `d2-ai-python/app/schemas/models.py`      | `KafkaWorker._handle` (191), `AgentRequestMessage`     |
| D    | 메시지마다 에이전트 호출                      | `d2-ai-python/app/services/agent_handler.py`                                           | `agent_handler` (29)                                   |
| E    | 대화 읽기, 모델 호출, 대화 저장 (핵심)        | `d2-ai-python/app/services/agent_graph.py`                                             | `run_agent` (354), `_history_to_messages` (330)        |
| F    | 대화를 Redis 에 읽고 쓰는 규칙                | `d2-ai-python/app/services/conversation_store.py`                                      | `load_history` (34), `append_turn` (40)                |
| G    | Redis 기본 읽기/쓰기 (Day 19)                 | `d2-ai-python/app/services/redis_client.py`                                            | `get_json`, `set_json`, `make_key`, `ttl_sec`          |
| H    | 저장된 대화를 HTTP 로 확인                    | `d2-ai-python/app/routers/sessions.py`                                                 | `get_session` (19), `delete_session` (31)              |
| I    | 직접 호출 경로                                | `d2-ai-python/app/routers/agent.py`                                                    | `run_agent_endpoint`                                   |

### 1-2. 어느 프로세스, 어느 스레드에서 도는가

| 구간                                | 프로세스 / 실행 방식                                                             | 의미                                                                                               |
| ----------------------------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| A, B, B'                            | Spring(8080) 프로세스, 요청 처리 스레드                                          | LLM 을 기다리지 않고 Kafka 에 넣은 뒤 바로 202                                                     |
| C, D                                | FastAPI(8000) 프로세스의 asyncio 이벤트 루프                                     | 한 워커는 메시지를 한 건씩 순서대로 처리한다                                                       |
| E, F, G (`run_agent` 안)            | `asyncio.to_thread` 로 만든 별도 스레드                                          | 동기 Redis 호출과 LLM 호출이 이벤트 루프를 막지 않는다                                             |
| H (`sessions.py`)                   | FastAPI 가 `def` 라서 스레드풀에서 실행                                          | 확인용 API 가 Redis 를 기다려도 루프를 막지 않는다                                                 |
| I (`POST /api/v1/agent/run`)        | `async def` 안에서 `run_agent` 를 직접 호출                                      | 20~30초 동안 이 프로세스의 이벤트 루프가 막힌다(Day 16 이전 경로를 그대로 둠, 코드 주석에도 적어 둠) |


## 2. 주요 코드

### 2-1. Spring: sessionId 를 받아 Kafka 메시지에 싣는다 (A, A', B, B')

**목적:** 클라이언트가 보낸 `sessionId` 를 검증하고 정리해서 Kafka 메시지에 실어 보낸다. Spring 은 대화 내용을 모르고, 식별자만 전달한다.

- 파일: `d3-was-spring/src/main/java/com/test/aiserving/was/api/AgentRequestBody.java`
- 파일: `d3-was-spring/src/main/java/com/test/aiserving/was/api/AgentRequestController.java`
- 파일: `d3-was-spring/src/main/java/com/test/aiserving/was/messaging/AgentRequestMessage.java`

**요청 본문 (`AgentRequestBody`)**

```java
@Size(max = 100, message = "sessionId 는 100자 이하여야 합니다")
String sessionId) {
```

| 줄/요소                  | 하는 일                                                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `@Size(max = 100)`       | 길이만 제한한다. 필수가 아니므로 `@NotBlank` 가 없고, 보내지 않으면 `null` 이다                                                 |
| 100자 제한의 이유        | 이 값이 Redis 키(`agent:conversation:<sessionId>`)의 일부가 되므로 너무 긴 키를 막는다. 문자 종류(공백, 특수문자)는 막지 않는다 |
| 검증 실패 시             | `@Valid` 가 걸려 있어 컨트롤러가 호출되기 전에 400 으로 거절된다                                                                |

**컨트롤러 (`AgentRequestController.accept`, 38줄)**

```java
String requestId = UUID.randomUUID().toString();                                      // ①
String provider = (body.provider() == null || body.provider().isBlank()) ? DEFAULT_PROVIDER : body.provider();  // ②
String sessionId = (body.sessionId() == null || body.sessionId().isBlank()) ? null : body.sessionId();
AgentRequestMessage message = new AgentRequestMessage(requestId, body.question(), provider, Instant.now(), sessionId);  // ③
producer.publish(message);                                                            // ④
return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("requestId", requestId, "status", "ACCEPTED"));  // ⑤
```

| 번호 | 하는 일                                                                                                                                       |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| ①    | 요청 한 건을 가리키는 id 를 새로 만든다. 결과 조회(`GET .../results/{requestId}`)에 쓴다. `sessionId` 와는 다른 값이다                         |
| ②    | `provider` 가 없거나 공백이면 `gemini`                                                                                                        |
| ③    | `sessionId` 가 `null`, 빈 문자열, 공백뿐이면 `null`("세션 없음")로 통일한 뒤 메시지 객체를 만든다. 공백이 섞인 값(`" a "`)은 그대로 보낸다(다듬지 않음) |
| ④    | Kafka 토픽 `agent.requests` 로 보낸다(key=`requestId`). 실패하면 `PublishFailedException` → 503 과 `REJECTED`(코드의 `catch` 블록)                |
| ⑤    | 처리 결과가 아니라 "접수됨"만 돌려준다. 이어가기가 됐는지는 이 응답에서 알 수 없다                                                           |

**메시지 JSON (`AgentRequestMessage.toJson`, 20줄)**

```java
+ "\"createdAt\":" + quote(createdAt.toString()) + ","
+ "\"sessionId\":" + (sessionId == null ? "null" : quote(sessionId))
```

| 알아둘 점                                | 설명                                                                                                                                       |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `requestId` 와 `sessionId` 는 다르다     | `requestId` 는 요청 한 건(결과 조회), `sessionId` 는 대화 한 묶음(여러 요청이 공유)                                                         |
| 맨 끝에 붙인 이유                        | 앞의 4개 필드 순서를 그대로 둬서 기존 JSON 모양과 테스트를 최대한 유지한다. 필드가 없는 옛 메시지도 FastAPI 가 그대로 읽는다               |
| `null` 도 필드로 보낸다                  | `"sessionId":null` 로 항상 필드가 있다. FastAPI 쪽은 `Optional[str] = None` 이라 둘 다 같은 `None` 이 된다                                  |
| `quote(sessionId)`                       | 따옴표, 역슬래시, 제어 문자를 이스케이프한다. `sessionId` 에 `"` 가 있어도 JSON 이 깨지지 않는다                                            |
| Spring 은 이 값으로 아무것도 찾지 않는다 | 이전 대화를 찾는 일은 모두 FastAPI 쪽이다                                                                                                   |

`toJson` 은 단위 테스트 `AgentRequestMessageTest` 로 확인했다(내 환경에서 통과). 컨트롤러가 `sessionId` 를 받아 메시지에 담는 부분은 이 테스트가 보지 않고, 아직 실행해 보지 않았다.

### 2-2. FastAPI: 메시지를 받아 해석한다 (C)

**목적:** Kafka 에서 온 JSON 의 `sessionId` 를 파이썬 객체로 받는다. 값이 없거나 `null` 이어도 읽히게 한다.

- 파일: `d2-ai-python/app/services/kafka_worker.py` (`KafkaWorker._handle`, 191줄)
- 파일: `d2-ai-python/app/schemas/models.py` (`AgentRequestMessage`, `AgentRequest`)

```python
# kafka_worker._handle (요약)
message = AgentRequestMessage.model_validate(json.loads(record.value.decode("utf-8")))
...
await self._handler(message, record)    # handler = agent_handler
```

```python
# models.py
class AgentRequestMessage(BaseModel):
    requestId: str
    question: str
    provider: str = "gemini"
    createdAt: datetime
    sessionId: Optional[str] = None   # 필드가 없거나 null 이면 None → 새 대화
```

| 줄/요소                      | 하는 일                                                                                                                          |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `json.loads(...)`            | Kafka 의 바이트를 UTF-8 문자열로 바꾸고 dict 로 해석한다                                                                         |
| `model_validate(...)`        | dict 를 `AgentRequestMessage` 로 검증, 변환한다. 필수 필드가 없으면 예외가 나고, 해석할 수 없는 메시지는 워커가 건너뛴다(Day 16)     |
| `sessionId: Optional[str] = None` | 기본값이 있어서 Day 20 이전에 만든 메시지(필드 없음)도 그대로 읽힌다                                                        |
| 이름 표기의 차이             | Kafka 메시지는 `sessionId`(카멜), `POST /api/v1/agent/run` 본문 `AgentRequest` 는 `session_id`(스네이크)다. 같은 의미이고 표기만 다르다 |

### 2-3. FastAPI: 메시지에서 에이전트로 넘기는 한 줄 (D)

**목적:** Kafka 에서 읽은 메시지의 `sessionId` 를 `run_agent` 로 넘기는 연결 지점이다.

- 파일: `d2-ai-python/app/services/agent_handler.py` (`agent_handler`, 29줄)

```python
logger.info("[agent] 시작 worker=%s requestId=%s partition=%s offset=%s provider=%s sessionId=%s", ...)
store.set_processing(message.requestId, worker=WORKER_ID, partition=record.partition, offset=record.offset)
...
answer = await asyncio.wait_for(
    asyncio.to_thread(run_agent, message.question, message.provider, message.sessionId),
    timeout=AGENT_TIMEOUT_SEC,
)
...
store.set_done(message.requestId, str(answer), elapsed_ms)
```

| 줄/요소                               | 하는 일                                                                                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| 시작 로그의 `sessionId=...`           | 어느 세션 요청인지, 어느 파티션/워커가 받았는지 로그로 추적한다                                                                                      |
| `store.set_processing`                | 결과 저장소(워커 메모리)에 PROCESSING 으로 기록. 이것은 **요청 결과**이고 대화 기록(Redis)과 별개다                                                   |
| `asyncio.to_thread(run_agent, ...)`   | 동기 함수 `run_agent` 를 별도 스레드에서 실행한다. 그 안의 Redis 호출과 LLM 호출이 이벤트 루프를 막지 않는다. 마지막 인자가 `session_id` 다           |
| `asyncio.wait_for(..., timeout=120)`  | 기본 120초(`AGENT_TIMEOUT_SEC`). 넘으면 실패로 처리하고 다음 메시지로 간다                                                                           |
| 타임아웃 후의 스레드                  | 코드 주석대로 타임아웃이 나도 스레드 안의 호출 자체는 멈추지 못한다. 그래서 `run_agent` 가 뒤늦게 끝나 ⑤ 저장까지 할 수 있다고 코드상 예상된다(실행해 보지 않음) |
| `store.set_done(...)`                 | 답을 결과 저장소에 DONE 으로 기록한다                                                                                                                |

### 2-4. 핵심: run_agent 가 읽고, 묻고, 저장한다 (E)

**목적:** 이전 대화를 모델 입력에 붙이고, 끝나면 이번 턴을 저장한다. Day 20 의 핵심이 이 함수 하나에 들어 있다.

- 파일: `d2-ai-python/app/services/agent_graph.py` (`_history_to_messages` 330줄, `run_agent` 354줄)

**① 저장 형식을 모델이 이해하는 객체로 바꾸기**

```python
def _history_to_messages(history: list[dict]) -> list:
    out = []
    for h in history:
        if h.get("role") == "user":
            out.append(HumanMessage(content=h["content"]))
        elif h.get("role") == "assistant":
            out.append(AIMessage(content=h["content"]))
    return out
```

| 줄/요소                        | 하는 일                                                                                                                                  |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Redis 의 모양                  | `[{"role":"user","content":"..."}, {"role":"assistant","content":"..."}]` (파이썬 dict 와 JSON)                                           |
| `HumanMessage` / `AIMessage`   | LangChain 이 쓰는 메시지 클래스. 모델 호출 시 각각 사용자 발화, 모델 발화로 전달된다                                                     |
| `role` 이 둘 다 아니면         | 조용히 건너뛴다(에러 없음). 지금은 우리 코드만 저장하므로 해당 없지만, 다른 값이 들어가면 그 항목이 사라진다                              |
| 도구 메시지                    | 저장하지 않으므로 여기서 복원되지도 않는다                                                                                               |

**run_agent 본문 (354줄)**

```python
def run_agent(question, provider="gemini", session_id=None):
    prior = []
    if session_id:                                                         # (a)
        try:
            prior = _history_to_messages(conversation_store.load_history(session_id))   # (b) ① ②
        except Exception as e:
            _log.warning("대화 기록을 못 불러옴 ...")                         # (c)
    print(f"[timing] 이전 대화 {len(prior)}개 메시지 session={session_id}")   # (d)

    initial_state = {
        "messages": prior + [HumanMessage(content=question)],              # (e) ②
        "provider": provider,
        "retry_count": 0,
        "pending_tool_calls": [],
    }
    final_state = AGENT_GRAPH.invoke(initial_state)                        # (f) ③
    last_message = final_state["messages"][-1]                             # (g) ④
    content = last_message.content
    if isinstance(content, list):                                          # (h)
        texts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
        content = "\n".join(texts)

    if session_id:                                                         # (i)
        try:
            conversation_store.append_turn(session_id, question, str(content))   # (j) ⑤
        except Exception as e:
            _log.warning("대화 기록을 못 저장함 ...")                         # (k)
    return content
```

| 표시 | 하는 일                                                                                                                                                                                  |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a)  | `session_id` 가 `None` 이나 빈 문자열이면 이어가기를 건너뛴다. 이 경우 예전 `run_agent(question, provider)` 호출도 그대로 동작한다(세 번째 인자의 기본값이 `None`)                          |
| (b)  | ① `load_history` 로 Redis 에서 읽고 ② `_history_to_messages` 로 객체로 바꾼다. 처음 보는 세션이나 만료된 세션이면 빈 리스트라 `prior` 도 빈 리스트다                                       |
| (c)  | Redis 가 꺼져 있거나 읽기가 실패하면 경고만 남기고 `prior=[]` 로 계속한다. 이 경우 이번 질문은 새 대화처럼 처리된다. Redis 연결 제한 시간이 1초라서(`redis_client.py`) 지연은 최대 그 정도다(예상, 재지 않음) |
| (d)  | `[timing] 이전 대화 N개 메시지` 로그. 이어가기가 실제로 됐는지 서버 로그로 확인하는 지점이다. 첫 질문은 0개, 같은 세션의 두 번째 질문은 2개여야 한다(DAY20_GUIDE 의 확인 항목)             |
| (e)  | 모델이 볼 입력 목록을 만든다: `[이전 질문, 이전 답, ..., 이번 질문]`. **이번 질문이 항상 마지막**이다. 이 한 줄이 이어가기의 전부라고 해도 된다                                              |
| (f)  | 그래프를 돌린다. `agent_node` 는 `[SystemMessage(SYSTEM_PROMPT)] + state["messages"]` 를 모델에 보낸다. 즉 모델 입력은 `시스템 프롬프트 + 이전 대화 + 이번 질문`이다                         |
| (g)  | 그래프가 끝난 뒤 `messages` 의 마지막 항목이 최종 답이다                                                                                                                                  |
| (h)  | Gemini 가 답을 블록 목록으로 줄 때가 있어 텍스트 블록만 이어 붙인다. 문자열이면 그대로 쓴다                                                                                              |
| (i)(j) | ⑤ 이번 질문과 답을 `append_turn` 으로 저장한다. `str(content)` 로 문자열을 보장한다                                                                                                     |
| (k)  | 저장이 실패해도 예외를 삼키고 `return content` 로 답은 정상으로 돌려준다. 대신 다음 질문에서는 이번 턴을 모른다                                                                          |

**그래프가 도는 동안 `messages` 안에서 일어나는 일**

| 시점                          | `state["messages"]` 의 내용                                                                                 |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------- |
| 시작 (e)                      | `[이전 Q1, 이전 A1, ..., 이번 Q]`                                                                           |
| agent_node 1번째 호출 뒤      | 위 + 모델 응답 `AIMessage`(도구 호출 요청 포함, 내용은 비어 있을 수 있음)                                   |
| tool_node 실행 뒤             | 위 + `ToolMessage`(검색 결과 문서)                                                                          |
| agent_node 2번째 호출 뒤      | 위 + 최종 답 `AIMessage` ← (g) 에서 꺼내는 마지막 항목                                                      |

중간의 `AIMessage`(도구 호출)와 `ToolMessage` 는 `final_state` 안에는 있지만 (j) 에서 저장하는 것은 `question` 과 `content`(최종 답)뿐이다. 그래서 Redis 에는 도구 호출과 검색 결과가 들어가지 않는다.

| 알아둘 점                              | 설명                                                                                                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 읽기와 저장이 따로 try/except          | Redis 가 꺼져 있으면 ① 에서 한 번, ⑤ 에서 한 번 경고가 찍히고 답은 정상으로 온다. 대화 이어가기만 포기하는 설계다                                            |
| 그래프가 예외를 내면                   | ⑤ 에 도달하지 못한다. 그 질문은 대화에 남지 않는다(코드 흐름상 예상, 실행해 보지 않음)                                                                      |
| 재시도 실패(`give_up_node`)의 경우     | 그래프의 마지막 메시지가 "죄송합니다, N번 시도했지만 ..." 안내문이다. 이것도 (g)~(j) 에서 "답"으로 저장된다. 다음 질문에서 모델이 이 안내문을 앞 답으로 보게 된다(코드상 예상) |
| 이전 대화가 길수록                     | 모델 입력이 길어진다. Day 18 에서 잰 대로 시간의 대부분이 LLM 호출이라 더 느려질 수 있다(이번에 재지 않았다)                                                |

### 2-5. Redis 에 대화를 읽고 쓰는 규칙 (F)

**목적:** 대화를 Redis 에 어떤 키와 값으로, 얼마나, 얼마 동안 두는지를 한 곳에서 정한다.

- 파일: `d2-ai-python/app/services/conversation_store.py` (`load_history` 34줄, `append_turn` 40줄, `clear` 52줄, `ttl_left` 57줄)

```python
MAX_TURNS = int(os.getenv("CONVERSATION_MAX_TURNS", "10"))   # 남기는 최근 턴 수
TTL_SEC   = int(os.getenv("CONVERSATION_TTL_SEC", "3600"))   # 마지막 저장 후 살아 있는 시간(초)

def conversation_key(session_id):
    return rc.make_key("conversation", session_id)          # → "agent:conversation:<sessionId>"

def load_history(session_id):
    return rc.get_json(conversation_key(session_id)) or []

def append_turn(session_id, question, answer):
    history = load_history(session_id)                                   # (1)
    history.append({"role": "user", "content": question})                # (2)
    history.append({"role": "assistant", "content": answer})
    history = history[-MAX_TURNS * 2:]                                   # (3)
    rc.set_json(conversation_key(session_id), history, ttl_sec=TTL_SEC)  # (4)
    return history
```

| 줄/요소                               | 하는 일                                                                                                                                                         |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `os.getenv(..., "10")`                | 환경변수가 없으면 10턴, 3600초. 서버를 시작할 때 한 번 읽는다(실행 중 변경 불가). `int(...)` 라서 숫자가 아닌 값이면 서버 시작 때 에러가 난다                    |
| `from app.services import redis_client as rc` | 파일 맨 위의 import. 서버와 `-m` 실행 모두 `d2-ai-python` 폴더에서 하므로 한 가지 경로만 쓴다(Day 20 당시에는 `try/except ModuleNotFoundError` 로 두 경우를 지원했다)                           |
| `conversation_key`                    | `make_key("conversation", "d20-a")` → `agent:conversation:d20-a`. 접두어 `agent:` 와 `:` 구분자는 `redis_client.make_key` 가 붙인다                              |
| `load_history`                        | `get_json` 은 키가 없거나 만료되면 `None` 을 주고, `or []` 가 이를 빈 리스트로 바꾼다. "처음 보는 세션"과 "만료된 세션"을 코드가 구분하지 않는다                  |
| (1)                                   | 저장된 대화를 **다시** 읽는다. `run_agent` 의 ① 에서 읽은 것을 재사용하지 않는다. 그래서 LLM 이 도는 20~30초 사이에 다른 워커가 쓴 턴은 반영해서 붙인다             |
| (2)                                   | 질문과 답을 순서대로 붙인다. 사용자 → 모델 순서가 항상 지켜진다                                                                                                 |
| (3)                                   | `[-20:]` 는 뒤에서 20개를 남기는 슬라이스다. 20개 이하면 전체가 그대로 남는다. `*2` 는 질문과 답이 1턴이라서다                                                    |
| (4)                                   | JSON 으로 바꿔 `SET key value EX 3600` 형태로 쓴다. 쓸 때마다 TTL 이 새로 시작해서 대화가 이어지는 동안은 만료되지 않는다                                         |
| `clear`                               | 키를 지운다. 지운 것이 있으면 `True`. `DELETE /sessions/{id}` 가 쓴다                                                                                           |
| `ttl_left`                            | 남은 초. 키가 없으면 `None`. `GET /sessions/{id}` 의 `ttlSec` 가 쓴다                                                                                           |

Redis 에 들어 있는 값의 실제 모양은 다음과 같다(2턴 뒤의 예시).

```json
[
  {"role": "user",      "content": "교환 시 배송비는 누가 부담하나요?"},
  {"role": "assistant", "content": "교환 배송비는 고객님께서 부담하셔야 합니다. ..."},
  {"role": "user",      "content": "그럼 반품할 때는요?"},
  {"role": "assistant", "content": "..."}
]
```

| 알아둘 점                              | 설명                                                                                                                                                              |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 읽기-수정-쓰기 구조                    | (1)에서 읽고 (4)에서 통째로 쓴다. 그 사이 다른 워커가 같은 세션에 저장하면 한쪽 턴이 덮어써진다. 락이 없는 지금의 한계이고 Day 21~22 에서 다룬다                     |
| 문자열 하나로 저장                     | Redis 의 리스트 타입이 아니라 문자열이라 "전체 읽기, 전체 쓰기"만 가능하고 일부만 갱신할 수는 없다. 대화가 커질수록 매번 읽고 쓰는 양도 커진다                       |
| 버리는 방식                            | 10턴을 넘으면 앞을 요약 없이 버린다. 이유와 대안은 [CONVERSATION_CONTEXT_NOTES.md](CONVERSATION_CONTEXT_NOTES.md)                                                  |
| 오류를 올린다                          | 이 파일은 예외를 잡지 않는다. 잡는 곳은 `run_agent`(경고만)와 `sessions.py`(503)로, 호출하는 쪽이 정한다                                                          |

### 2-6. Redis 호출의 실제 구현 (G)

**목적:** `conversation_store` 가 부르는 함수들이 Redis 에 실제로 무엇을 보내는지 확인한다. (Day 19 에서 만든 코드)

- 파일: `d2-ai-python/app/services/redis_client.py`

| 함수/요소                            | 하는 일                                                                                                                                                          |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_client()`                       | 프로세스에서 클라이언트를 하나만 만들어 재사용한다(전역 `_client`). 내부에 연결 풀이 있어 여러 스레드가 같이 써도 된다                                             |
| `decode_responses=True`              | Redis 에서 오는 값을 bytes 가 아니라 str 로 받는다                                                                                                               |
| `socket_connect_timeout` 1초, `socket_timeout` 2초 | Redis 가 꺼져 있으면 연결을 1초 안에 포기한다. 환경변수 `REDIS_CONNECT_TIMEOUT_SEC`, `REDIS_SOCKET_TIMEOUT_SEC` 로 조절한다                              |
| `make_key(*parts)`                   | `"agent:" + ":".join(parts)`                                                                                                                                     |
| `set_json(key, value, ttl_sec)`      | `json.dumps(value, ensure_ascii=False)` 로 문자열로 바꿔 `SET key value EX ttl_sec` 을 보낸다. `ensure_ascii=False` 라 한글이 그대로 저장된다(`redis-cli` 에서 읽기 쉽다) |
| `get_json(key)`                      | `GET` 을 보내고, 키가 없으면 `None`, 있으면 `json.loads` 한 값을 돌려준다                                                                                         |
| `ttl_sec(key)`                       | Redis `TTL` 명령 결과를 돌려주되 키 없음(-2)만 `None` 으로 바꾼다. 만료 시간이 없는 키(영구)는 -1 이다                                                           |
| 접속 주소                            | `REDIS_HOST`(기본 localhost), `REDIS_PORT`(6379), `REDIS_DB`(0)                                                                                                  |

Redis 에 TTL 과 함께 저장된 것은 `redis-cli` 로 `TTL agent:conversation:d20-a` 를 치면 남은 초를 볼 수 있다(DAY20_GUIDE ⑤, 아직 실행하지 않음).

### 2-7. 저장된 대화를 HTTP 로 보는 확인용 API (H)

**목적:** `redis-cli` 없이 저장 내용과 남은 시간을 확인한다.

- 파일: `d2-ai-python/app/routers/sessions.py` (`get_session` 19줄, `delete_session` 31줄)
- 등록: `d2-ai-python/app/main.py` 의 `app.include_router(sessions.router)`

```python
@router.get("/sessions/{session_id}")
def get_session(session_id):
    try:
        history = conversation_store.load_history(session_id)
        ttl = conversation_store.ttl_left(session_id)
    except Exception as e:                      # Redis 장애
        raise HTTPException(503, ...)
    if not history:
        raise HTTPException(404, ...)           # 없는 세션이거나 만료
    return {"sessionId": session_id, "turns": len(history) // 2, "ttlSec": ttl, "messages": history}
```

| 줄/요소                  | 하는 일                                                                                                                                  |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `def` (async 아님)       | FastAPI 가 스레드풀에서 실행한다. 동기 Redis 호출이 이벤트 루프를 막지 않는다                                                            |
| 404                      | 키가 없는 경우. 한 번도 안 쓴 세션과 TTL 로 만료된 세션을 구분하지 못한다                                                                |
| 503                      | Redis 를 쓸 수 없는 경우. `run_agent` 는 오류를 숨기지만, 이 API 는 Redis 확인이 목적이라 드러낸다                                        |
| `turns`                  | `len(history) // 2`. 메시지 두 개가 1턴                                                                                                  |
| `ttlSec`                 | 남은 만료 시간. 대화가 저장될 때마다 3600 부근으로 되돌아간다. DAY20_RESULT 3-3 에서 3595 를 확인했다                                    |
| `DELETE`                 | 대화를 지운다. 응답의 `deleted` 는 지운 게 있었는지(True/False)                                                                          |
| `main.py` 등록           | `from routers import ..., sessions` 와 `include_router(sessions.router)` 두 군데가 필요하다. 빠지면 404 가 난다                           |

### 2-8. Kafka 를 거치지 않는 직접 호출 경로 (I)

**목적:** DAY20_RESULT 의 실행에 쓴 경로가 Kafka 경로와 어디서 갈리고 어디서 합쳐지는지 구분한다.

- 파일: `d2-ai-python/app/routers/agent.py` (`run_agent_endpoint`), `d2-ai-python/app/schemas/models.py` (`AgentRequest`)

```python
@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    answer = run_agent(req.query, provider=req.provider, session_id=req.session_id)
    return AgentResponse(answer=answer, provider=req.provider)
```

| 비교               | Kafka 경로                                  | 직접 경로                                                        |
| ------------------ | ------------------------------------------- | ---------------------------------------------------------------- |
| 요청 본문 필드     | Spring 이 만든 JSON 의 `sessionId`          | `AgentRequest.session_id`                                        |
| 호출 방식          | `asyncio.to_thread(run_agent, ...)`         | `async def` 안에서 `run_agent` 직접 호출                         |
| 응답               | 202 와 `requestId` (결과는 따로 조회)       | 답을 바로 반환 (20~30초 기다림)                                   |
| 이벤트 루프        | 막지 않음                                   | 끝날 때까지 막음 (그동안 `/health` 도 응답 지연)                  |
| 합쳐지는 곳        | `run_agent`                                 | `run_agent`                                                      |

이벤트 루프가 막히는 점 때문에 같은 서버에서 직접 경로 요청 둘을 동시에 보내도 순서대로 처리된다. 같은 세션 동시 요청 경합을 보려면 워커 두 대가 필요한 이유다(DAY20_GUIDE ⑧).


## 3. 실패 지점별 동작

**목적:** 어디서 무엇이 실패하면 사용자와 대화 기록에 어떻게 나타나는지 한 표에 모은다. 아래는 코드를 읽고 정리한 예상이다. Redis 중지 케이스는 아직 실행하지 않았다(DAY20_GUIDE ⑦).

| 실패 상황                           | 잡는 곳                                | 사용자에게                                           | 대화 기록                                                   |
| ----------------------------------- | -------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------- |
| Redis 꺼짐, 읽기 단계 (①)           | `run_agent` 의 첫 `try/except`         | 답은 오지만 앞 대화를 모르는 답                      | 아직 영향 없음                                              |
| Redis 꺼짐, 저장 단계 (⑤)           | `run_agent` 의 둘째 `try/except`       | 답은 정상으로 옴                                     | 이번 턴이 저장되지 않아 다음 질문이 이 턴을 모름            |
| 그래프(LLM/도구)가 예외             | `agent_handler` 의 `except`            | 요청이 FAILED 로 기록됨                              | 저장 안 됨 (⑤ 에 도달하지 못함)                             |
| 재시도 한계 도달 (`give_up_node`)   | 그래프 안                              | "죄송합니다, N번 시도했지만 ..." 답                  | 이 안내문이 "답"으로 저장됨                                 |
| 120초 타임아웃                      | `agent_handler` 의 `wait_for`          | 요청이 FAILED 로 기록됨                              | 스레드가 뒤늦게 끝나면 저장될 수도 있음(예상)               |
| `sessionId` 100자 초과              | Spring `@Valid`                        | 400                                                  | 해당 없음                                                   |
| 존재하지 않는/만료된 세션 조회      | `sessions.get_session`                 | 404                                                  | 해당 없음                                                   |
| Redis 꺼짐, 세션 조회               | `sessions.get_session`                 | 503                                                  | 해당 없음                                                   |


## 4. 한 요청을 따라가 보기 (예시)

세션 `d20-c` 에 질문을 두 번 보낸다고 하자. 아래는 코드를 읽고 정리한 **예상 흐름**이며, 실제 실행 결과가 아니다. 실제 실행 결과는 DAY20_RESULT 3번.

| 순서 | 일어나는 일                                                                  | Redis `agent:conversation:d20-c`                  | 모델이 받는 입력                                              |
| ---- | ---------------------------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------- |
| 1    | 첫 질문 "교환 시 배송비는?" (sessionId=d20-c). ① 읽기: 키 없음 → 빈 리스트   | (없음)                                            | 시스템 프롬프트 + [Q1]                                        |
| 2    | 모델이 도구를 부르고 답 A1 을 만든다 ③. ⑤ 저장: [Q1, A1], TTL 3600초         | `[Q1, A1]`                                        | (그래프 안에서 도구 호출/결과가 추가되나 저장되지 않음)       |
| 3    | 두 번째 질문 "그럼 반품할 때는요?" ① 읽기: [Q1, A1] → ② 질문2 앞에 붙임       | `[Q1, A1]`                                        | 시스템 프롬프트 + [Q1, A1, Q2]                                |
| 4    | 모델이 A2 를 만든다. ⑤ `append_turn` 이 다시 읽어 [Q1, A1] 에 Q2, A2 를 붙여 저장, TTL 다시 3600초 | `[Q1, A1, Q2, A2]`                  | -                                                             |

세 번째 이후에도 같다. 11번째 질문이 저장될 때 가장 오래된 턴이 잘려 나가 항상 최근 10턴(20개 메시지)까지만 남는다. 마지막 저장 후 1시간 동안 같은 세션 요청이 없으면 키가 사라져서 다음 질문은 새 대화가 된다.

### 4-1. 시간 순서로 본 두 워커의 경합 (락이 없을 때, 예상)

같은 세션의 두 요청이 서로 다른 워커 W1, W2 에서 거의 동시에 처리되는 경우다. Kafka 키가 `requestId` 라서 같은 세션의 요청이 다른 파티션, 다른 워커로 갈 수 있다. 코드 흐름상의 예상이며 DAY20_GUIDE ⑧ 로 확인할 항목이다.

| 시각 | W1 (질문 Qa)                        | W2 (질문 Qb)                        | Redis 의 대화                |
| ---- | ----------------------------------- | ----------------------------------- | ---------------------------- |
| t0   | ① 읽기 → `[]`                       | ① 읽기 → `[]`                       | `[]`                         |
| t1   | LLM 처리 중 (20~30초)               | LLM 처리 중                         | `[]`                         |
| t2   | ⑤ `append_turn`: 읽기 `[]`, 쓰기 `[Qa, Aa]` |                                | `[Qa, Aa]`                   |
| t3   |                                     | ⑤ `append_turn`: 읽기 `[Qa, Aa]`, 쓰기 `[Qa, Aa, Qb, Ab]` | `[Qa, Aa, Qb, Ab]` |

이 예에서는 `append_turn` 이 저장 직전에 다시 읽기 때문에 유실이 없다. 유실은 t2 와 t3 의 읽기-쓰기가 서로 겹칠 때(둘이 같은 순간에 `[]` 를 읽고 각자 쓰기) 생긴다. 이 구간은 몇 밀리초라 쉽게 재현되지 않을 수 있다. 또 ① 에서 읽은 대화는 질문 처리 중 20~30초 동안 낡은 상태가 되어, Qb 의 답은 Qa 를 모르는 채로 만들어진다. 이 두 가지가 락(Day 21)과 동시 요청 테스트(Day 22)의 대상이다.


## 5. 설계 선택과 이유

| 선택                                   | 이유                                                                                                                                   | 대신 포기한 것                                                                                      |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 질문과 최종 답만 저장                  | 저장이 단순하고 작다. LangChain 의 도구 메시지(tool_calls 등)를 직렬화하지 않아도 된다                                                  | 앞에서 검색한 문서 원문. 필요하면 모델이 다시 검색한다                                              |
| Redis 오류를 삼키고 답은 돌려줌        | 대화 이어가기가 부가 기능이라 Redis 장애로 질문 자체가 실패하면 손해가 크다                                                              | 장애를 요청 결과에서 알 수 없다(로그에만 남는다). 정말 필요한 기능이면 실패로 올리는 선택도 가능   |
| 동기 Redis 클라이언트                  | `run_agent` 가 이미 별도 스레드에서 돌아서 그대로 쓸 수 있다                                                                            | async 클라이언트(`redis.asyncio`)의 이점. 직접 경로는 어차피 동기로 LLM 을 기다린다                 |
| 대화 전체를 한 키에 JSON 으로          | 구현이 가장 단순하다                                                                                                                    | 동시 요청에서 덮어쓰기(락 없음), 대화가 커지면 매번 전체를 읽고 쓰는 비용                           |
| 최근 10턴만 유지, 요약 없음            | 추가 LLM 호출이 없고 몇 줄이면 된다                                                                                                     | 10턴 이전 내용(CONVERSATION_CONTEXT_NOTES 참고)                                                     |
| `sessionId` 를 클라이언트가 정함       | 세션 발급과 로그인 같은 흐름을 아직 만들지 않았다                                                                                       | 남의 `sessionId` 를 알면 그 대화를 읽거나 이어갈 수 있다(권한 없음, 한계)                            |
| Kafka 키는 그대로 `requestId`          | 이번 범위가 아니다                                                                                                                      | 같은 세션의 요청이 다른 파티션, 다른 워커로 가서 동시에 처리될 수 있다. 키를 `sessionId` 로 바꾸는 방법이 있다 |


## 6. 코드 흐름을 따라 읽는 순서

1. `AgentRequestController.accept` — sessionId 가 어떻게 메시지에 실리는지 (Spring)
2. `KafkaWorker._handle` → `agent_handler.agent_handler` — 메시지가 해석되어 `run_agent` 로 넘어가는 한 줄
3. `agent_graph.run_agent` — ①~⑤ 주석을 따라 읽는다 (핵심)
4. `agent_graph.agent_node` — `SYSTEM_PROMPT + messages` 가 모델에 가는 곳
5. `conversation_store.append_turn` — 저장이 어떻게 이루어지는지
6. `redis_client.set_json`, `get_json` — 실제 Redis 호출
7. `sessions.get_session` — 저장된 결과를 보는 방법

`run_agent` 와 `AgentRequestController.accept` 에는 흐름 번호가 코드 주석으로 들어 있어서 파일을 열면 위 순서가 그대로 보인다.


## 7. 확인하지 않은 것

| 항목                                              | 내용                                                                                                          |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Spring 경로의 이어가기 품질                       | `d20-c` 로 전체 경로를 실행해 저장은 확인했다(DAY20_RESULT 3-4). 두 번째 답이 앞 대화를 이용했는지는 세션 없는 비교를 하지 않아 모른다. `sessionId` 빈 문자열(→ null), 101자(→ 400)도 실행하지 않았다 |
| 서버 로그의 `[timing] 이전 대화 N개 메시지`       | 확인하지 않았다                                                                                               |
| Redis 에 저장된 값 직접 보기                      | `redis-cli` 로 보지 않았고, `GET sessions/d20-b` 만 확인했다. d20-a 의 `sessions` 조회는 하지 않았다           |
| Redis 중지 시 동작 (3번 표의 첫 두 줄)            | 실행해 보지 않았다                                                                                            |
| 같은 세션 동시 요청 (4-1)                         | 워커 두 대로 한 번 실행했을 때 두 턴이 모두 저장됐다(DAY20_RESULT 3-6). 겹치게 만든 실험이 아니라서 유실이 일어나는지는 모른다 |
| 타임아웃 뒤 저장 여부, `give_up` 안내문 저장      | 코드를 읽은 예상이다                                                                                          |
| 대화가 길 때의 지연                               | 이전 대화가 길수록 LLM 입력이 늘어 더 느려지는지 재지 않았다                                                   |
