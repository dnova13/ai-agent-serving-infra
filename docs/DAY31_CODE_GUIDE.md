# Day 31 코드 가이드 — 도구 실패가 두 갈래로 갈리는 과정

> 실행 방법과 확인 항목은 [DAY31_GUIDE.md](DAY31_GUIDE.md), 결과는 [DAY31_RESULT.md](DAY31_RESULT.md). 이 문서는 **코드가 어떤 순서로 무엇을 하는지**를 파일 위치와 함수 단위로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다. 괄호의 숫자는 작성 시점(2026-10-17)의 줄이다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, 9092 = Kafka, 6379 = Redis, 6333 = Qdrant. 시나리오 검증(`selfcorrect_check`)은 이 중 아무것도 쓰지 않는다.

**목적:** Day 6 에서 만든 "도구가 실패하면 같은 호출을 최대 2번 재실행"을 실패 종류에 따라 둘로 나눈다. 연결 끊김 같은 **일시 장애**는 지금처럼 같은 호출을 다시 실행하고, 같은 입력으로 다시 해도 소용없는 **입력 오류**는 실패 원인을 모델에게 돌려줘 입력을 고쳐 다시 부르게 한다.

**Day 31 의 코드 변경:** 커밋 `feb7c11` 하나. `agent_graph.py`(+160줄), `fake_llm.py`(+41줄), `metrics.py`(+24줄), 새 파일 `selfcorrect_check.py`. Day 6 의 그래프 모양(agent → tools → agent, 실패 시 give_up)은 그대로이고 `tool_node`, `after_tools_router`, `give_up_node` 안이 바뀌었다.


## 1. 전체 흐름

```
agent_node (모델이 도구 호출을 정함)
   │
   ▼
tool_node ── 도구마다 실행 ──▶ 성공 / 입력 오류(input) / 일시 장애(transient) 로 분류
   │            └ 실패 지표 센다(TOOL_FAILURES), 입력 오류면 결과 문자열 뒤에 "[정정 안내]" 를 붙임
   ▼
after_tools_router
   ├ 성공                                   → agent   (모델이 결과를 보고 답을 쓴다)
   ├ 입력 오류, correction_count ≤ 2        → agent   (모델이 실패 원인을 읽고 입력을 고쳐 다시 부른다)
   ├ 입력 오류, correction_count > 2        → give_up
   ├ 일시 장애, retry_count < 2             → tools   (모델을 거치지 않고 같은 호출 재실행)
   └ 일시 장애, retry_count ≥ 2             → give_up
give_up_node → 포기 이유에 맞는 문구로 답하고 종료 (HTTP 200, 워커 실패 아님)
```

| 단계 | 하는 일                                               | 파일                                              | 함수 (줄)                                    |
| ---- | ----------------------------------------------------- | ------------------------------------------------- | -------------------------------------------- |
| A    | 한도와 입력 오류 예외, 상태 필드                      | `d2-ai-python/app/services/agent_graph.py`        | `MAX_RETRIES`, `MAX_CORRECTIONS`, `ToolInputError`, `AgentState` (62~98) |
| B    | 도구가 입력 오류를 던지는 곳                          | `d2-ai-python/app/services/agent_graph.py`        | `search_vector_db` (129)                     |
| C    | 도구 실행, 실패 분류, 정정 안내 붙이기, 복구 세기     | `d2-ai-python/app/services/agent_graph.py`        | `tool_node` (230)                            |
| D    | 실패 뒤 어디로 갈지 정하기                            | `d2-ai-python/app/services/agent_graph.py`        | `after_tools_router` (367)                   |
| E    | 포기 문구와 포기 지표                                 | `d2-ai-python/app/services/agent_graph.py`        | `give_up_node` (320)                         |
| F    | 정정 안내를 읽고 입력을 고치는 가짜 모델              | `d2-ai-python/app/services/fake_llm.py`           | `FakeAgentChatModel` (59)                    |
| G    | 지표 정의                                             | `d2-ai-python/app/services/metrics.py`            | `TOOL_FAILURES`, `TOOL_RECOVERIES`, `GIVE_UPS` |
| H    | 시나리오 7개 확인 스크립트                            | `d2-ai-python/app/checks/selfcorrect_check.py`  | `SCENARIOS`, `run_one`, `main`               |


## 2. 주요 코드

### 2-1. 한도, 예외, 상태 (A)

**목적:** 두 종류의 실패를 따로 세고, "직전 실패가 어떤 종류였는지"를 상태에 남긴다.

```python
MAX_RETRIES = 2        # 일시 장애: 같은 호출을 다시 실행하는 횟수 (Day 6)
MAX_CORRECTIONS = 2    # 입력 오류: 모델에게 돌려보내 고치게 하는 횟수 (Day 31)

class ToolInputError(ValueError):
    """같은 입력으로 다시 실행해도 소용없는 실패. 모델이 입력을 고쳐 다시 불러야 한다."""

class AgentState(TypedDict):
    ...
    retry_count: int
    correction_count: int        # [NEW Day31]
    last_failure_kind: str       # [NEW Day31] "" / "transient" / "input"
```

| 항목                  | 의미                                                                                                                       |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `MAX_RETRIES`         | 일시 장애 재실행 한도. 한도에 닿으면 포기                                                                                  |
| `MAX_CORRECTIONS`     | 입력 오류 정정 한도. 정정은 2번까지라서 도구 호출은 처음 1번 + 정정 2번 = 최대 3번                                         |
| `ToolInputError`      | 도구가 "입력이 잘못됐다"고 알리는 전용 예외                                                                                |
| `correction_count`    | 이번 요청에서 입력 오류가 난 횟수. 요청 안에서 누적되고 성공해도 0 으로 돌아가지 않는다                                     |
| `last_failure_kind`   | 직전 도구 실행의 결과. 성공이면 `""`. 복구를 어느 쪽으로 셀지, 포기 문구를 무엇으로 할지 정하는 데 쓴다                      |

`_run_agent_turn` 의 초기 상태에는 `retry_count: 0` 만 있고 새 필드는 넣지 않았다. 그래서 `tool_node` 와 `after_tools_router` 는 `state.get("correction_count", 0)` 처럼 기본값을 주고 읽는다. `selfcorrect_check.py` 의 초기 상태에는 두 필드를 명시했다.

### 2-2. 도구가 입력 오류를 던지는 곳 (B)

- 파일: `agent_graph.py` (`search_vector_db`)

```python
if "강제실패" in query:                      # Day 6: 일시 장애 흉내 (RuntimeError)
    raise RuntimeError("의도적으로 발생시킨 테스트용 실패 (retry_count 검증용)")
if not query or not query.strip():          # [NEW Day31] 빈 검색어
    raise ToolInputError("검색어가 비어 있습니다. ...")
for _bad in ("교정테스트", "교정불가"):       # [NEW Day31] 테스트 전용 단어
    if _bad in query:
        raise ToolInputError(f"검색어에 '{_bad}' 가 들어 있어 검색할 수 없습니다. 이 단어를 뺀 ...")
```

| 던지는 예외         | 분류        | 의미                                                                                  |
| ------------------- | ----------- | ------------------------------------------------------------------------------------- |
| `RuntimeError`      | 일시 장애   | `강제실패` 가 들어 있을 때. Day 6 의 재시도 검증용이라 그대로 두었다                  |
| `ToolInputError`    | 입력 오류   | 빈 검색어, 또는 `교정테스트` / `교정불가` 가 들어 있을 때                             |

`교정테스트` 는 단어를 빼면 성공하고, `교정불가` 는 가짜 모델이 못 고치는 경우를 만들려는 것이다. 이 단어 검사는 모델이 정정 안내를 읽고 따르는지 보려는 테스트 전용 코드다. Qdrant 나 임베딩에서 나는 예외(연결 실패, 429 등)는 `Exception` 으로 올라와 일시 장애가 된다.

### 2-3. tool_node: 분류, 안내 붙이기, 복구 세기 (C)

**목적:** 도구를 실행해 실패를 두 종류로 나누고, 상태 갱신과 지표 기록, 모델에게 줄 메시지를 한 곳에서 만든다.

```python
for tool_call in state["pending_tool_calls"]:
    tool_fn = TOOLS_BY_NAME.get(tool_call["name"])
    kind = None
    try:
        if tool_fn is None:
            raise ToolInputError(f"알 수 없는 도구: {tool_call['name']}")     # (a)
        content = tool_fn.invoke(tool_call["args"])
    except (ToolInputError, ValidationError) as e:                           # (b)
        content = f"[도구 실행 실패] {e}"; kind = "input"
    except Exception as e:                                                   # (c)
        content = f"[도구 실행 실패] {e}"; kind = "transient"
    if kind:
        metrics.TOOL_FAILURES.labels(tool=tool_call["name"], kind=kind).inc()
    done.append((tool_call, content, kind))

kinds = {k for _, _, k in done if k}
failure_kind = "transient" if "transient" in kinds else ("input" if "input" in kinds else "")   # (d)
new_retry_count = state["retry_count"] + (1 if failure_kind == "transient" else 0)
new_correction_count = state.get("correction_count", 0) + (1 if failure_kind == "input" else 0)
corrections_left = max(0, MAX_CORRECTIONS - (new_correction_count - 1))                         # (e)
```

| 표시 | 하는 일                                                                                                                                                                     |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a)  | 모델이 없는 도구 이름을 부르면 입력 오류로 본다                                                                                                                              |
| (b)  | `ToolInputError` 와 pydantic `ValidationError`(인자 형식이 안 맞음)는 입력 오류                                                                                              |
| (c)  | 그 밖의 모든 예외는 일시 장애. 어느 쪽인지 모를 때는 같은 호출을 다시 해 보는 쪽(Day 6 동작)이 기본이다                                                                      |
| (d)  | 한 번에 여러 도구를 불러 종류가 섞이면 **transient 로 본다**. 재실행은 이번에 부른 도구를 전부 다시 실행하므로, 입력 오류인 도구는 다시 해도 같은 결과가 나오지만 일시 장애인 도구는 회복될 수 있기 때문이다 |
| (e)  | 이번 정정이 몇 번째인지에서 남은 기회를 계산한다. 1번째 정정이면 2, 2번째면 1, 3번째면 0                                                                                       |

이어서 결과 메시지를 만든다.

```python
for tool_call, content, kind in done:
    if kind == "input" and failure_kind == "input" and corrections_left > 0:
        content += "\n[정정 안내] 같은 입력으로 다시 호출하지 말고 위 실패 원인을 읽고 입력을 고쳐 다시 호출하세요. " \
                   f"남은 정정 기회: {corrections_left}번. 고칠 수 없으면 사용자에게 사정을 알리세요."
    results.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))

if not kinds and state.get("last_failure_kind"):                             # (f)
    via = "retry" if state["last_failure_kind"] == "transient" else "correction"
    metrics.TOOL_RECOVERIES.labels(via=via).inc()
```

| 부분 | 내용                                                                                                                                                                            |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 정정 안내 | 실패 원인 문자열(`[도구 실행 실패] ...`) 뒤에 이어 붙인다. 따로 메시지를 만들지 않는다. 마지막 기회(0)를 넘긴 실패에는 안내가 붙지 않는다 |
| (f)  | 이번에 모두 성공했고 직전에 실패가 있었으면 복구다. **복구는 직전 실패의 종류로 센다**. 정정 뒤에 일시 장애가 나서 재실행으로 성공했으면(시나리오 G) `via=retry` 다                   |

반환값은 `messages`(도구 결과), `retry_count`, `correction_count`, `last_failure_kind` 네 필드다.

### 2-4. after_tools_router: 실패 뒤 어디로 갈지 (D)

```python
failed = isinstance(last_tool_message, ToolMessage) and "[도구 실행 실패]" in str(last_tool_message.content)
if failed:
    if state.get("last_failure_kind") == "input":
        if state.get("correction_count", 0) > MAX_CORRECTIONS:
            return "give_up"
        return "agent"                        # 모델이 읽고 고치게 한다
    if state["retry_count"] >= MAX_RETRIES:
        return "give_up"
    return "tools"                            # 모델을 거치지 않고 같은 호출을 다시 실행
return "agent"
```

| 갈래                         | 이유                                                                                                                                                                            |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 일시 장애 → `tools`          | Day 6 에서 실측한 결과, 실패하면 모델에게 돌려보내 "재시도할지 스스로 판단하게" 했더니 모델이 1번 실패하고 바로 포기했다. 그래서 그래프가 모델을 거치지 않고 결정적으로 재실행한다(코드의 `[OLD]` 주석) |
| 입력 오류 → `agent`          | 입력을 고치는 일은 그래프가 할 수 없고 모델만 할 수 있다. 그래서 일시 장애와 반대로 모델에게 돌려보낸다                                                                          |
| `correction_count > 2`       | 정정이 3번째 실패까지 이어지면(처음 + 정정 2번 모두 실패) 포기한다. 이때는 이미 안내가 붙지 않은 마지막 실패다                                                                    |

모델이 안내를 읽지 않고 사과 문구로 끝내면(도구를 다시 부르지 않으면) 그래프는 그 답으로 그대로 종료한다. Day 6 에서 본 동작이고 이번에도 고치지 않았다.

### 2-5. give_up_node: 포기 이유에 맞는 문구 (E)

```python
if state.get("last_failure_kind") == "input":
    reason = "correction_exhausted"; text = "죄송합니다, 검색 조건을 바꿔 2번 다시 시도했지만 ..."
else:
    reason = "retry_exhausted";      text = "죄송합니다, 2번 시도했지만 ..."
metrics.GIVE_UPS.labels(reason=reason).inc()
return {"messages": [AIMessage(content=text)]}
```

포기해도 정상 응답(`DONE`)이다. 워커 실패(`FAILED`)가 아니므로 Day 32 의 DLQ 대상도 아니다.

### 2-6. 가짜 모델과 지표 (F, G)

`FakeAgentChatModel` 은 provider `fake-agent`(`FAKE_LLM_ENABLED=true` 일 때만)로 선택되며, 대화를 보고 정해진 대로 움직인다.

| 마지막 메시지                                        | 모델의 행동                                                                                |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| 사용자 질문                                          | 질문을 검색어로 `search_vector_db` 를 부른다                                                |
| 도구 결과에 `[도구 실행 실패]` + `[정정 안내]`       | 질문에서 `교정테스트` 를 뺀 검색어로 다시 부른다 (`교정불가` 는 못 고치므로 그대로 둔다)   |
| 도구 결과에 `[도구 실행 실패]` 만 있음               | 도구를 부르지 않고 "도구 실패로 답하지 못했습니다"로 끝낸다                                |
| 도구 성공 결과                                       | "검색 결과 N자를 받아 답했습니다"                                                          |

기존 `FakeChatModel`(Day 25)은 도구를 부르지 않아 재시도나 정정 경로가 실행되지 않으므로 새로 만들었다.

| 지표                               | 라벨           | 세는 곳            |
| ---------------------------------- | -------------- | ------------------ |
| `agent_tool_failures_total`        | `tool`, `kind` | `tool_node` (실패한 도구마다) |
| `agent_tool_recoveries_total`      | `via`          | `tool_node` (복구 때)         |
| `agent_give_ups_total`             | `reason`       | `give_up_node`                |


## 3. 시간 순서

### 3-1. 시나리오 D: 입력 오류 후 정정 성공 (질문 "교정테스트 환불 정책 알려줘")

```
1. agent_node   모델 호출 #1  → search_vector_db(query="교정테스트 환불 정책 알려줘")
2. tool_node    ToolInputError → kind=input, correction_count=1, 남은 기회 2
                결과 = "[도구 실행 실패] ... [정정 안내] ... 남은 정정 기회: 2번 ..."   TOOL_FAILURES{input}+1
3. router       failed, kind=input, 1 > 2 아님 → agent
4. agent_node   모델 호출 #2  → 안내를 읽고 query="환불 정책 알려줘"
5. tool_node    성공. kinds 비어 있고 last_failure_kind="input" → TOOL_RECOVERIES{correction}+1
6. router       성공 → agent
7. agent_node   모델 호출 #3  → 최종 답
결과: 도구 2번, 모델 3번, retry 0, correction 1
```

### 3-2. 시나리오 G: 정정 뒤에 일시 장애

```
1~3. D 와 같음 (입력 오류 → 정정 안내 → 모델에게)
4.   agent_node   모델 호출 #2 → 고친 검색어
5.   tool_node    가짜 검색이 ConnectionError → kind=transient, retry_count=1
6.   router       failed, kind=transient, 1 < 2 → tools   (모델을 거치지 않음)
7.   tool_node    같은 호출 재실행, 성공 → last_failure_kind="transient" 이므로 TOOL_RECOVERIES{retry}+1
8.   router → agent → 모델 호출 #3 → 최종 답
결과: 도구 3번, 모델 3번, retry 1, correction 1
```

### 3-3. 시나리오 E: 입력 오류를 못 고침 (`교정불가`)

```
tool_node #1 실패: correction_count=1, 남은 기회 2 → 안내 붙음 → router: agent
tool_node #2 실패: correction_count=2, 남은 기회 1 → 안내 붙음 → router: agent
tool_node #3 실패: correction_count=3, 남은 기회 0 → 안내 없음 → router: 3 > 2 → give_up
give_up_node: "검색 조건을 바꿔 2번 ..." / GIVE_UPS{correction_exhausted}+1
결과: 도구 3번, 모델 3번, retry 0, correction 3
```


## 4. 실패 지점별 동작

| 상황                                        | 동작                                                                                                       |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| 연결 끊김, 타임아웃, 429                    | 일시 장애. 같은 호출을 최대 2번 재실행하고 그래도 실패하면 포기 (시나리오 B, C)                            |
| 없는 도구 이름, 인자 형식 오류              | 입력 오류. 모델에게 원인과 안내를 돌려줘 고치게 함                                                         |
| 한 번에 여러 도구, 실패 종류가 섞임         | 일시 장애로 취급해 전부 재실행                                                                             |
| 모델이 안내를 읽고도 도구를 다시 안 부름    | 모델의 답으로 그대로 종료 (포기 문구가 아니다)                                                             |
| 정정 한도(2번) 초과                         | `give_up_node` 가 "검색 조건을 바꿔 ..." 문구로 응답, 지표 `correction_exhausted`                          |
| 재시도 한도(2번) 초과                       | `give_up_node` 가 "N번 시도했지만 ..." 문구로 응답, 지표 `retry_exhausted`                                  |
| `강제실패`                                  | 일시 장애로 취급되어 Day 6 과 똑같이 도구 2번 실행 후 포기 (시나리오 F)                                    |


## 5. 설계 선택과 이유

| 선택                                                         | 이유                                                                                                                       |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| 카운터를 `retry_count` 와 `correction_count` 로 나눔         | 한도의 의미가 다르다. 재실행은 비용이 거의 없고, 정정은 모델 호출(시간과 토큰)을 쓴다. 한 숫자로 합치면 둘 중 하나의 한도를 못 맞춘다 |
| 정정 안내를 도구 결과 문자열 뒤에 이어 붙임                  | 모델은 도구 결과(`ToolMessage`)를 읽고 다음 행동을 정한다. 안내를 그 결과에 붙이면 메시지 순서(도구 호출 → 도구 결과)를 바꾸지 않고 전달할 수 있다 |
| 섞인 실패는 일시 장애로 봄                                   | 재실행이 이번에 부른 도구를 전부 다시 실행하므로, 일시 장애 쪽이 하나라도 있으면 재실행이 의미가 있다                         |
| 복구를 직전 실패의 종류로 셈                                 | "무엇이 문제를 해결했나"를 지표로 보려는 것이다. 정정 뒤 일시 장애가 재실행으로 풀렸으면 그 복구는 retry 다                    |
| 포기 문구를 이유별로 나눔                                    | 사용자에게 "잠시 후 다시"(일시 장애)와 "질문을 바꿔서"(입력 오류)는 다른 안내여야 한다                                       |
| 포기를 정상 응답(DONE)으로 처리                              | 사용자는 답을 받았다. 워커 실패로 보면 DLQ 로 가서 재처리 대상이 되는데, 같은 질문을 다시 처리해도 결과는 같다                  |
| `[OLD]` 코드를 주석으로 남김                                 | Day 6 에서 모델에게 맡겼을 때 포기해 버린 실측 기록이 코드 안에 남아 있다                                                    |


## 6. 코드를 따라 읽는 순서

1. `agent_graph.py` 맨 위 docstring과 상수(`MAX_RETRIES`, `MAX_CORRECTIONS`, `ToolInputError`, `AgentState`)
2. `search_vector_db` 의 입력 오류 검사
3. `tool_node` (분류 → 안내 붙이기 → 복구 세기)
4. `after_tools_router` → `give_up_node` → `build_graph`
5. `fake_llm.py` 의 `FakeAgentChatModel`
6. `selfcorrect_check.py` 의 `SCENARIOS` 표와 `main` 의 검증 항목
7. [DAY31_RESULT.md](DAY31_RESULT.md) 3-1 의 시나리오별 표로 위 흐름이 실제로 나온 숫자를 대조
