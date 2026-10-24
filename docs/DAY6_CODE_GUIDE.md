# Day 6 코드 가이드 — 도구 실패 시 결정적 재시도 (retry_count)

> 실행하는 방법과 확인 항목은 [DAY6_GUIDE.md](DAY6_GUIDE.md), 당시 테스트 로그는 [DAY6_TEST_LOG.md](DAY6_TEST_LOG.md). 이 문서는 재시도 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 코드가 이후 Day 에 계속 바뀌어서 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI**.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 아래 `[DEBUG]` 로그 순서는 DAY6_GUIDE 에 당시 기록으로 적힌 것을 옮겼다. 설명한 코드는 저장소의 가장 오래된 기록(Day 7 시점 첫 커밋)의 `agent_graph.py` 이다.

**전체 목적:** 도구가 예외를 내면 그래프가 같은 도구 호출을 정해진 횟수(`MAX_RETRIES`)만큼 다시 실행하고, 그래도 안 되면 포기 문구로 끝나는 흐름을 코드 순서대로 이해한다.


## 1. 전체 흐름

```
        START
          │
          ▼
        agent ── 도구 필요 없음 ──▶ END
          │ tool_calls 있음 (pending_tool_calls 에 요청을 저장)
          ▼
        tools ── 도구 실행. 예외가 나면 실패로 기록하고 retry_count + 1
          │ after_tools_router
          ├── 실패 아님 ─────────────────────────▶ agent (결과를 보고 최종 답)
          ├── 실패, retry_count < MAX_RETRIES(2) ─▶ tools (같은 요청을 그대로 다시)
          └── 실패, retry_count ≥ MAX_RETRIES ───▶ give_up ──▶ END (포기 문구)
```

### 1-1. 단계별 함수

| 단계 | 하는 일                                          | 함수                                   |
| ---- | ------------------------------------------------ | -------------------------------------- |
| A    | 모델이 도구를 요청하면 그 요청을 따로 저장        | `agent_node` (`pending_tool_calls`)    |
| B    | 저장된 요청을 실행, 예외는 잡아서 실패 표시        | `tool_node`                            |
| C    | 실패 여부와 횟수로 다음 노드 결정                  | `after_tools_router`                   |
| D    | 포기 문구로 끝내기                                 | `give_up_node`                         |
| E    | 위를 그래프로 연결                                 | `build_graph`                          |
| F    | 시험용 실패 만들기                                 | `search_vector_db` 의 `"강제실패"` 분기 |


## 2. 주요 코드

### 2-1. 상태에 요청을 보관 (`AgentState`, `agent_node`)

```python
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    provider: str
    retry_count: int
    pending_tool_calls: list        # Day 6 에서 추가

def agent_node(state):
    ...
    pending = response.tool_calls if getattr(response, "tool_calls", None) else []
    return {"messages": [response], "pending_tool_calls": pending}
```

왜 필요한가: 실패해서 `tools` 를 다시 실행할 때 `state["messages"][-1]` 은 방금 실패한 `ToolMessage` 라서 원래의 도구 요청(`tool_calls`)이 아니다. 요청을 따로 들고 있어야 "같은 요청을 그대로" 다시 실행할 수 있다(코드의 `[OLD]` 주석이 이 문제를 설명한다).

### 2-2. 도구 실행과 실패 기록 (`tool_node`)

```python
for tool_call in state["pending_tool_calls"]:
    tool_fn = TOOLS_BY_NAME.get(tool_call["name"])
    try:
        if tool_fn is None:
            raise ValueError(f"알 수 없는 도구: {tool_call['name']}")
        content = tool_fn.invoke(tool_call["args"])
    except Exception as e:
        content = f"[도구 실행 실패] {e}"
        had_error = True
    results.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))

new_retry_count = state["retry_count"] + 1 if had_error else state["retry_count"]
return {"messages": results, "retry_count": new_retry_count}
```

| 부분                         | 설명                                                                                                                        |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| 예외를 잡아 문자열로 바꿈    | 실패해도 그래프가 터지지 않는다. 결과는 `[도구 실행 실패] ...` 로 시작하는 `ToolMessage` 다                                  |
| `retry_count` 는 실패한 때만 증가 | 성공하면 그대로다. 한 번의 `tool_node` 실행에서 도구가 여러 개 실패해도 1만 증가한다(`had_error` 가 참/거짓 하나이기 때문)  |

### 2-3. 다음 길 고르기 (`after_tools_router`)

```python
failed = isinstance(last_tool_message, ToolMessage) and "[도구 실행 실패]" in str(last_tool_message.content)
if failed:
    if state["retry_count"] >= MAX_RETRIES:
        return "give_up"
    return "tools"        # 모델을 거치지 않고 같은 요청을 그대로 다시
return "agent"
```

`MAX_RETRIES = 2` 일 때의 흐름: 첫 실패 후 `retry_count=1` 이라 `tools` 로 다시 간다. 두 번째 실패 후 `retry_count=2` 라 `give_up` 으로 간다. 따라서 도구는 최대 두 번 실행된다(처음 한 번과 재시도 한 번). `give_up_node` 의 문구가 "2번 시도했지만"인 것과 맞는다. DAY6_GUIDE 에 적힌 당시 로그도 `retry_count=1`, `retry_count=2`, `give_up_node 진입` 순서다.

실패 여부를 `"[도구 실행 실패]"` 라는 문자열로 판단한다. 도구의 정상 결과에 이 문구가 우연히 들어가면 실패로 오인할 수 있는 구조다.

### 2-4. 포기와 그래프 연결 (`give_up_node`, `build_graph`)

```python
graph.add_conditional_edges("tools", after_tools_router,
                            {"tools": "tools", "agent": "agent", "give_up": "give_up"})
graph.add_edge("give_up", END)
```

`tools → tools` 로 자기 자신에게 돌아오는 길이 이 Day 에 생겼다. `give_up_node` 는 모델을 부르지 않고 고정된 사과 문구를 `AIMessage` 로 돌려준다.

### 2-5. 왜 `agent` 가 아니라 `tools` 로 돌려보내는가

| 방식                                | 결과                                                                                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [OLD] 실패하면 `agent` 로             | 모델이 한 번 실패한 뒤 도구를 다시 부르지 않고 사과 답을 써서 끝났다(`should_continue` 가 `tool_calls` 없음을 보고 `END`). `retry_count=1` 에서 멈췄고 `give_up_node` 에 닿지 못했다 |
| [NEW] 실패하면 `tools` 로            | 모델의 판단을 거치지 않고 그래프가 같은 요청을 다시 실행한다. 일시적인 기술 오류는 시스템이 기계적으로 처리하는 영역이라는 판단이었다 |

### 2-6. 시험용 실패 (`search_vector_db`)

```python
if "강제실패" in query:
    raise RuntimeError("의도적으로 발생시킨 테스트용 실패 (retry_count 검증용)")
```

질문에 `강제실패` 가 들어가면 도구가 예외를 낸다. 모델이 이 단어를 개념 질문으로 오해해 도구를 아예 부르지 않는 경우가 있어서, 테스트 질문은 "환불 정책이 어떻게 되나요? (검색 키워드: 강제실패)" 처럼 자연스러운 문장으로 바꿨다(코드 맨 아래 `__main__` 의 주석). 이 분기는 이후 Day 의 `search_vector_db` 에도 유지되어, 재시도 로직의 회귀 시험에 쓰인다.

### 2-7. 이 Day 에 함께 바뀐 것

| 항목                  | 내용                                                                                                                             |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `get_current_time` 도구 | 시간 질문용 도구. `AVAILABLE_TOOLS` 가 2개가 되어 분기 경로(검색, 시간, 도구 없음)가 세 가지가 됐다                               |
| Gemini 모델 교체      | 일일 무료 한도에 걸려 `gemini-3.1-flash-lite` 로 바꿨다(DAY6_GUIDE 의 쿼터 이슈 부분). 코드에 이전 모델 이름이 주석으로 남아 있다 |
| `[DEBUG]` 로그        | `after_tools_router`, `give_up_node` 에 `print` 로 남겼다. DAY6_GUIDE 는 이후에도 디버깅에 쓰려고 지우지 않기로 했다고 적었다     |


## 3. 결과를 읽는 순서

1. 실패 시험을 돌렸을 때 콘솔에 `after_tools_router 호출됨: failed=True, retry_count=1`, `retry_count=2`, `give_up_node 진입함` 이 이 순서로 나오는가.
2. 최종 답이 `give_up_node` 의 고정 문구와 같은가(모델이 지어낸 사과문이 아니라).
3. 정상 질문은 `failed=False` 로 곧바로 `agent` 로 가서 최종 답을 쓰는가.


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                         |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                          | 이 문서를 쓰면서 그래프를 실행하지 않았다. 로그 순서는 DAY6_GUIDE 의 당시 기록을 옮긴 것이다                                                    |
| 도구가 여러 개 동시에 실패    | `retry_count` 가 1 만 증가한다는 것은 코드에서 읽은 것이고 실행해 보지 않았다                                                                  |
| 실패 판단의 문자열 의존       | 정상 결과에 `[도구 실행 실패]` 가 들어가는 경우는 시험하지 않았다                                                                              |
