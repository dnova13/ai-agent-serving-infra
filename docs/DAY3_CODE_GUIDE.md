# Day 3 코드 가이드 — LangGraph 로 바꾼 에이전트 흐름

> 실행하는 방법과 확인 항목은 [DAY3_GUIDE.md](DAY3_GUIDE.md), LangChain 수동 루프와 LangGraph 의 개념 비교는 [CONCEPTS_LANGCHAIN_VS_LANGGRAPH.md](CONCEPTS_LANGCHAIN_VS_LANGGRAPH.md). 이 문서는 바뀐 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 코드가 이후 Day 에 계속 바뀌어서 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI**.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명에 쓴 `agent_graph.py` 는 가장 오래된 저장소 기록(Day 7 시점의 첫 커밋)과 DAY3_GUIDE 를 기준으로 하며, 그 파일에는 Day 6 에 더한 재시도 부분이 이미 들어 있다. 그 부분은 [DAY6_CODE_GUIDE.md](DAY6_CODE_GUIDE.md) 에서 따로 설명하고, 여기서는 Day 3 의 뼈대만 다룬다.

**전체 목적:** Day 2 의 "호출 두 번" 수동 루프가 LangGraph 의 State, Node, Conditional Edge 로 바뀌면서, 같은 요청이 코드에서 어떤 경로로 처리되는지 따라간다.


## 1. 전체 흐름

```
POST /api/v1/agent/run → routers/agent.py (import 한 줄만 agent_core 에서 agent_graph 로 바뀜)
  ▼
services/agent_graph.py 의 run_agent(question, provider)
  │ initial_state = {"messages": [질문], "provider": ..., "retry_count": 0, ...}
  │ AGENT_GRAPH.invoke(initial_state)
  ▼
        START
          │
          ▼
        agent  ← 모델을 부른다. 도구가 필요한지 판단
          │ should_continue
          ├── 응답에 tool_calls 가 있다 ──▶ tools ──▶ agent (결과를 보고 다시 판단)
          └── 없다 ─────────────────────▶ END
  ▼
마지막 메시지의 content 에서 글자를 꺼내 문자열로 응답
```

### 1-1. Day 2 와 Day 3 의 같은 곳, 다른 곳

| 항목                | Day 2 (`agent_core.run_agent`)                | Day 3 (`agent_graph`)                                              |
| ------------------- | --------------------------------------------- | ------------------------------------------------------------------ |
| 반복                | 1차 호출, 도구 실행, 2차 호출로 고정           | agent 와 tools 사이를 도는 그래프. 2차 호출 뒤에도 도구를 요청하면 다시 tools 로 간다 |
| 도구가 필요 없을 때 | 1차 호출 결과를 바로 반환                      | agent 한 번 실행 뒤 `END`                                          |
| 상태                | 함수 안의 지역 변수 `messages`                 | `AgentState` (`messages`, `provider`, `retry_count`)                |
| API                 | `routers/agent.py` 가 `agent_core.run_agent` 호출 | 같은 모양(`run_agent(question, provider)`)이라 import 한 줄만 바뀜 |


## 2. 주요 코드

### 2-1. 상태 (`AgentState`)

```python
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]   # 대화. add_messages 가 새 메시지를 뒤에 붙여 준다
    provider: str                             # "gemini" | "claude"
    retry_count: int                          # Day 3 에서는 선언만. Day 6 에서 쓰기 시작
    # pending_tool_calls: list                # Day 6 에서 추가됨 (DAY6_CODE_GUIDE 2-1)
```

노드는 상태를 통째로 바꾸지 않고, 바꿀 필드만 담은 사전을 돌려준다(`{"messages": [response]}`). `add_messages` 는 그것을 기존 목록 뒤에 붙인다.

### 2-2. 노드 (`agent_node`, `tool_node`)

```python
def agent_node(state):
    model = get_model(state["provider"])
    model_with_tools = model.bind_tools(AVAILABLE_TOOLS)
    response = model_with_tools.invoke(state["messages"])
    return {"messages": [response]}          # Day 6 에서 pending_tool_calls 도 함께 돌려주도록 바뀜
```

| 부분                         | 설명                                                                                                          |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `get_model(state["provider"])` | 요청마다 모델을 새로 만든다. 상태의 `provider` 가 그 값을 나른다                                              |
| `tool_node`                  | 모델이 요청한 도구를 이름으로 찾아(`TOOLS_BY_NAME`) 실행하고, 결과를 `ToolMessage` 로 돌려준다. Day 3 에는 실패 처리가 없었고, 예외 처리와 `retry_count` 증가는 Day 6 에 들어왔다 |

### 2-3. 분기 (`should_continue`)

```python
def should_continue(state):
    last_message = state["messages"][-1]
    if getattr(last_message, "tool_calls", None):
        return "tools"
    return END
```

agent 가 돌려준 마지막 메시지에 `tool_calls` 가 있으면 `tools`, 없으면 끝이다. Day 2 의 `if not ai_response.tool_calls: return ...` 이 같은 판단이다.

### 2-4. 그래프 조립과 실행 (`build_graph`, `run_agent`)

```python
graph = StateGraph(AgentState)
graph.add_node("agent", agent_node)
graph.add_node("tools", tool_node)
graph.set_entry_point("agent")
graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
# tools 가 끝나면 agent 로 되돌아간다 (Day 6 에서 이 줄이 after_tools_router 로 바뀜)
return graph.compile()

AGENT_GRAPH = build_graph()      # 모듈을 불러올 때 한 번 만들어 두고 요청마다 invoke 만 한다
```

`run_agent` 는 처음 상태를 만들어 `AGENT_GRAPH.invoke(...)` 하고, 마지막 메시지의 `content` 가 블록 목록이면 `text` 블록만 이어 붙여 문자열로 돌려준다(Day 2 의 `_extract_text` 와 같은 일).


## 3. 결과를 읽는 순서

1. 같은 질문에 Day 2 와 Day 3 의 답 내용이 같은가. DAY3_GUIDE 는 도구를 한 번만 부르는 단순한 질문에서는 두 방식의 답 텍스트가 같았다고 적고 있다.
2. 차이는 구조에서 나온다. 도구가 여러 번 필요한 질문이나 실패 처리(Day 6)를 붙일 때 그래프 쪽이 고치기 쉽다는 것이 전환의 이유였다(DAY3_GUIDE 왜 이 작업을 했는가).


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                              |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Day 3 시점 코드 원문          | 저장소의 가장 오래된 기록이 Day 7 시점이라 Day 3 때의 파일을 그대로 보지는 못했다. 2번의 코드는 그 파일과 DAY3_GUIDE 에서 Day 3 에 해당하는 부분만 추린 것이다 |
| 실행                          | 이 문서를 쓰면서 그래프를 실행하지 않았다                                                                                          |
| Claude 로 그래프 실행         | DAY3_GUIDE 체크리스트에 "Claude provider 로도 그래프 버전 테스트"는 미완료(비용 문제로 보류)로 적혀 있다                              |
