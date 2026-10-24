# Day 5 코드 가이드 — API 계층과 로직 계층의 분리

> 실행하는 방법과 확인 항목은 [DAY5_GUIDE.md](DAY5_GUIDE.md). 이 문서는 "에이전트 로직이 바뀌어도 API 코드는 그대로"가 코드에서 어떻게 가능했는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 코드가 이후 Day 에 계속 바뀌어서 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI**.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. Day 5 는 새 코드를 만든 날이 아니라(DAY5_GUIDE 의 "실제로는 언제 끝났는가"), Day 2~3 에 만든 코드의 구조를 확인한 날이다. 그래서 이 문서는 이미 있던 코드를 계층 관점에서 다시 읽는다.

**전체 목적:** `routers/agent.py`(API 계층)와 `services/agent_*.py`(로직 계층)가 어디서 나뉘고, Day 3 에서 로직을 `agent_core` 에서 `agent_graph` 로 바꿀 때 어느 줄만 바뀌었는지 코드로 확인한다.


## 1. 전체 흐름

```
HTTP 요청                       API 계층 (routers/, schemas/)              로직 계층 (services/)
─────────────────────────────  ────────────────────────────────────────  ─────────────────────────────
POST /api/v1/agent/run  ─────▶  run_agent_endpoint(req: AgentRequest)  ──▶  run_agent(question, provider)
{"query", "provider"}           · 본문을 AgentRequest 로 검사               · agent_core 또는 agent_graph
                                · run_agent 호출                           · 모델, 도구, 그래프는 여기서만
        ◀───────────────────────  · AgentResponse(answer, provider)  ◀──  문자열 답
200 {"answer", "provider"}
```

| 계층   | 파일                                                                   | 맡은 일                                                   | 모르는 것                         |
| ------ | ---------------------------------------------------------------------- | --------------------------------------------------------- | --------------------------------- |
| API    | `d2-ai-python/app/routers/agent.py`, `d2-ai-python/app/schemas/models.py` | HTTP 경로, 요청 검사, 응답 모양                           | 모델이 무엇인지, 그래프인지        |
| 로직   | `d2-ai-python/app/services/agent_graph.py` (Day 2 때는 `agent_core.py`)   | `run_agent(question, provider)` 를 문자열로 돌려주는 일   | HTTP, FastAPI                     |


## 2. 주요 코드

### 2-1. 구현체를 고르는 한 줄 (`routers/agent.py`)

```python
# from services.agent_core import run_agent      # Day 2: LangChain 수동 루프
from services.agent_graph import run_agent       # Day 3 부터: LangGraph

@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    answer = run_agent(req.query, provider=req.provider)
    return AgentResponse(answer=answer, provider=req.provider)
```

두 구현체의 `run_agent` 가 같은 모양(질문 문자열과 `provider` 를 받아 답 문자열을 돌려줌)이라 import 한 줄을 바꾸는 것으로 로직이 통째로 교체됐다. 기존 줄을 지우지 않고 주석으로 남겨 둔 것도 이 파일에서 볼 수 있다.

### 2-2. 요청과 응답의 검사 (`schemas/models.py`)

```python
class AgentRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"

class AgentResponse(BaseModel):
    answer: str
    provider: str
```

FastAPI 는 요청 본문을 `AgentRequest` 에 맞춰 읽는다. `query` 가 빠지는 등 모양이 맞지 않으면 일반적으로 FastAPI 가 오류 응답을 만들고 `run_agent_endpoint` 까지 들어오지 않는다(이 동작을 이번에 실행해 확인하지는 않았다). `response_model` 은 반환값을 `AgentResponse` 모양으로 맞춘다.

### 2-3. 이 구조가 이후에도 유지되는지

| Day      | 로직 계층에서 바뀐 것                                  | API 계층(`run_agent_endpoint`)                                     |
| -------- | ------------------------------------------------------ | ------------------------------------------------------------------ |
| Day 10   | `search_vector_db` 가 Qdrant 검색으로 바뀜             | 그대로                                                             |
| Day 20   | `run_agent` 가 `session_id` 를 받아 대화를 Redis 에 저장 | `session_id` 를 넘기는 인자가 추가됨(DAY20_CODE_GUIDE 의 `routers/agent.py` 설명) |

DAY5_GUIDE 는 "4주 차에 Redis 로 세션을 관리해도 `routers/agent.py` 는 그대로 유지될 가능성이 높다"고 예상했다. 코드를 보면 인자가 하나 늘어난 변경이 있었다는 점이 예상과 조금 다르다(Day 20 의 변경 내용은 DAY20_CODE_GUIDE 에 있다).


## 3. 결과를 읽는 순서

1. `routers/agent.py` 에서 `from services...import run_agent` 가 어느 파일을 가리키는지 본다.
2. `run_agent_endpoint` 안에 모델이나 LangChain 코드가 없는지 본다(HTTP 처리만 있어야 한다).
3. 로직 계층의 `run_agent` 가 FastAPI 를 import 하지 않는지 본다.


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                         |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 이 문서 작성 시점의 실행      | 코드를 읽기만 했다. 서버를 띄우거나 호출하지 않았다                                                           |
| 잘못된 요청의 응답            | `query` 누락 시 응답 코드와 본문은 확인하지 않았다(2-2 의 설명은 일반적인 FastAPI 동작이다)                    |
| 2-3 의 Day 20 항목            | `session_id` 를 넘기는 변경은 현재 코드를 근거로 한 설명이다. 다른 Day 의 `routers/agent.py` 전체는 이번에 다시 읽지 않았다 |
