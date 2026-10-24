# Day 2 코드 가이드 — FastAPI 구조와 수동 루프 에이전트

> 실행하는 방법과 확인 항목은 [DAY2_GUIDE.md](DAY2_GUIDE.md). 이 문서는 Day 2 에 만든 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 코드가 이후 Day 에 계속 바뀌어서 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI**. 이 시점에는 Kafka, Redis, Qdrant, Spring 이 없다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 당시 실행 여부는 DAY2_GUIDE 의 체크리스트를 따른다.

**전체 목적:** 스크립트로 실험하던 에이전트 로직이 FastAPI 프로젝트 구조(`main.py` → 라우터 → 서비스)로 옮겨지면서, `POST /api/v1/agent/run` 요청 한 건이 코드에서 어떤 순서로 처리되는지 따라간다.


## 1. 전체 흐름

```
클라이언트
  │ POST /api/v1/agent/run  {"query": "환불 정책이 어떻게 되나요?", "provider": "gemini"}
  ▼
[FastAPI :8000]  main.py 가 등록한 라우터 중 routers/agent.py 의 run_agent_endpoint
  │ 요청 본문을 AgentRequest 로 검사 (schemas/models.py)
  ▼
services/agent_core.py 의 run_agent(question, provider)
  ① get_model(provider)  → 모델 객체 (gemini 또는 claude)
  ② model.bind_tools(...) → 도구 목록을 모델에 알려 줌
  ③ 1차 호출: 모델이 "도구가 필요한가" 판단
  ④ 도구 호출이 있으면 실행하고 결과를 ToolMessage 로 붙임
  ⑤ 2차 호출: 도구 결과를 보고 최종 답을 씀
  ▼
AgentResponse(answer, provider) 를 JSON 으로 응답
```

### 1-1. 단계별 파일 위치

| 단계 | 하는 일                          | 파일                                          | 함수                                   |
| ---- | -------------------------------- | --------------------------------------------- | -------------------------------------- |
| A    | 앱 만들기, 라우터 등록           | `d2-ai-python/app/main.py`                    | (모듈 전체. `include_router` 두 줄)     |
| B    | 헬스체크                         | `d2-ai-python/app/routers/health.py`          | `health_check`                         |
| C    | 에이전트 호출 엔드포인트         | `d2-ai-python/app/routers/agent.py`           | `run_agent_endpoint`                   |
| D    | 요청과 응답의 모양               | `d2-ai-python/app/schemas/models.py`          | `AgentRequest`, `AgentResponse`        |
| E    | 모델 선택, 도구, 호출 두 번      | `d2-ai-python/app/services/agent_core.py`     | `get_model`, `search_vector_db`, `run_agent` |
| F    | Claude 응답 글자 꺼내기          | `d2-ai-python/app/services/agent_core.py`     | `_extract_text`                        |


## 2. 주요 코드

### 2-1. 앱과 라우터 (`main.py`, `routers/`)

```python
# main.py
from routers import health, agent
app = FastAPI(title="AI Agent Backend", ...)
app.include_router(health.router)
app.include_router(agent.router)

# routers/agent.py
router = APIRouter(prefix="/api/v1", tags=["agent"])

@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    answer = run_agent(req.query, provider=req.provider)
    return AgentResponse(answer=answer, provider=req.provider)
```

| 부분                                    | 설명                                                                                                             |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `from app.routers import health, agent` | 앞에 `app.` 이 붙은 import 다. 그래서 `d2-ai-python` 폴더에서 `uvicorn app.main:app` 을 실행한다(DAY2_GUIDE 5번의 첫 에러가 이것). Day 2 당시에는 `from routers import ...` 여서 `app` 폴더 안에서 실행했다 |
| `prefix="/api/v1"` 와 `/agent/run`      | 합쳐서 `/api/v1/agent/run`                                                                                        |
| `response_model=AgentResponse`          | 응답을 `answer`, `provider` 두 필드로 맞춰 준다                                                                    |
| `async def` 인데 `run_agent` 는 동기 함수 | `run_agent` 가 모델을 부르는 동안 이 워커의 이벤트 루프가 멈춘다. 이때는 요청이 한 건씩만 오니 문제 되지 않았다(이 코드는 지금도 그대로이고, 이 한계는 DAY23_GUIDE 7번에 기록됨) |

### 2-2. 요청과 응답의 모양 (`schemas/models.py`)

```python
class AgentRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"   # "gemini" 또는 "claude"

class AgentResponse(BaseModel):
    answer: str
    provider: str
```

`provider` 를 안 보내면 `gemini` 다. 이 값 하나로 모델이 바뀌는 구조가 이후 모든 Day 에서 유지된다.

### 2-3. 모델과 도구 (`agent_core.py`)

```python
def get_model(provider="gemini"):
    if provider == "gemini": return ChatGoogleGenerativeAI(model="gemini-3.6-flash", ...)
    elif provider == "claude": return ChatAnthropic(model="claude-3-5-haiku-20241022", ...)
    else: raise ValueError(...)

@tool
def search_vector_db(query: str) -> str:
    """사내 문서/상품 리뷰 등에서 질문과 의미적으로 유사한 문서를 검색한다."""
    mock_results = [ ... ]            # Qdrant 가 없던 때라 고정된 가짜 문서 2개
    return "\n".join(mock_results)

AVAILABLE_TOOLS = [search_vector_db]
TOOLS_BY_NAME = {t.name: t for t in AVAILABLE_TOOLS}
```

| 부분                          | 설명                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `@tool`                       | 함수와 독스트링을 모델에게 "쓸 수 있는 도구"로 알려 주는 LangChain 데코레이터. 독스트링이 모델이 도구를 고르는 설명이 된다   |
| mock 문서                     | `"환불은 구매 후 7일 이내 가능합니다."` 같은 고정 문장. Day 10 에서 실제 Qdrant 검색으로 바뀐다                           |
| `TOOLS_BY_NAME`               | 모델이 "이 이름의 도구를 이 인자로 불러 달라"고 하면 이름으로 함수를 찾는 사전                                            |
| 모델 이름                     | 이 파일의 `gemini-3.6-flash`, `claude-3-5-haiku-20241022` 는 Day 2 시점 값이다. 이후 `agent_graph.py` 에서 바뀌었다(Gemini 모델 교체는 DAY6_GUIDE 의 쿼터 이슈 부분 참고) |

### 2-4. 수동 루프 (`run_agent`)

```python
model_with_tools = model.bind_tools(AVAILABLE_TOOLS)
messages = [HumanMessage(content=question)]

ai_response = model_with_tools.invoke(messages)          # 1차 호출
messages.append(ai_response)
if not ai_response.tool_calls:
    return _extract_text(ai_response.content)            # 도구가 필요 없으면 바로 답

for tool_call in ai_response.tool_calls:                 # 도구 실행
    result = TOOLS_BY_NAME[tool_call["name"]].invoke(tool_call["args"])
    messages.append(ToolMessage(content=result, tool_call_id=tool_call["id"]))

final_response = model_with_tools.invoke(messages)       # 2차 호출
return _extract_text(final_response.content)
```

| 부분                         | 설명                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 호출은 최대 두 번            | 1차에서 도구가 필요 없다고 하면 한 번, 도구가 필요하면 두 번이다. 2차 호출 뒤에 모델이 또 도구를 요청해도 실행하지 않는다 |
| `ToolMessage` 의 `tool_call_id` | 어떤 도구 요청에 대한 결과인지 모델이 짝을 맞추는 값                                                                    |
| 알 수 없는 도구 이름         | 코드에는 "알 수 없는 도구: ..." 문장을 결과로 넘기는 분기가 있다                                                         |
| `_extract_text`              | Gemini 는 답이 문자열로 오고, Claude 는 `[{"type": "text", "text": ...}, ...]` 같은 블록 목록으로 올 수 있다. `text` 블록만 모아 문자열로 만든다 |

이 반복 구조가 Day 3 에서 LangGraph 로 바뀐다([DAY3_CODE_GUIDE.md](DAY3_CODE_GUIDE.md)).


## 3. 결과를 읽는 순서

1. `GET /health` 가 `{"status":"ok"}` 인가 (`health_check`).
2. `POST /api/v1/agent/run` 의 응답에 `answer`, `provider` 가 있는가.
3. 답에 mock 문서의 내용(환불은 7일 이내)이 들어 있으면 도구가 불렸다는 뜻이다.


## 4. 확인하지 않은 것

| 항목                                | 내용                                                                                                                                  |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 이 문서 작성 시점의 실행            | 코드를 읽기만 했다. 서버를 띄우거나 호출하지 않았다                                                                                    |
| Claude 로 호출                      | `agent_core.py` 의 `_extract_text` 는 Claude 응답을 위한 것이지만, 이 파일로 Claude 를 호출해 본 기록은 확인하지 않았다                  |
| `agent_core.py` 의 mock 문서 2      | 해당 문자열 안에 `/api/v1/agent/run — Claude도 정상 작동 확인 필요` 같은 메모 문장이 섞여 있다. 의도한 내용인지 확인하지 않았다(이 파일은 현재 라우터가 쓰지 않는다) |
