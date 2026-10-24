# Day 10 코드 가이드 — 에이전트 도구를 Qdrant 로 교체하고, RAG 있음/없음 비교하기

> 실행하는 방법과 확인 항목은 [DAY10_GUIDE.md](DAY10_GUIDE.md), 검색 개념은 [CONCEPTS_RAG.md](CONCEPTS_RAG.md). 이 문서는 바뀐 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 코드가 이후 Day 에 바뀌어서 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI**, **6333 = Qdrant**. 검색마다 Gemini 임베딩 API 를 호출하고, 답은 `provider` 가 정한 채팅 모델이 쓴다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 10 커밋(`fa9f387`) 시점의 것이다.

**전체 목적:** (1) 에이전트의 `search_vector_db` 도구가 mock 대신 실제 Qdrant 검색을 하게 된 변화와, (2) 같은 질문을 "검색 없이 / 검색 후" 두 방식으로 답하게 해 차이를 보는 새 API(`/api/v1/rag/*`)가 어떻게 만들어졌는지 따라간다.


## 1. 전체 흐름

### 1-1. 에이전트 경로 (바뀐 곳: 도구 한 개)

```
POST /api/v1/agent/run → agent_graph (Day 3~6 그대로)
   agent 노드가 search_vector_db 도구를 요청
   ▼
tool_node → search_vector_db(query)
   ① (테스트용) query 에 "강제실패" 가 있으면 예외  ← Day 6 의 재시도 시험 분기는 유지
   ② qdrant_search(query, limit=3)    Day 9 의 검색 함수
   ③ format_for_llm(hits)             "[문서 1] ...\n[문서 2] ..." 문자열
   ▼
agent 노드가 그 문자열을 보고 최종 답
```

### 1-2. 비교 경로 (새로 생김: 에이전트를 거치지 않는다)

```
POST /api/v1/rag/compare {"query","provider","limit"}      routers/compare.py  compare_endpoint
   ▼ services/rag_compare.py  compare()
   ├─ without_rag: 채팅 모델 1번 호출 (검색 없음)
   └─ with_rag   : 검색(임베딩 + Qdrant) → 문서를 프롬프트에 붙여 채팅 모델 1번 호출
POST /api/v1/rag/ask        with_rag 만         ┐ 실패하면 결과 안의 error 대신 502 로 응답
POST /api/v1/rag/ask-plain  without_rag 만      ┘
```

| 단계 | 하는 일                          | 파일                                              | 함수                                           |
| ---- | -------------------------------- | ------------------------------------------------- | ---------------------------------------------- |
| A    | 도구 교체                        | `d2-ai-python/app/services/agent_graph.py`        | `search_vector_db`                             |
| B    | RAG 있음/없음 답변               | `d2-ai-python/app/services/rag_compare.py`        | `answer_without_rag`, `answer_with_rag`, `compare` |
| C    | HTTP 엔드포인트                  | `d2-ai-python/app/routers/compare.py`             | `compare_endpoint`, `ask_with_rag`, `ask_without_rag` |
| D    | 요청과 응답의 모양               | `d2-ai-python/app/schemas/models.py`              | `CompareRequest`, `CompareSide`, `CompareResponse`, `AskRequest`, `AskResponse` |
| E    | 라우터 등록                      | `d2-ai-python/app/main.py`                        | `include_router(compare.router)`               |


## 2. 주요 코드

### 2-1. 도구 교체 (`search_vector_db`)

```python
@tool
def search_vector_db(query: str) -> str:
    """쇼핑몰 고객센터 문서(환불, 배송, 교환, 결제, 회원, 상품 후기)에서 질문과 의미가 비슷한 문서를 검색한다. ..."""
    if "강제실패" in query:
        raise RuntimeError("의도적으로 발생시킨 테스트용 실패 (retry_count 검증용)")
    hits = qdrant_search(query, limit=3)
    return format_for_llm(hits)
```

| 부분                         | 설명                                                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 독스트링이 구체적으로 바뀜    | 독스트링은 모델이 "이 도구를 언제 쓸지" 판단하는 설명서다. 어떤 문서가 들어 있는지 적어 줘야 관련 질문에서만 호출한다(코드 주석의 설명)          |
| `search as qdrant_search`    | `search` 라는 이름이 너무 일반적이라 바꿔서 가져온다                                                                                          |
| 예외를 잡지 않음             | Qdrant 가 꺼져 있거나 임베딩 API 가 429 를 내면 예외가 `tool_node` 로 올라가 Day 6 의 재시도가 동작한다. 재시도가 가짜 실패가 아니라 실제 일시 장애에 대한 것이 된다는 것이 코드 주석의 설명이다 |
| `try/except ModuleNotFoundError` import | 서버(`app` 폴더에서 실행)는 `services.` 로, `-m` 실행(프로젝트 루트)은 `app.services.` 로 import 되므로 둘 다 지원한다. 이 프로젝트의 서비스 파일이 공통으로 쓰는 패턴이다 |

기존 mock 도구는 `[OLD Day 6 까지의 mock 버전]` 주석으로 남아 있다.

### 2-2. 왜 비교용 코드를 에이전트와 따로 만들었나 (`rag_compare.py` 머리말)

| 이유                                    | 설명                                                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| 경로를 고정하기 위해                    | 에이전트는 모델이 도구를 쓸지 스스로 판단해서, 같은 질문에도 검색을 할 때도 안 할 때도 있다. "있음/없음" 비교는 두 경로를 코드로 고정해야 한다 |
| 호출 수를 같게 하기 위해                | 각 방식이 채팅 모델을 딱 1번만 호출한다. 에이전트는 도구 판단과 최종 답으로 2번이다                                         |
| 공정한 비교                             | 두 방식 모두 같은 모델, 같은 역할 설정(`SYSTEM_BASE`)을 쓰고, RAG 쪽에만 참고 문서와 `RAG_RULE`(문서에 있는 내용만 근거로, 없으면 "문서에 해당 내용이 없습니다")을 더한다 |

### 2-3. 두 방식의 답변 함수

```python
def answer_without_rag(question, provider):
    resp = get_model(provider).invoke([SystemMessage(SYSTEM_BASE), HumanMessage(question)])
    return {"answer": ..., "elapsed_ms": ..., "llm_calls": 1}      # 실패하면 {"error": ..., ...}

def answer_with_rag(question, provider, limit=3):
    hits = search(question, limit=limit)                              # 임베딩 + Qdrant. retrieval_ms 로 따로 잼
    prompt = f"[참고 문서]\n{format_for_llm(hits)}\n\n[질문]\n{question}"
    resp = get_model(provider).invoke([SystemMessage(SYSTEM_BASE + "\n" + RAG_RULE), HumanMessage(prompt)])
    return {"answer": ..., "elapsed_ms": ..., "llm_calls": 1, "retrieval_ms": ..., "sources": hits}
```

| 부분                     | 설명                                                                                                                              |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| 예외를 값으로 돌려줌     | `compare` 가 두 방식을 차례로 실행하므로, 한쪽이 실패해도 다른 쪽 결과를 보여 주려고 `{"error": ...}` 로 돌려준다                   |
| `_safe_error`            | 에러 문자열에 섞인 API 키 값을 `***` 로 가리고 300자로 줄인다                                                                      |
| `sources` 와 `retrieval_ms` | RAG 가 추가로 드는 시간(임베딩 + 검색)과 근거로 쓴 문서와 점수를 응답에 담는다. 검색까지 성공하고 모델에서 실패하면 `sources` 는 남긴다 |

### 2-4. 라우터 (`compare.py`)

```python
@router.post("/compare", response_model=CompareResponse)
def compare_endpoint(req: CompareRequest): ...      # provider, limit 검사 후 compare()

@router.post("/ask", response_model=AskResponse)
def ask_with_rag(req: AskRequest): ...              # _to_response 가 error 가 있으면 HTTPException(502)
```

| 부분                         | 설명                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `async def` 가 아니라 `def`  | 안에서 모델과 Qdrant 호출이 끝날 때까지 기다린다. `async def` 안에서 이런 호출을 하면 서버 전체가 멈추지만 `def` 는 FastAPI 가 별도 스레드에서 실행한다는 것이 코드 주석의 설명이다. (`/agent/run` 은 이 시점에 `async def` 로 남아 있었다) |
| 검증                         | `provider` 가 `gemini`, `claude` 가 아니면 400, `limit` 이 1~10 이 아니면 400                                                                      |
| `/compare` 와 `/ask` 의 실패 | `/compare` 는 결과 안의 `error` 로 보여 주고, 단독 API 는 502 로 응답한다. 단독 API 는 호출한 쪽이 상태 코드로 성공과 실패를 가를 수 있어야 해서라고 코드에 적혀 있다 |
| 파일 이름                    | 처음 `compare` 로 만들고 `ask` 계열도 같은 `/rag` 라우터에 둬서 이름이 그대로다                                                                    |

### 2-5. 응답의 모양 (`schemas/models.py`)

`CompareSide` 가 한쪽 결과(`answer`, `error`, `elapsed_ms`, `llm_calls`, `retrieval_ms`, `sources`)이고, `CompareResponse` 는 `without_rag` 와 `with_rag` 두 개를 담는다. `AskResponse` 는 `CompareSide` 를 상속해 `query`, `provider`, `mode`(`rag` 또는 `plain`)를 더한다.


## 3. 결과를 읽는 순서

1. `/rag/compare` 응답에서 `without_rag.answer` 와 `with_rag.answer` 를 나란히 본다. 문서에 있는 값(예: 7일, 왕복 배송비)이 어느 쪽에 나오는지 본다.
2. `with_rag.sources` 의 문서와 `score` 가 질문과 맞는지 본다.
3. `elapsed_ms`, `retrieval_ms`, `llm_calls` 로 RAG 가 더한 시간을 본다.
4. 에이전트 경로는 `/api/v1/agent/run` 의 답에 문서의 값이 들어 있는지 본다.


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                          | 이 문서를 쓰면서 서버를 띄우거나 API 를 호출하지 않았다. 실제 비교 결과는 DAY10_GUIDE 의 기록을 따른다                                  |
| 현재 코드와의 차이            | `agent_graph.py` 는 이후 Day 에 시스템 프롬프트(Day 11), 대화 저장과 락(Day 20, 21) 등이 더해졌다. 이 문서는 Day 10 커밋 시점의 도구만 다룬다 |
| `/rag/*` 의 동시 요청         | 라우터가 `def` 라서 스레드풀에서 돈다는 것은 코드 주석을 옮긴 것이고, 동시에 여러 건을 보내 확인하지 않았다                              |
