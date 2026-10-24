# Day 5 실행 가이드 — 에이전트 API화

## 원래 목표
Day3~4에서 만든 LangGraph 에이전트 로직을 `/api/v1/agent/run` 엔드포인트로
감싸서, HTTP 요청으로 외부에서 호출 가능하게 만드는 것.

---

## 실제로는 언제 끝났는가

이 작업은 **Day2~3 진행 과정에서 이미 완료**됐다. 별도로 "Day5"라는 이름의
작업 시간을 따로 갖지 않고, 아래처럼 자연스럽게 진행 중에 끝났다.

| 원래 계획 | 실제 진행 |
|---|---|
| Day2: `/health` 엔드포인트만 | `/health` + `/api/v1/agent/run` 둘 다 Day2에 완성 (agent_core.py 연결) |
| Day3: LangGraph로 State/Node/Edge 구현 | 완료 후, 기존 라우터의 import 대상만 `agent_core` → `agent_graph`로 교체 |
| Day5: API화 | → 위 과정에서 자연히 완료됨 |

즉 "에이전트 로직을 언제 API로 감쌌는가"가 아니라, **"에이전트 로직의
내부 구현(수동 루프 → LangGraph)이 바뀌어도 API 계층은 그대로 유지된다"**
는 게 이번에 실제로 확인된 부분이다. 이건 우연이 아니라, 애초에
`app/routers/agent.py`(API 계층)와 `app/services/agent_*.py`(로직 계층)를
분리해서 설계했기 때문에 가능했다.

---

## 지금 구조 (계층 분리 확인)

```
app/
├── routers/
│   └── agent.py         ← API 계층. HTTP 요청/응답만 담당
│       "POST /api/v1/agent/run 받으면 services의 run_agent()를 호출한다"
│
└── services/
    ├── agent_core.py     ← 로직 계층 (Day2, LangChain 수동 루프 버전)
    └── agent_graph.py    ← 로직 계층 (Day3, LangGraph 버전) — 현재 사용 중
```

`agent.py` 라우터는 아래처럼 **어떤 로직 구현체를 쓰는지만 한 줄로 결정**한다:

```python
# app/routers/agent.py
from services.agent_graph import run_agent   # ← 이 한 줄만 바꾸면
                                               #    구현체가 통째로 교체됨

@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    answer = run_agent(req.query, provider=req.provider)
    return AgentResponse(answer=answer, provider=req.provider)
```

이 구조 덕분에 로직을 `agent_core` → `agent_graph`로 바꾸는 작업이
**라우터 코드는 한 줄도 안 건드리고 import문 하나만 바꾸는 것**으로 끝났다.
(실제로 이 전환 과정에서 겪은 에러는 `run_agent_core`라는 존재하지 않는
함수명을 잘못 import했던 것 — `DAY3_GUIDE.md` 참고)

---

## 검증 방법 (Postman / curl)

```bash
curl -X POST http://localhost:8000/api/v1/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "환불 정책이 어떻게 되나요?", "provider": "gemini"}'
```

기대 응답:
```json
{
  "answer": "환불 정책은 다음과 같습니다...",
  "provider": "gemini"
}
```

`200 OK`와 함께 mock RAG 데이터가 반영된 자연스러운 답변이 오면 정상.

---

## Day 5 완료 체크리스트

- [x] `/api/v1/agent/run` 엔드포인트 존재
- [x] Pydantic으로 요청(`AgentRequest`)/응답(`AgentResponse`) 스키마 검증
- [x] 로직 계층(services)과 API 계층(routers) 분리 — 로직 교체가 API에 영향 안 줌
- [x] Postman으로 실제 호출 성공 확인 (200 OK)

---

## 이번 작업에서 얻은 설계 교훈

라우터와 서비스 로직을 처음부터 분리해둔 게 결과적으로 옳은 선택이었다.
만약 `agent.py` 라우터 안에 LangChain 호출 코드를 그대로 박아넣었다면,
Day3에서 LangGraph로 전환할 때 라우터 코드까지 다 뜯어고쳐야 했을 것이다.

이 패턴은 앞으로도 계속 도움이 된다 — 예를 들어 4주 차에 Redis로 세션
상태를 관리하게 되어도, `services/agent_graph.py`의 `run_agent()` 내부
구현만 바뀌고 `routers/agent.py`는 또 그대로 유지될 가능성이 높다.

---

## 다음 (2주 차)
`services/agent_graph.py`의 `search_vector_db` 도구 내부를 Qdrant 실제
호출로 교체한다. 이번에도 API 계층(`routers/agent.py`)은 손댈 필요가 없을
것으로 예상된다.
