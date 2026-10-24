# Day 3 실행 가이드 — LangGraph 전환

## 목표
Day2에서 만든 LangChain 기반 수동 루프(`agent_core.py`)를,
**LangGraph의 State / Node / Conditional Edge 구조로 재구성**한다.
(개념 설명은 `CONCEPTS_LANGCHAIN_VS_LANGGRAPH.md` 참고)

---

## 왜 이 작업을 했는가

원래 Day3 계획은 "LangGraph로 State/Node/Edge를 처음부터 만드는 것"이었다.
Day2 산출물이 이미 꽤 완성도 있어서 한때 "Day3는 스킵해도 된다"고 판단했다가,
**멀티스텝 도구 호출 확장성**과 **4~6주 차(Redis 세션 관리, 재시도 로직)와의 연결성**을 고려해
LangGraph로 실제 전환하기로 결정을 뒤집었다.

---

## 무엇을 만들었나

### 1) State 정의
```python
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    provider: str
    retry_count: int
```
- `messages`: 대화 히스토리 (add_messages 리듀서가 자동으로 append)
- `provider`: "gemini" | "claude" — 요청마다 어떤 모델을 쓸지
- `retry_count`: 6주 차 Self-Correction(재시도 로직)에서 사용 예정 (지금은 선언만 해둠, 미사용)

### 2) Node 2개
- `agent_node`: 모델을 호출해서 "도구가 필요한지" 판단하는 두뇌 역할
- `tool_node`: 모델이 요청한 도구(`search_vector_db`)를 실제로 실행

### 3) Conditional Edge
```python
def should_continue(state: AgentState) -> str:
    last_message = state["messages"][-1]
    if getattr(last_message, "tool_calls", None):
        return "tools"
    return END
```
모델 응답에 `tool_calls`가 있으면 `tools` 노드로, 없으면 그래프 종료(`END`).

### 4) 그래프 구성
```
START → agent → (조건부 분기)
                   ├─ 도구 필요 → tools → agent (다시 순환)
                   └─ 도구 불필요 → END
```

---

## 실행 및 검증 과정

### 1) 단독 스크립트 실행
```bash
cd app
python services/agent_graph.py
```
Gemini/Claude 각각 질문을 던져서 콘솔에 답변이 정상 출력되는지 확인.

### 2) FastAPI 라우터 연결
`app/routers/agent.py`에서 import 대상을 교체:
```python
# 변경 전
from services.agent_core import run_agent

# 변경 후
from services.agent_graph import run_agent
```

### 3) 겪은 에러와 해결
| 에러 | 원인 | 해결 |
|---|---|---|
| `ImportError: cannot import name 'run_agent_core'` | 라우터가 실제로 존재하지 않는 함수명(`run_agent_core`)을 import 하려 함 | `agent_graph.py`에 정의된 실제 함수명 `run_agent`로 라우터의 import문·호출부 수정 |

### 4) 최종 API 테스트 (Postman)
```
POST http://localhost:8000/api/v1/agent/run
Body: {"query": "환불 정책이 어떻게 되나요?", "provider": "gemini"}
```
→ `200 OK`, mock RAG 데이터(7일 이내 환불)가 반영된 답변 정상 수신 확인.

---

## Day2 결과물과의 비교 (실제 테스트 기준)

지금까지 테스트한 질문은 **도구를 1번만 호출하면 끝나는 단순 케이스**라서,
Day2(수동 루프) 방식과 Day3(LangGraph) 방식의 **최종 답변 텍스트 자체는 동일**하게 나온다.

차이는 결과값이 아니라 **구조적 확장성**에 있다 — 자세한 내용은
`CONCEPTS_LANGCHAIN_VS_LANGGRAPH.md` 참고.

---

## Day 3 완료 체크리스트

- [x] State 정의 (`AgentState`)
- [x] Agent Node + Tool Node 작성
- [x] Conditional Edge로 도구 필요 여부 분기
- [x] 그래프 컴파일 및 단독 실행 확인
- [x] FastAPI 엔드포인트에 연결, 실제 API 호출 성공 (Postman 200 OK)
- [ ] Claude provider로도 그래프 버전 테스트 (선택 — 비용 문제로 보류 중, 코드 구조는 유지)
- [ ] `requirements.txt` 갱신 (`pip freeze > requirements.txt`)
- [ ] Git 커밋

---

## 다음 (2주 차 예고)
지금 mock 데이터로 응답하고 있는 `search_vector_db` 도구를,
Qdrant에 실제로 문서를 임베딩해서 넣고 진짜 유사도 검색을 하도록 교체한다.
`agent_graph.py`의 `search_vector_db` 함수 내부만 교체하면 되고,
State/Node/Edge 구조는 그대로 재사용된다.
