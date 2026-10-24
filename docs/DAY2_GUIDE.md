# Day 2 실행 가이드

## 목표
지금까지 스크립트로 실험한 에이전트 로직을 **FastAPI 프로젝트 구조**로 정리하고,
`/health`, `/api/v1/agent/run` 두 엔드포인트가 정상 동작하는 것 확인.

---

## 1. 폴더 구조

받으신 파일들을 아래처럼 배치하세요 (기존 `app.py`, `model_provider.py` 등이 있던
프로젝트 루트 기준):

```
ai-python/
├── venv/                      # 이미 있음
├── .env                       # 이미 있음
├── requirements.txt           # 이미 있음
└── app/
    ├── main.py                 # ← 새로 받음
    ├── routers/
    │   ├── __init__.py
    │   ├── health.py            # ← 새로 받음
    │   └── agent.py              # ← 새로 받음
    ├── schemas/
    │   ├── __init__.py
    │   └── models.py             # ← 새로 받음
    └── services/
        ├── __init__.py
        └── agent_core.py          # ← 새로 받음 (기존 agent_interactions_api.py 로직 이식)
```

기존에 만드셨던 `agent_interactions_api.py`, `app.py`(단독 테스트용)는
루트에 그대로 둬도 되고, 지워도 됩니다 — 로직은 `app/services/agent_core.py`로
옮겨졌습니다.

---

## 2. 실행

```bash
cd d2-ai-python
uvicorn app.main:app --reload --port 8000
```

정상이면 터미널에 아래처럼 뜹니다:
```
INFO:     Uvicorn running on http://127.0.0.1:8000
INFO:     Application startup complete.
```

---

## 3. 테스트

### 3-1) 헬스체크
```bash
curl http://localhost:8000/health
```
기대 응답: `{"status":"ok"}`

### 3-2) 에이전트 실행 (Qdrant는 아직 mock)
```bash
curl -X POST http://localhost:8000/api/v1/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "환불 정책이 어떻게 되나요?"}'
```
기대 응답: `{"answer": "..."}` 형태로 환불 관련 답변

### 3-3) Swagger UI로 확인 (추천)
브라우저에서 http://localhost:8000/docs 접속 →
자동 생성된 API 문서에서 직접 요청 날려볼 수 있음. Postman 대신 이거 써도 됩니다.

---

## 4. 오늘의 완료 기준

- [ ] `uvicorn app.main:app --reload` 정상 기동
- [ ] `/health` 200 응답
- [ ] `/docs` (Swagger UI) 정상 로딩
- [ ] `/api/v1/agent/run`에 질문 보내면 답변 정상 반환
- [ ] Git 커밋: "Day2: FastAPI 구조화 + agent endpoint"

---

## 5. 흔한 에러

| 증상 | 원인 | 해결 |
|---|---|---|
| `ModuleNotFoundError: No module named 'app'` | `d2-ai-python` 폴더가 아닌 곳(예: `app` 폴더 안)에서 uvicorn 실행함 | 반드시 `d2-ai-python` 폴더에서 `uvicorn app.main:app` 실행 |
| `.env` 못 찾음 (GOOGLE_API_KEY 인식 안됨) | `.env`가 `d2-ai-python` 폴더가 아닌 다른 곳에 있음 | `.env`를 `d2-ai-python` 폴더에 둔다(`load_dotenv()` 가 코드 파일 위치에서 위로 올라가며 찾는다) |
| `/docs` 접속 안됨 | 서버 자체가 안 뜬 상태 | 터미널에 에러 로그 있는지 먼저 확인 |

---

## 다음 (Day 3 예고)
LangGraph State/Node/Edge로 지금의 단순 흐름을 "조건부 분기가 있는 그래프"로 전환
(지금은 도구가 1개뿐이라 분기가 단순한데, 여기에 실제 조건 분기 로직 추가)
