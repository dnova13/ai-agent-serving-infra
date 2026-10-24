# API 문서

AI Agent Backend (FastAPI) — 현재 버전 0.1.0

- Base URL: `http://localhost:8000`
- 자동 문서(Swagger UI): `http://localhost:8000/docs`
- 실행: `d2-ai-python` 폴더에서 `uvicorn app.main:app --reload --port 8000`

> 2주 차 이후(Kafka 연동) 에이전트 실행은 비동기(202 Accepted + 결과 조회) 방식으로 바뀔 예정입니다.
> 지금의 `POST /api/v1/agent/run`은 요청 스레드에서 모델을 호출하고 끝날 때까지 기다리는 **동기 방식**이며, k6 부하 테스트의 비교 기준(sync)이 됩니다.

---

## 1. 헬스체크

`GET /health`

서버가 떠 있는지 확인합니다. (3~4주 차에 Kafka/Redis/Qdrant 연결 상태 확인으로 확장 예정)

**응답 200**

```json
{ "status": "ok" }
```

```bash
curl http://localhost:8000/health
```

---

## 2. 에이전트 실행

`POST /api/v1/agent/run`

질문을 LangGraph 에이전트에 전달합니다. 모델이 필요하다고 판단하면 도구를 호출하고, 그 결과를 반영해 최종 답변을 반환합니다.

### 요청 Body

| 필드 | 타입 | 필수 | 기본값 | 설명 |
|---|---|---|---|---|
| `query` | string | O | - | 사용자 질문 |
| `provider` | string | X | `"gemini"` | `"gemini"` 또는 `"claude"` |

```json
{
  "query": "환불 정책이 어떻게 되나요?",
  "provider": "gemini"
}
```

### 응답 200

| 필드 | 타입 | 설명 |
|---|---|---|
| `answer` | string | 에이전트의 최종 답변 |
| `provider` | string | 실제 사용한 provider |

```json
{
  "answer": "환불은 구매 후 7일 이내에 가능합니다.",
  "provider": "gemini"
}
```

### 에러

| 상태 | 원인 |
|---|---|
| 422 | body 형식 오류 (`query` 누락 등) |
| 500 | 모델 호출 실패. 예: `provider: "claude"`인데 `ANTHROPIC_API_KEY`가 비어 있음, Gemini 429(무료 한도 초과) |

> `claude`는 API 비용 때문에 키를 비워 둔 상태입니다. 코드 구조만 유지하므로 `gemini`를 사용하세요.

### 에이전트 동작

| 질문 유형 | 도구 호출 | 예시 |
|---|---|---|
| 문서/규정/리뷰 검색 | `search_vector_db` | "환불 정책이 어떻게 되나요?" |
| 날짜/시간 | `get_current_time` | "지금 몇 시야?" |
| 일반 대화 | 없음 | "안녕!" |

**도구 실패 시**

실패 종류에 따라 두 경로로 갈립니다.

- 일시 장애(연결 실패 등): 같은 도구 호출을 다시 실행합니다. 모델은 재시도를 보지 못합니다. (`MAX_RETRIES = 2`)
- 입력 오류(없는 도구, 잘못된 인자): 실패 원인과 "[정정 안내]"를 모델에게 돌려주고, 모델이 입력을 고쳐 다시 부릅니다. (`MAX_CORRECTIONS = 2`, 도구 호출은 처음 1번 + 정정 2번까지)
- 두 가지가 섞여 실패하면 일시 장애로 셉니다.
- 어느 한도든 넘으면 `give_up_node`가 포기 문구로 답하고 종료합니다. 이때도 HTTP 상태는 200이고, 워커 실패(DLQ 대상)로 보지 않습니다.
- 시험용 검색어: `강제실패`(일시 장애 경로), `교정테스트` / `교정불가`(입력 오류 경로, `provider: "fake-agent"`로 가짜 모델을 써서 확인)

```json
{ "query": "환불 정책이 어떻게 되나요? (검색 키워드: 강제실패)" }
```

```bash
curl -X POST http://localhost:8000/api/v1/agent/run \
  -H "Content-Type: application/json" \
  -d '{"query": "환불 정책이 어떻게 되나요?", "provider": "gemini"}'
```

---

## 현재 한계 / 예정

- 도구 `search_vector_db`는 mock입니다. (10/8~10/10에 실제 Qdrant 호출로 교체)
- 요청마다 새 대화로 시작합니다. 세션 이어가기는 Redis 도입(4주 차) 후 추가합니다.
- 실패한 요청에 대한 400 응답, 인증, rate limit은 아직 없습니다.
