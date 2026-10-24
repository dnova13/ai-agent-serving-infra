# Day 17 코드 가이드 — 처리 결과를 메모리에 임시 보관하고 requestId 로 조회, E2E 스크립트

> 실행하는 방법과 확인 항목은 [DAY17_GUIDE.md](DAY17_GUIDE.md), 결과는 [DAY17_RESULT.md](DAY17_RESULT.md). 이 문서는 결과가 저장되고 조회되기까지의 코드 순서를 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **9092 = Kafka**.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 17 커밋(`064f5cb`, 스크립트는 `ee6a342`) 시점의 것이다. `result_store.py` 는 Day 23 에서 Redis 로 바뀌었다(DAY23_CODE_GUIDE).

**전체 목적:** Spring 은 202 와 `requestId` 만 돌려주고 답은 워커가 만든다. 그 답을 워커가 어디에 두고 사용자가 어떻게 찾는지(메모리 저장소 + 조회 API)와, 접수부터 조회까지 전 구간을 한 번에 확인하는 스크립트가 무엇을 판정하는지 따라간다.


## 1. 전체 흐름

```
POST 8080 /api/agent/requests → 202 {requestId} → Kafka → 워커 _handle → agent_handler
   ① store.set_processing(requestId)               받자마자 PROCESSING
   ② run_agent(...)                               to_thread + wait_for(120초)
   ③ 성공: store.set_done(requestId, answer, elapsed_ms)
      실패: store.set_failed(requestId, "TypeName: 이유") 후 예외를 다시 던짐 (워커가 failed 로 센다)
GET 8000 /api/v1/agent/results/{requestId} → store.get → 200 {status, answer, ...} 또는 404
```

| 단계 | 하는 일                    | 파일                                              | 함수 / 클래스                              |
| ---- | -------------------------- | ------------------------------------------------- | ------------------------------------------ |
| A    | 결과 저장소(메모리)        | `d2-ai-python/app/services/result_store.py`       | `ResultStore` (`set_processing`, `set_done`, `set_failed`, `get`), 전역 `store` |
| B    | handler 가 상태를 기록     | `d2-ai-python/app/services/agent_handler.py`      | `agent_handler`                            |
| C    | 조회 API                   | `d2-ai-python/app/routers/results.py`             | `get_result`                               |
| D    | 라우터 등록                | `d2-ai-python/app/main.py`                        | `include_router(results.router)`           |
| E    | 전 구간 확인 스크립트      | `scripts/day17_e2e.sh`                            | 단계 1)~5)                                 |


## 2. 주요 코드

### 2-1. 메모리 저장소 (`result_store.py`)

| 부분                              | 설명                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 상태 3가지                        | `PROCESSING`(받아서 에이전트를 부르는 중), `DONE`(답이 나옴), `FAILED`(실패, 타임아웃 포함)                                              |
| `OrderedDict`                     | `_put` 은 항목을 꺼냈다가 다시 넣어 가장 최근 것으로 옮기고, `RESULT_STORE_MAX`(기본 1000)를 넘으면 가장 오래된 것부터 지운다            |
| 저장 필드                         | `requestId`, `status`, `answer`, `error`, `elapsedMs`, `updatedAt`(UTC ISO)                                                            |
| 전역 `store`                      | 앱 전체에서 하나를 같이 쓴다(`agent_handler` 가 쓰고 `results.py` 가 읽는다)                                                            |
| 파일 머리의 한계                  | 서버를 껐다 켜면 사라진다 / 워커가 여러 개이거나 uvicorn 프로세스가 여러 개면 서로의 결과를 못 본다 / Redis 를 붙이는 4주차에 교체한다 |

### 2-2. handler 의 기록 (`agent_handler`)

```python
store.set_processing(message.requestId)           # 받은 즉시. 실험용 대기(AGENT_DEBUG_DELAY_SEC) 중에도 조회된다
try:
    answer = await asyncio.wait_for(asyncio.to_thread(run_agent, ...), timeout=AGENT_TIMEOUT_SEC)
except Exception as e:                             # CancelledError(서버 종료)는 Exception 이 아니라 여기서 안 잡힌다
    reason = str(e) or (f"{AGENT_TIMEOUT_SEC:g}초 안에 끝나지 않음" if isinstance(e, asyncio.TimeoutError) else "원인 메시지 없음")
    store.set_failed(message.requestId, f"{type(e).__name__}: {reason}")
    raise
store.set_done(message.requestId, str(answer), elapsed_ms)
```

| 부분                      | 설명                                                                                                              |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| 실패 후 `raise`           | 상태만 기록하고 예외는 다시 던져서 Day 16 의 동작(워커가 `failed` 로 세고 건너뜀)이 그대로 유지된다                  |
| 에러 문구                 | `TimeoutError` 는 메시지가 비어 있어서 "120초 안에 끝나지 않음" 을 직접 붙인다                                       |

### 2-3. 조회 API (`get_result`)

`GET /api/v1/agent/results/{request_id}` 는 있으면 저장된 dict 를, 없으면 `404` 를 돌려준다. 404 의 이유는 셋 중 하나인데 구분하지 못한다고 응답 문구에 적혀 있다: 없는 `requestId`, 아직 워커가 받지 않음, 서버 재시작으로 지워짐.

### 2-4. E2E 스크립트 (`scripts/day17_e2e.sh`)

```
bash scripts/day17_e2e.sh [요청 건수, 기본 3]      환경변수: SPRING, FASTAPI, E2E_TIMEOUT_SEC(180), PROVIDER(gemini)
```

| 단계                        | 하는 일                                                                                                                                |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| 1) 시작 전 `/health`         | `received`, `failed`, `skipped`, `committed`, `commit_mode` 를 읽어 기준값으로 삼는다. 응답이 없으면 FAIL                                |
| 2) 요청 N건 전송             | 질문 5개를 돌려 가며 8080 에 POST. 응답이 `202` 이고 `requestId` 가 있어야 접수로 센다                                                    |
| 3) 처리 완료 대기            | 1초마다 `/health` 를 읽어 `(received+failed+skipped)` 증가분이 접수 건수에 이르고 `current_request_id` 가 비면 끝. manual 모드면 `committed` 증가분도 접수 건수 이상이어야 한다 |
| 4) 결과 조회                 | `requestId` 마다 조회 API 를 호출해 `DONE` / `FAILED` / 그 외(FAIL)로 센다                                                                |
| 5) 결과                      | 완료 시간, 실패와 건너뜀 건수를 출력하고 `RESULT: PASS` 또는 `FAIL`                                                                      |

JSON 값은 `jq` 없이 `grep -oE` 와 `sed` 로 꺼낸다(`num`, `str` 함수). 스크립트는 답이 "맞는지"는 판정하지 않는다(파일 머리의 한계).


## 3. 결과를 읽는 순서

1. 단계 2)의 응답 코드가 `202` 이고 시간이 수십 ms 인지 본다(LLM 을 기다리지 않는다).
2. 단계 3)의 `processed` 와 `committed` 가 접수 건수까지 올라가는지 본다.
3. 단계 4)의 `DONE` 건수와 `elapsedMs` 를 본다.
4. 마지막 줄이 `RESULT: PASS` 인지 본다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 스크립트나 서버를 실행하지 않았다. 실제 출력은 DAY17_RESULT 를 따른다                                                  |
| 현재 코드와의 차이    | 저장소는 Day 23 에 Redis 로 바뀌었고, 스크립트는 Day 18(여러 워커 `/health`, 동시 처리 배율)과 Day 23 에 바뀌었다. 이 문서는 Day 17 커밋 시점이다 |
| 동시 쓰기             | 이 Day 의 저장소는 워커 하나의 메모리다. 여러 워커 사이의 공유는 Day 23 의 주제다                                                       |
