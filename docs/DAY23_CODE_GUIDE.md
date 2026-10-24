# Day 23 코드 가이드 — 결과 저장소(Redis) 코드 흐름

> 사용법과 확인 방법은 [DAY23_GUIDE.md](DAY23_GUIDE.md). 이 문서는 바뀐 코드가 어떤 순서로 도는지 설명한다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, **6379 = Redis**.

**전체 목적:** `result_store` 를 메모리에서 Redis 로 옮기면서 호출하는 쪽(핸들러, 조회 라우터)이 어떻게 바뀌었는지, 그리고 Redis 가 안 될 때 어떻게 버티는지를 코드 순서대로 이해한다.


## 1. 전체 흐름

```
Spring(8080) ── Kafka ──▶ 워커(8000 또는 8001)
                              │  agent_handler
                              │   ① set_processing  ─▶ Redis  agent:result:<requestId>  (PROCESSING, 워커/파티션/offset)
                              │   ② run_agent(...)   (에이전트 실행, Day 20~21 의 대화 저장과 락은 그대로)
                              │   ③ set_done 또는 set_failed ─▶ 같은 키를 갱신 (DONE + 답 / FAILED + 에러)
                              ▼
어느 워커의 GET /api/v1/agent/results/{requestId} ─▶ Redis 를 읽는다 (같은 키)
```

| 구분             | Day 17~22                              | Day 23                                           |
| ---------------- | -------------------------------------- | ------------------------------------------------ |
| 저장 위치        | 워커 프로세스의 `OrderedDict`          | Redis 키 (메모리 `OrderedDict` 는 대체용으로 남김) |
| 호출하는 쪽      | `store.set_*` 를 이벤트 루프에서 직접   | `asyncio.to_thread` 로 스레드에서 호출            |
| 조회 라우터      | `async def get_result`                 | `def get_result` (스레드풀에서 실행)              |


## 2. 주요 코드

### 2-1. 키와 TTL (`result_store.py`)

```python
TTL_SEC = int(os.getenv("RESULT_TTL_SEC", "86400"))   # 임시값. Day 24 에서 정식 정책으로

def result_key(request_id):
    return rc.make_key("result", request_id)           # "agent:result:<requestId>"
```

`make_key` 는 Day 19 의 `redis_client` 가 `agent:` 접두어를 붙여 준다. 대화(`agent:conversation:...`), 락(`agent:lock:...`)과 키 이름이 섞이지 않는다.

### 2-2. 쓰기 (`_put`)

```python
def _put(self, request_id, **fields):
    item = self.get(request_id) or {}        # (1) 지금 값을 읽는다 (Redis, 안 되면 메모리)
    item.update(fields)                      # (2) 새 필드를 합친다 (status, answer 등)
    ...                                      # (3) 메모리 복사본 갱신, 1000건 넘으면 오래된 것부터 지움
    try:
        rc.set_json(result_key(request_id), item, ttl_sec=TTL_SEC)   # (4) Redis 에 쓴다. 쓸 때마다 TTL 새로 시작
    except Exception as e:
        _log.warning(...)                    # Redis 장애: 경고만 남기고 계속한다
```

| 부분 | 설명 |
| ---- | ---- |
| (1)  | `set_done` 은 `answer` 와 `elapsedMs` 만 넘긴다. 앞에서 저장한 `worker`, `partition`, `offset` 이 남으려면 현재 값을 읽어서 합쳐야 한다 |
| (4)  | 예외를 올리지 않는다. 결과를 못 저장해도 답을 만드는 일(에이전트)은 실패하면 안 된다. 대화 저장(Day 20)과 같은 방침이다 |

### 2-3. 읽기 (`get`)

```python
def get(self, request_id):
    try:
        return rc.get_json(result_key(request_id))      # Redis 가 되면 Redis 가 정답. 키가 없으면 None
    except Exception as e:
        item = self._items.get(request_id)              # Redis 장애일 때만 이 프로세스 메모리를 본다
        return dict(item) if item is not None else None
```

Redis 가 되는데 키가 없으면 메모리를 보지 않는다. TTL 이 지난 결과를 메모리 복사본이 되살리지 않게 하려는 것이다(`result_check.py` 7번 항목이 이것을 본다).

### 2-4. 호출하는 쪽

```python
# agent_handler.py (async 함수)
await asyncio.to_thread(store.set_processing, message.requestId, worker=WORKER_ID, ...)
...
await asyncio.to_thread(store.set_done, message.requestId, str(answer), elapsed_ms)
```

`store` 는 동기 Redis 클라이언트를 쓰므로 `async` 함수 안에서 직접 부르면 그동안 이벤트 루프가 멈춘다(`redis_client.py` 머리말의 주의). `routers/results.py` 는 `def` 로 바꿔 FastAPI 가 스레드풀에서 실행하게 했다.

### 2-5. 확인 스크립트 (`result_check.py`)

| 항목 | 하는 일                                                                                                       |
| ---- | ------------------------------------------------------------------------------------------------------------- |
| 2~3  | `set_processing` → `set_done` 후 `rc.get_json` 으로 Redis 에 든 값과 TTL 을 직접 읽는다                       |
| 4    | 메모리가 빈 새 `ResultStore()` 로 읽어 "다른 워커"를 흉내 낸다(같은 프로세스 안의 새 객체이지 진짜 다른 프로세스는 아니다) |
| 7    | `TTL_SEC` 를 1 로 잠깐 바꿔 쓰고 1.3초 뒤 읽어 None 인지 본다                                                |
| 8    | `rc._client` 를 닫힌 포트의 클라이언트로 바꿔 장애를 만들고, 쓰기가 예외 없이 지나가는지와 메모리로 읽는지를 본다. 끝나면 원래 클라이언트로 되돌린다 |


## 3. 결과를 읽는 순서

1. ① 스크립트 결과: 12개 항목이 모두 PASS 인가.
2. ② Redis 키: 값에 `status`, `worker`, `partition` 이 있고 TTL 이 걸려 있는가.
3. ④ 다른 워커 주소 조회: `200` 인가. 이 한 가지가 Day 23 의 가장 큰 변화다.
4. ⑤ 재시작 뒤 조회: 남아 있는가.


## 4. 확인하지 않은 것

| 항목                                | 내용                                                                                                                                  |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `agent_handler` 의 `to_thread` 호출 | 샌드박스에서는 `aiokafka` 가 없어 실행하지 못했다. 내 환경의 ③ 에서 요청 4건이 FAILED 없이 DONE 으로 끝났으므로(DAY23_RESULT 3-3) 이 호출 경로는 오류 없이 실행된 것으로 본다. 워커 로그에서 `to_thread` 동작 자체를 따로 본 것은 아니다                                              |
| 읽기-수정-쓰기 경합                 | `_put` 은 읽고 합쳐 다시 쓴다. 같은 `requestId` 는 한 워커가 순서대로 처리하므로(PROCESSING → DONE) 경합이 없다고 보지만 동시 갱신은 시험하지 않았다 |
| Redis 가 느릴 때(끊기지는 않고)      | 소켓 시간 제한(기본 2초)까지 기다린 뒤 경고하고 메모리로 대신한다. 이 동작은 장애(접속 거부) 경우만 시험했다                           |
