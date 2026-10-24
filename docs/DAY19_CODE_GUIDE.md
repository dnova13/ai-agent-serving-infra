# Day 19 코드 가이드 — Redis 접속, 기본 함수, 확인 스크립트, /health 의 Redis 상태

> 실행하는 방법과 확인 항목은 [DAY19_GUIDE.md](DAY19_GUIDE.md), 결과는 [DAY19_RESULT.md](DAY19_RESULT.md). 이 문서는 이후 Day 20~24 가 모두 가져다 쓰는 `redis_client.py` 의 구조를 설명한다.
> 파일 경로는 `d2-ai-python/app/` 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **6379 = Redis(`redis1` 컨테이너)**, **8000 = FastAPI 워커 1**(`/health` 확인용).
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 19 커밋(`26e4a80`, `/health` 는 `0e30c3d`) 시점의 것이다.

**전체 목적:** 에이전트 상태(Day 20), 분산 락(Day 21), 결과 저장(Day 23)을 Redis 에 올리기 전에, 연결과 기본 동작(저장, 조회, 삭제, 만료)을 담은 얇은 모듈과 그것을 확인하는 스크립트가 어떻게 만들어졌는지 따라간다.


## 1. 전체 흐름

```
redis_client.py   get_client() → make_key() → set_json / get_json / delete / exists / ttl_sec / ping
redis_check.py    ping → 저장·조회 → 덮어쓰기 → 없는 키 → 영구 키 → TTL 만료 → 삭제  (PASS / FAIL 출력, 테스트 키는 끝에 지움)
routers/health.py GET /health → {"status":"ok","kafka_consumer":...,"redis": ping 결과}
```

| 단계 | 하는 일                    | 파일                                            | 함수                                                    |
| ---- | -------------------------- | ----------------------------------------------- | ------------------------------------------------------- |
| A    | 연결과 기본 함수           | `d2-ai-python/app/services/redis_client.py`     | `get_client`, `make_key`, `ping`, `set_json`, `get_json`, `delete`, `exists`, `ttl_sec` |
| B    | 동작 확인                  | `d2-ai-python/app/checks/redis_check.py`      | `main`                                                  |
| C    | 상태 노출                  | `d2-ai-python/app/routers/health.py`            | `health_check`                                          |
| D    | 의존성                     | `d2-ai-python/requirements.txt`, `d2-ai-python/app/requirements.txt` | `redis` 추가                  |


## 2. 주요 코드

### 2-1. 연결 (`get_client`)

```python
_client = redis.Redis(host=HOST, port=PORT, db=DB, decode_responses=True,
                      socket_connect_timeout=CONNECT_TIMEOUT_SEC, socket_timeout=SOCKET_TIMEOUT_SEC)
```

| 부분                       | 설명                                                                                                                              |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 환경변수                   | `REDIS_HOST`(localhost), `REDIS_PORT`(6379), `REDIS_DB`(0), `REDIS_CONNECT_TIMEOUT_SEC`(1), `REDIS_SOCKET_TIMEOUT_SEC`(2)          |
| 프로세스에 하나만 만듦      | 전역 `_client` 를 재사용한다. 내부에 연결 풀이 있어서 스레드에서 같이 써도 된다                                                     |
| 짧은 타임아웃              | Redis 가 꺼져 있어도 서버가 오래 멈추지 않게 연결 1초, 소켓 2초로 끊는다                                                            |
| `decode_responses=True`    | bytes 가 아니라 str 로 받는다                                                                                                       |
| 동기 클라이언트            | 에이전트가 `asyncio.to_thread` 로 도는 스레드에서는 그대로 쓸 수 있다. `async` 함수 안에서 직접 부르면 그동안 이벤트 루프가 멈추므로 `to_thread` 로 감싼다(파일 머리의 주의). Day 23 의 `agent_handler` 와 `/health` 가 그렇게 한다 |

### 2-2. 기본 함수

| 함수                         | 동작                                                                                                                          |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `make_key(*parts)`           | `agent:` 접두어를 붙여 `agent:state:abc` 처럼 만든다. 다른 용도의 키와 섞이지 않게 한다                                          |
| `set_json(key, value, ttl_sec)` | dict 나 list 를 JSON 문자열로 저장한다. `ensure_ascii=False` 라 `redis-cli` 에서 한글이 그대로 읽힌다. `ttl_sec` 이 있으면 그 초 뒤에 Redis 가 지운다. 같은 키는 덮어쓴다 |
| `get_json(key)`              | 꺼내서 파싱한다. 키가 없거나 만료됐으면 `None`                                                                                 |
| `delete(key)`                | 실제로 지웠으면 `True`, 원래 없었으면 `False`                                                                                  |
| `exists(key)`                | 있으면 `True`                                                                                                                  |
| `ttl_sec(key)`               | 남은 초. Redis `TTL` 은 없는 키 -2, 영구 키 -1 을 주는데, -2 만 `None` 으로 바꾸고 -1 은 그대로 둔다                              |
| `ping()`                     | 예외를 던지지 않고 `{"connected": True, "latencyMs": ...}` 또는 `{"connected": False, "error": "TypeName: ..."}` 를 돌려준다. Redis 가 꺼져 있어도 `/health` 가 응답해야 하기 때문 |

### 2-3. 확인 스크립트 (`redis_check.py`)

테스트 키는 `agent:day19:check:<run_id>:...` 아래에만 만들고 끝에 지운다(중간에 실패해도 `finally` 에서 지운다).

| 항목 | 확인하는 것                                                                         |
| ---- | ----------------------------------------------------------------------------------- |
| 1    | `ping` 이 연결됨(실패하면 나머지는 건너뛰고 FAIL)                                    |
| 2    | 한글, 중첩 dict, list, bool, null 이 저장 후 조회에서 그대로 돌아옴                  |
| 3    | 같은 키에 다시 쓰면 덮어씀                                                           |
| 4    | 없는 키는 `None`, `exists` 는 있는 키 True 와 없는 키 False                          |
| 5    | TTL 없이 저장하면 `ttl_sec == -1` (영구)                                             |
| 6    | `ttl_sec=3` 으로 저장하면 남은 시간이 0~3초, 4초 기다린 뒤 조회와 `ttl_sec` 모두 `None` |
| 7    | `delete` 가 있던 키는 True, 이미 없는 키는 False                                      |

마지막 줄은 `RESULT: PASS` 또는 `RESULT: FAIL (실패한 항목)`. 항목 개수(스크립트 출력의 `n/n 통과`)는 이 문서에서 세지 않았다.

### 2-4. `/health` (`0e30c3d`)

```python
redis_status = await asyncio.to_thread(redis_client.ping)
return {"status": "ok", "kafka_consumer": ..., "redis": redis_status}
```

Redis 가 꺼져 있어도 `/health` 는 200 이고 `status` 는 `ok` 다(Kafka 와 같은 방침). `ping` 은 동기 호출이고 접속 실패 시 최대 1초(`REDIS_CONNECT_TIMEOUT_SEC`) 걸리므로, 이벤트 루프를 막지 않게 스레드에서 부른다(코드 주석).


## 3. 결과를 읽는 순서

1. `python -m app.services.redis_client` 의 `ping` 이 `connected: True` 와 지연(ms)을 보여 주는지 본다.
2. `python -m app.checks.redis_check` 의 항목이 모두 `PASS` 이고 마지막이 `RESULT: PASS` 인지 본다.
3. `GET /health` 의 `redis.connected` 가 `true` 인지, `docker stop redis1` 후에는 `false` 이면서 `status` 는 여전히 `ok` 인지 본다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 Redis 나 스크립트를 실행하지 않았다. 실제 출력은 DAY19_RESULT 를 따른다                                                |
| 현재 코드와의 차이    | `redis_client.py` 는 Day 19 커밋 이후 현재 파일과 차이가 없음을 `diff` 로 확인했다. 키 종류(`conversation`, `lock`, `result`)와 `redis_lock.py` 는 이후 Day 에 더해졌다 |
| 재접속                | 연결이 끊긴 뒤 다시 붙는 동작은 redis-py 의 기본 동작에 맡겼고 이 Day 에는 시험하지 않았다                                              |
