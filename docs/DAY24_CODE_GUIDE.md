# Day 24 코드 가이드 — TTL 정책 코드 흐름

> 사용법과 확인 방법은 [DAY24_GUIDE.md](DAY24_GUIDE.md). 이 문서는 바뀐 코드가 어떤 순서로 도는지 설명한다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, **6379 = Redis**.

**전체 목적:** 흩어져 있던 TTL 값이 `ttl_policy.py` 한곳으로 모이고, 결과 저장소와 대화 저장소가 그 값을 어떻게 쓰는지, 그리고 확인 스크립트가 만료를 어떻게 빠르게 시험하는지를 코드 순서대로 이해한다.


## 1. 전체 흐름

```
                         ttl_policy.py (한곳)
                  ┌──────────┬────────────┬──────────────┐
                  │ PROCESSING 600        │ 대화 3600     │   (락 150 은 redis_lock.py)
                  │ DONE 86400, FAILED 86400              │
                  └──────────┴────────────┴──────────────┘
                        │                         │
            result_store._put                conversation_store.append_turn
   status 를 보고 result_ttl(status)            TTL_SEC (= ttl_policy 의 값)
                        ▼                         ▼
   Redis  agent:result:<id>                Redis  agent:conversation:<sessionId>
```

| 구분             | Day 23                                  | Day 24                                                   |
| ---------------- | --------------------------------------- | -------------------------------------------------------- |
| 결과 TTL         | `TTL_SEC` 하나(86400), 상태와 무관      | 상태별: PROCESSING 600, DONE 86400, FAILED 86400         |
| 대화 TTL         | `conversation_store.py` 안에 3600       | `ttl_policy.CONVERSATION_TTL_SEC` (같은 값, 같은 환경변수) |
| 값이 잘못되면    | 0 이면 Redis 가 오류를 내는 때에야 앎    | 시작할 때 `ValueError`                                   |


## 2. 주요 코드

### 2-1. 값을 읽는 곳 (`ttl_policy.py`)

```python
def _positive_int(name, default):
    raw = os.getenv(name)
    value = default if raw is None else int(raw)
    if value <= 0:
        raise ValueError(f"{name} 는 1 이상의 초 단위 정수여야 한다 (값: {value})")
    return value

RESULT_PROCESSING_TTL_SEC = _positive_int("RESULT_PROCESSING_TTL_SEC", 600)
RESULT_DONE_TTL_SEC = _positive_int("RESULT_DONE_TTL_SEC", _positive_int("RESULT_TTL_SEC", 86400))
RESULT_FAILED_TTL_SEC = _positive_int("RESULT_FAILED_TTL_SEC", RESULT_DONE_TTL_SEC)
CONVERSATION_TTL_SEC = _positive_int("CONVERSATION_TTL_SEC", 3600)
```

| 부분                         | 설명                                                                                                                    |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 0 이하 거부                  | Redis 의 `SET ... EX 0` 은 오류다. 서버가 뜨고 첫 요청에서 터지기보다 시작할 때 알리는 편이 낫다                         |
| `RESULT_TTL_SEC` 를 DONE 기본값으로 | Day 23 의 환경변수만 줘 둔 환경에서도 같은 값(DONE, FAILED)으로 동작한다. 새 변수가 있으면 새 변수가 우선한다            |
| FAILED 기본값이 DONE 값      | 따로 줄이고 싶으면 `RESULT_FAILED_TTL_SEC` 만 주면 된다                                                                  |

### 2-2. 결과를 쓸 때 (`result_store._put`)

```python
item.update(fields)                       # 새 필드를 합친다 → item["status"] 가 지금 상태
...
rc.set_json(result_key(request_id), item, ttl_sec=ttl_policy.result_ttl(item.get("status")))
```

`set_processing` 이 `status=PROCESSING` 을 합치면 600초, 이어서 `set_done` 이 `status=DONE` 을 합치면 86400초로 **같은 키에 TTL 을 다시 건다**. Redis 의 `SET` 은 키를 덮어쓰면서 이전 TTL 을 지우고 새 TTL 을 시작한다. 그래서 별도의 `EXPIRE` 호출 없이 상태가 바뀔 때 TTL 이 따라 바뀐다.

`result_ttl` 은 모듈 변수를 **호출할 때** 읽는다. 확인 스크립트가 값을 잠깐 바꿔서 만료를 짧게 시험하려면 이래야 한다(2-4).

### 2-3. 대화를 쓸 때 (`conversation_store.append_turn`)

바뀐 것은 `TTL_SEC` 가 어디서 오는지뿐이다(`ttl_policy.CONVERSATION_TTL_SEC`). 쓸 때마다 TTL 이 새로 시작되는 동작(슬라이딩)은 Day 20 그대로다.

### 2-4. 확인 스크립트가 만료를 시험하는 방법 (`ttl_check.py`)

| 항목 | 하는 일                                                                                                                                      |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 3~5  | 저장한 뒤 `rc.ttl_sec` 로 남은 시간을 읽어 정책 값에서 5초 이내로 줄어든 값인지 본다(`near`)                                                   |
| 6    | `ttl_policy.RESULT_PROCESSING_TTL_SEC` 를 1 로 잠깐 바꿔 쓰고, 1.3초 뒤 키가 사라졌는지 본다. 600초를 기다리지 않고 "죽은 워커의 PROCESSING" 을 흉내 낸 것이다 |
| 8    | `conversation_store.TTL_SEC` 를 6 으로 잠깐 바꿔 저장 → 3초 뒤 TTL 읽기 → 다시 저장 → TTL 읽기(6 근처로 돌아옴) → 6.5초 뒤 대화가 비었는지 본다   |
| 9    | `with lock(...)` 안에서 락 키의 TTL 을 읽고, 블록을 나온 뒤 키가 없는지 본다                                                                  |
| 10   | `agent:*` 를 `SCAN` 으로 훑어 TTL 이 -1(영구)인 키를 찾는다. 키를 읽기만 하고 지우지 않는다. 이 스크립트가 만든 `day24check-` 키는 제외한다       |

값을 바꾸는 항목은 끝나면(`finally`) 원래 값으로 되돌린다.


## 3. 결과를 읽는 순서

1. ① 스크립트: 12개 항목이 모두 PASS 인가. 10번이 FAIL 이면 영구 키가 무엇인지 본다.
2. ② 실제 요청: TTL 이 `-2` → 600 근처 → 86400 근처로 바뀌는가.
3. ③ 키별 TTL: `-1` 이 있는 줄이 없는가.


## 4. 확인하지 않은 것

| 항목                                | 내용                                                                                                                                              |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| PROCESSING 도중 TTL 만료            | 처리 시간은 최대 120초(`AGENT_TIMEOUT_SEC`)이고 PROCESSING TTL 은 600초라 처리 중에 만료되지 않는다고 본다. 만료 뒤에 `set_done` 이 오면 키가 새로 만들어지지만(`worker`, `partition`, `offset` 은 빠진다) 이 경우는 시험하지 않았다 |
| 환경변수로 값을 바꾼 서버 실행      | 샌드박스에서 `ttl_policy` 를 직접 불러 값이 바뀌는 것만 보았다. 워커를 환경변수와 함께 띄운 경우는 확인하지 않았다                                  |
| 만료 시점 정확도                    | Redis 는 만료된 키를 읽을 때와 주기적 검사 때 지운다. `ttl_check.py` 는 읽는 쪽(`GET`)으로 확인하므로 키가 메모리에서 실제로 치워지는 시점은 보지 않았다 |
