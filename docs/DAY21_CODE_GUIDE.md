# Day 21 코드 가이드 — 분산 락이 코드에서 어떻게 걸리고 풀리는가

> 실행 방법과 확인 항목은 [DAY21_GUIDE.md](DAY21_GUIDE.md). 이 문서는 **코드가 어떤 순서로 무엇을 하는지**를 파일 위치, 함수, 줄 단위 설명으로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이고, 줄 번호는 작성 시점(2026-10-13) 기준이라 코드를 고치면 밀릴 수 있으니 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커(1번)**, **8001 = FastAPI 워커(2번)**, **6379 = Redis**, 9092 = Kafka.
> 상태: 코드를 읽고 설명한 문서다. 코드의 동작은 샌드박스(실제 `redis-server`, 가짜 모델)에서 확인했고, 내 환경에서의 실행은 아직 하지 않았다(DAY21_GUIDE 6번).

**목적:** Day 20 의 `run_agent`(이전 대화 읽기 → 모델 → 저장)에 "같은 세션은 한 번에 한 요청"이라는 규칙을 어떻게 얹었는지 따라간다. 락을 잡고 푸는 함수(`redis_lock.py`), 그것을 부르는 곳(`agent_graph.run_agent`), 오류가 났을 때 어떻게 되는지를 파일 위치와 함께 정리한다.


## 1. 전체 흐름

```
요청(sessionId=s1)  ──▶  run_agent(question, provider, "s1")                         [agent_graph.py]
                          │
                          ├─ session_id 가 없거나 SESSION_LOCK_ENABLED=false → 락 없이 _run_agent_turn
                          │
                          └─ with redis_lock.lock("conversation:s1", fail_open=True):   ← 락 잡기 (못 잡으면 기다림)
                                 │
                                 └─ _run_agent_turn(...)                            [Day 20 의 본문 그대로]
                                        ① 이전 대화 읽기 → ② 입력 만들기 → ③ 모델 → ④ 답 꺼내기 → ⑤ 대화 저장
                              (with 블록을 나오면 — 정상이든 예외든 — 락 풀기: 내 토큰일 때만 DEL)
```

Day 20 의 ①~⑤ 는 한 줄도 바꾸지 않았다. 함수 이름만 `run_agent` 에서 `_run_agent_turn` 으로 바꾸고, `run_agent` 라는 이름은 락을 잡고 `_run_agent_turn` 을 부르는 얇은 겉껍데기가 맡는다. 그래서 `agent_handler.py`, `routers/agent.py` 가 부르는 `run_agent(question, provider, session_id)` 호출 모양은 그대로다.

| 단계 | 하는 일                                          | 파일                                                         | 함수 (줄)                                        |
| ---- | ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------ |
| A    | 락을 걸고 `_run_agent_turn` 호출, 스위치 확인     | `d2-ai-python/app/services/agent_graph.py`                   | `run_agent` (369), `SESSION_LOCK_ENABLED` (364)  |
| B    | 락 안에서 실행되는 Day 20 본문                    | `d2-ai-python/app/services/agent_graph.py`                   | `_run_agent_turn` (381)                          |
| C    | 락을 잡고 푸는 컨텍스트 매니저                    | `d2-ai-python/app/services/redis_lock.py`                    | `lock` (99), `_acquire` (80)                     |
| D    | 한 번 시도해 잡기 / 내 토큰일 때만 풀기           | `d2-ai-python/app/services/redis_lock.py`                    | `try_acquire` (67), `release` (75)               |
| E    | 락 키 이름, Redis 에 보낼 Lua 스크립트            | `d2-ai-python/app/services/redis_lock.py`                    | `lock_key` (62), `_RELEASE_SCRIPT` (49)          |
| F    | Redis 실제 호출 (Day 19)                          | `d2-ai-python/app/services/redis_client.py`                  | `get_client`, `make_key`                         |
| G    | 락 대기 시간 초과를 HTTP 409 로 바꿈              | `d2-ai-python/app/routers/agent.py`                          | `run_agent_endpoint`                             |
| H    | 확인 스크립트 (21개 항목)                         | `d2-ai-python/app/checks/lock_check.py`                    | `main` (37)                                      |


## 2. 주요 코드

### 2-1. 락을 거는 곳: run_agent (A)

**목적:** 같은 `session_id` 의 요청이 `_run_agent_turn`(읽기 ~ 저장)을 한 번에 하나씩만 실행하게 감싼다.

- 파일: `d2-ai-python/app/services/agent_graph.py` (`run_agent` 369줄)

```python
SESSION_LOCK_ENABLED = os.getenv("SESSION_LOCK_ENABLED", "true").strip().lower() != "false"

def run_agent(question, provider="gemini", session_id=None):
    if not session_id or not SESSION_LOCK_ENABLED:                 # (a)
        return _run_agent_turn(question, provider, session_id)
    with redis_lock.lock(f"conversation:{session_id}", fail_open=True):   # (b)
        return _run_agent_turn(question, provider, session_id)           # (c)
```

| 표시 | 하는 일                                                                                                                                                                      |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a)  | 세션이 없으면(새 대화) 대화를 읽지도 저장하지도 않으므로 보호할 것이 없다. 락 없이 바로 실행한다. 스위치를 `false` 로 끈 경우도 같다. `SESSION_LOCK_ENABLED` 는 서버 시작 때 한 번 읽는다 |
| (b)  | 락 이름은 `conversation:<sessionId>`. Redis 키는 `redis_lock.lock_key` 가 `agent:lock:` 접두어를 붙여 `agent:lock:conversation:<sessionId>` 가 된다. 세션마다 이름이 달라서 다른 세션은 서로 기다리지 않는다 |
| (b)  | `fail_open=True`: Redis 오류로 락을 못 잡으면 예외 대신 락 없이 (c) 를 실행한다. 이 경우 (c) 안의 ①, ⑤ 도 Redis 오류를 삼키고 답을 돌려주므로(Day 20) 답은 정상으로 온다 |
| (c)  | 블록 안의 `return` 도 `with` 를 거쳐 나가므로 정상 종료, 예외, `return` 어느 쪽이든 락이 풀린다                                                                              |

`LockTimeout` 은 여기서 잡지 않고 호출한 쪽으로 올라간다. `agent_handler` 는 모든 예외를 받아 요청을 `FAILED` 로 기록하고(코드를 읽은 판단, 실행 확인은 못 함), `routers/agent.py` 는 409 로 바꾼다(G).

### 2-2. 락을 잡는다: try_acquire 와 _acquire (D, C)

**목적:** "키가 없을 때만 저장"을 Redis 한 번의 명령으로 해서, 두 요청이 동시에 와도 한 쪽만 성공하게 한다.

- 파일: `d2-ai-python/app/services/redis_lock.py` (`try_acquire` 67줄, `_acquire` 80줄)

```python
def try_acquire(name, ttl_sec=LOCK_TTL_SEC):
    token = uuid.uuid4().hex
    ok = rc.get_client().set(lock_key(name), token, nx=True, px=max(1, int(ttl_sec * 1000)))
    return token if ok else None
```

| 요소                            | 하는 일                                                                                                                                                           |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uuid.uuid4().hex`              | 락마다 새로 만드는 무작위 토큰(32자리). 키의 값으로 저장해 "내 락"임을 표시한다                                                                                   |
| `set(..., nx=True, px=...)`     | Redis 명령 `SET key token NX PX ms` 하나. 키가 없을 때만 저장하므로 두 요청이 동시에 보내도 Redis 가 한 줄로 세워 하나만 성공시킨다. 저장과 만료 설정도 한 명령이라 "잡았는데 만료가 안 걸린" 상태가 없다 |
| `px=max(1, int(ttl_sec*1000))`  | 만료를 밀리초 정수로. 0.3초 같은 짧은 값도 확인 스크립트에서 쓰려고 `EX`(초 정수) 대신 `PX` 를 썼다                                                                  |
| 반환                            | `set` 이 성공하면 `True`, 이미 키가 있으면 `None`. 성공이면 토큰, 실패면 `None`                                                                                    |

```python
def _acquire(name, ttl_sec, wait_sec, fail_open):
    deadline = time.monotonic() + wait_sec
    while True:
        try:
            token = try_acquire(name, ttl_sec)
        except redis.RedisError as e:
            if not fail_open: raise
            _log.warning("락을 못 잡음, Redis 오류라 락 없이 계속 ...")
            return None
        if token is not None: return token
        if time.monotonic() >= deadline: raise LockTimeout(...)
        time.sleep(RETRY_INTERVAL_SEC * (0.5 + random.random()))
```

| 줄/요소                        | 하는 일                                                                                                                                       |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `deadline`                     | 대기 한도를 시계 시간(`monotonic`)으로 정한다. 시스템 시계가 바뀌어도 흔들리지 않는다                                                          |
| `except redis.RedisError`      | 연결 실패, 타임아웃 등 Redis 쪽 오류. `fail_open=False` 면 그대로 올리고, `True` 면 경고만 남기고 `None`(토큰 없음)을 돌려줘 락 없이 진행한다  |
| `if token is not None`         | 잡았으면 바로 반환                                                                                                                            |
| `deadline` 확인                | 못 잡았고 한도가 지났으면 `LockTimeout`. `LockTimeout` 은 `RedisError` 가 아니라서 `fail_open` 과 상관없이 항상 올라간다                       |
| `time.sleep(... * (0.5~1.5))`  | 0.1초 쉬고 다시 시도. 여러 요청이 같은 순간에 몰려 다시 부딪히지 않게 간격을 조금씩 흔든다. 폴링 방식이라 풀린 직후에 바로 잡히지는 않고 최대 0.15초 정도 늦을 수 있다 |

### 2-3. 락을 푼다: release 와 lock (D, C)

**목적:** 내가 잡은 락만 풀고, 내 락이 이미 만료되어 남이 새로 잡은 것은 지우지 않는다.

- 파일: `d2-ai-python/app/services/redis_lock.py` (`_RELEASE_SCRIPT` 49줄, `release` 75줄, `lock` 99줄)

```python
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

def release(name, token):
    return rc.get_client().eval(_RELEASE_SCRIPT, 1, lock_key(name), token) == 1
```

| 요소                     | 하는 일                                                                                                                                                                |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lua 스크립트             | Redis 서버 안에서 실행되고, 실행되는 동안 다른 명령이 끼어들지 못한다. "값이 내 토큰이면 지운다"가 쪼개지지 않는다                                                       |
| 왜 그냥 `DELETE` 가 아닌가 | 락이 만료된 뒤 다른 요청 B 가 새로 잡았는데, 늦게 끝난 A 가 `DELETE` 를 하면 B 의 락을 지운다. 토큰을 비교하면 A 의 `release` 는 0 을 돌려주고 아무것도 지우지 않는다(확인 스크립트 7번) |
| 왜 `GET` 후 `DEL` 이 아닌가 | 파이썬에서 `GET` 으로 확인하고 `DEL` 하면 그 사이에 만료와 재획득이 끼어들 수 있다. 한 스크립트로 묶어 그 틈을 없앤다                                                    |
| 반환                     | 지웠으면 `True`, 이미 없거나 남의 것이면 `False`                                                                                                                         |

```python
@contextmanager
def lock(name, ttl_sec=LOCK_TTL_SEC, wait_sec=LOCK_WAIT_SEC, fail_open=False):
    token = _acquire(name, ttl_sec, wait_sec, fail_open)      # (1)
    try:
        yield token                                            # (2)
    finally:
        if token is not None:                                  # (3)
            try:
                if not release(name, token):
                    _log.warning("락이 풀기 전에 만료됨 ...")   # (4)
            except redis.RedisError as e:
                _log.warning("락을 푸는 중 Redis 오류 ...")     # (5)
```

| 표시 | 하는 일                                                                                                                                                  |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (1)  | 락을 잡을 때까지 기다린다. 여기서 `LockTimeout` 이 나면 (2) 에 들어가지 않으므로 `finally` 도 실행되지 않는다(풀 락이 없다)                                |
| (2)  | `with` 블록이 실행되는 자리. 토큰을 넘겨 준다(`fail_open` 으로 락 없이 진행하면 `None`)                                                                    |
| (3)  | 블록이 정상이든 예외든 여기를 지난다. 락 없이 진행한 경우(`None`)는 풀 것이 없다                                                                          |
| (4)  | `release` 가 `False` 면 락이 먼저 만료된 것이다. 일이 `ttl_sec` 보다 오래 걸렸다는 신호이고, 그동안은 보호되지 않았다. 경고만 남긴다                           |
| (5)  | 푸는 중에 Redis 가 죽어도 예외로 요청을 망치지 않는다. 락은 만료로 어차피 풀린다                                                                           |

### 2-4. 오류를 HTTP 로: 409 (G)

**목적:** 앞 요청이 너무 오래 걸려 락을 못 잡은 경우를 500(서버 오류)이 아니라 409(충돌)로 알린다.

- 파일: `d2-ai-python/app/routers/agent.py` (`run_agent_endpoint`)

```python
try:
    answer = run_agent(req.query, provider=req.provider, session_id=req.session_id)
except LockTimeout as e:
    raise HTTPException(status_code=409, detail=f"같은 세션의 앞 요청이 아직 처리 중이다: {e}")
```

Kafka 경로(`agent_handler`)는 바꾸지 않았다. `LockTimeout` 은 `Exception` 이어서 기존 `except Exception` 에서 요청을 `FAILED`(`LockTimeout: ...` 메시지)로 기록하고 다시 던질 것으로 읽었다. 실행해 확인하지는 못했다.

### 2-5. 확인 스크립트 (H)

**목적:** 위 코드가 의도대로 동작하는지 실제 Redis 에서 21개 항목으로 본다.

- 파일: `d2-ai-python/app/checks/lock_check.py` (`main`)

| 항목 | 확인하는 것                                                                                          | 어떻게                                                                                                 |
| ---- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 1    | 연결                                                                                                 | `ping`                                                                                                 |
| 2~4  | 잡기, 만료(PX), 이미 잡힌 락, 남의 토큰으로 풀기 실패, 내 토큰으로 풀기, 풀린 뒤 재획득               | `try_acquire`, `release` 호출 후 Redis 의 값과 `pttl` 직접 조회                                         |
| 5    | 앞 사람이 풀 때까지 기다렸다 잡기                                                                     | 0.5초 뒤 풀리게 `threading.Timer` 를 걸고 `lock()` 이 기다린 시간을 잼                                  |
| 6    | `LockTimeout`                                                                                        | 오래 쥔 락에 0.5초만 기다리게 해 예외와 대기 시간을 확인                                               |
| 7    | 만료된 락을 늦게 푸는 쪽이 남의 락을 지우지 않음                                                      | a 를 0.3초 만료로 잡고 0.5초 쉰 뒤 b 가 잡고, a 가 `release` 하면 `False` 이고 b 의 값이 남아야 함       |
| 8    | 같은 세션 저장을 20개 스레드가 같은 순간에: 락 없음(정보용), 락 있음(20턴 모두 남아야 판정)            | `threading.Barrier` 로 동시에 시작, `cs.append_turn` 직접 호출                                          |
| 9    | Redis 가 안 될 때 `fail_open` True/False                                                              | `rc._client` 를 닫힌 포트로 잠깐 바꿔 끼움                                                              |
| 10   | `run_agent` 연결: 락 있음(4턴 저장, 이전 메시지 수 0·2·4·6, 차례로 처리), 락 없음(정보용), 다른 세션이 서로 안 기다림, 끝나면 락이 남지 않음 | `AGENT_GRAPH` 를 0.4초 자는 가짜 모델로 바꿔 끼우고 스레드 4개를 동시에 시작. `langchain` 이 없으면 이 항목은 건너뜀 |

스크립트는 끝나면(실패해도) 자기가 만든 `day21check:...` 키를 지운다. 항목 8, 10 의 "락 없음" 줄은 매번 숫자가 달라서 판정하지 않고 `[INFO]` 로만 출력한다.


## 3. 두 워커가 같은 세션을 동시에 받으면 (시간 순서)

락이 있을 때의 흐름이다. 아래는 코드를 읽고 정리한 설명이고, 같은 구조를 샌드박스의 스레드로 확인했다(DAY21_GUIDE 6번). 두 워커(8000, 8001)로 확인한 것은 아니다.

| 시각 | 워커 A (질문 Qa)                                  | 워커 B (질문 Qb)                                       | Redis                                              |
| ---- | ------------------------------------------------- | ------------------------------------------------------ | -------------------------------------------------- |
| t0   | `SET lock:s1 tokA NX PX` → 성공                   | `SET lock:s1 tokB NX PX` → 실패(키 있음)               | `lock:s1 = tokA`                                   |
| t1   | ① 대화 읽기 → `[]`, ③ 모델 호출 (수 초)           | 0.1초 간격으로 다시 시도, 계속 실패                    | `lock:s1 = tokA`                                   |
| t2   | ⑤ `[Qa, Aa]` 저장, `release(tokA)` → 락 삭제       | 다음 시도에서 `SET ... NX` 성공                        | `conversation:s1 = [Qa, Aa]`, `lock:s1 = tokB`     |
| t3   |                                                   | ① 대화 읽기 → `[Qa, Aa]` (앞 대화를 봄), ③ 모델 호출    | -                                                  |
| t4   |                                                   | ⑤ `[Qa, Aa, Qb, Ab]` 저장, `release(tokB)`             | `conversation:s1 = [Qa, Aa, Qb, Ab]`, 락 없음      |

Day 20 의 문제(턴 유실, 앞 질문을 모르고 답함)는 B 가 A 의 저장이 끝나기 전에는 대화를 읽지도 쓰지도 않으므로 사라진다. 대가는 B 의 응답이 A 의 처리 시간만큼 늦어지는 것이다.


## 4. 실패 지점별 동작

코드를 읽고 정리한 예상이다. 샌드박스에서 확인한 항목은 표시했다.

| 상황                                              | 동작                                                                                                         | 확인 |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---- |
| 모델 호출이나 도구에서 예외                       | `with` 를 나가며 락이 풀리고 예외가 올라간다. 대화는 저장되지 않는다(⑤ 에 도달 못 함)                          | 샌드박스에서 락이 풀림을 확인 |
| 락을 `wait_sec` 안에 못 잡음                      | `LockTimeout`. 직접 경로는 409, Kafka 경로는 요청 `FAILED`                                                    | 409 는 샌드박스에서 확인, `FAILED` 는 미확인 |
| Redis 가 처음부터 안 됨                           | `fail_open` 으로 락 없이 진행. ①, ⑤ 도 경고만 남기고 답은 돌려줌                                              | 락 함수 수준만 확인(닫힌 포트) |
| 락을 쥔 워커가 죽음                               | 락 키가 `REDIS_LOCK_TTL_SEC`(150초) 뒤 만료. 그동안 같은 세션 요청은 기다리다 `LockTimeout` 가 날 수 있음     | 미확인 |
| 처리가 락 만료보다 오래 걸림                      | 락이 풀려 다른 요청이 들어올 수 있음. 늦게 끝난 쪽의 `release` 는 `False`(남의 락을 지우지 않음), 경고 로그    | 만료 후 `release` 동작은 확인, 전체 시나리오는 미확인 |
| 푸는 중 Redis 오류                                | 경고만 남기고 넘어감. 락은 만료로 풀림                                                                       | 미확인 |


## 5. 설계 선택과 이유

| 선택                                              | 이유                                                                                                                                       | 대신 포기한 것                                                                                          |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| 락 범위를 읽기 ~ 저장 전체(모델 호출 포함)로      | 저장만 감싸면 유실은 막지만, 두 요청이 둘 다 빈 대화를 읽고 시작하는 문제(앞 질문을 모름)는 남는다. 전체를 감싸야 뒤 요청이 앞 답을 본다       | 같은 세션의 처리량. 같은 세션은 차례로만 처리되고, 락을 쥐는 시간이 모델 응답 시간(수 초~수십 초)만큼 길다 |
| 세션별 락 (`conversation:<sessionId>`)            | 다른 세션은 서로 막지 않는다                                                                                                                 | -                                                                                                       |
| `SET NX PX` + 토큰 + Lua 해제                      | 락 하나를 안전하게 쓰는 최소 구성. 잡기가 한 명령이고, 죽은 워커의 락은 만료되고, 남의 락을 지우지 않는다                                     | 락 연장(갱신), 복제 환경의 안전성                                                                       |
| 직접 구현                                         | 동작 원리를 코드로 보이려고. redis-py 에 같은 일을 하는 내장 락(`client.lock()`)이 있고, 실무에서는 그것이나 Redlock 구현체를 쓰는 선택도 있다   | 검증된 구현의 세부 처리                                                                                 |
| `fail_open=True`                                  | 대화 이어가기가 부가 기능이라 Redis 장애로 답까지 막지 않는 Day 20 의 방침을 따른다                                                          | Redis 장애 중에는 동시성 보호가 없다                                                                    |
| 대기는 폴링(0.1초 간격 재시도)                    | 구현이 단순하다                                                                                                                              | 풀린 직후 최대 0.15초 늦게 잡힘, 대기하는 요청이 많으면 Redis 에 반복 요청                              |
| `SESSION_LOCK_ENABLED` 스위치                     | Day 22 에서 락 있음/없음을 같은 코드로 비교하려고                                                                                            | 설정 하나가 늘어남                                                                                      |
| `run_agent` 를 감싸는 방식 (`_run_agent_turn` 분리) | Day 20 의 ①~⑤ 코드와 주석을 그대로 두고, 호출하는 쪽도 바꾸지 않기 위해                                                                      | 함수가 하나 늘어남                                                                                      |


## 6. 코드 흐름을 따라 읽는 순서

1. `agent_graph.run_agent` — 락을 거는 겉껍데기 (A)
2. `redis_lock.lock` — `try/finally` 로 푸는 구조 (C)
3. `redis_lock._acquire`, `try_acquire` — 한 명령으로 잡고 기다리는 방식 (D)
4. `redis_lock.release` 와 `_RELEASE_SCRIPT` — 토큰 비교 해제 (D, E)
5. `agent_graph._run_agent_turn` — Day 20 의 ①~⑤ (DAY20_CODE_GUIDE 2-4)
6. `lock_check.py` — 항목 7(만료된 락)과 10(`run_agent` 연결)


## 7. 확인하지 않은 것

| 항목                                              | 내용                                                                                                           |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| 내 환경에서의 실행                                | `redis1`, 실제 Gemini, 워커 2대로 확인하지 않았다. 위 시간표(3번)는 코드를 읽은 설명이다                         |
| `agent_handler` 경로                              | `LockTimeout` → `FAILED` 기록을 실행하지 못했다(샌드박스에 `aiokafka` 없음)                                     |
| 락 만료와 워커 사망                               | 150초 만료, 락을 쥔 채 죽은 워커, 만료보다 오래 걸리는 요청의 전체 시나리오를 실행하지 않았다                    |
| 락의 비용                                         | 락을 거는 데 걸리는 시간, 같은 세션에 요청이 몰릴 때의 대기 시간 분포를 재지 않았다                               |
