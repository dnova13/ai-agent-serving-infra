# Day 33 코드 가이드 — 같은 세션 동시 요청 스트레스 테스트가 도는 순서

> 실행 방법과 확인 항목은 [DAY33_GUIDE.md](DAY33_GUIDE.md), 결과는 [DAY33_RESULT.md](DAY33_RESULT.md). 이 문서는 **`lock_stress.py` 가 무엇을 어떤 순서로 하는지**를 함수 단위로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다.
> 포트: **6379 = Redis**. 이 스크립트는 Spring, Kafka, FastAPI 워커, Qdrant 를 쓰지 않는다.

**목적:** Day 22 의 `race_test.py` 는 N 이 2~10 이었다. 이 스크립트는 같은 세션 요청을 100개 이상 한 프로세스 안에서 동시에 보내, 락이 정상일 때와 깨지는 조건(기다림 한도, 락 만료, 연결 풀 부족)을 숫자로 본다.

**Day 33 의 코드 변경:** 새 파일 `d2-ai-python/app/experiments/lock_stress.py` 하나. 기존 코드(`redis_lock.py`, `redis_client.py`, `agent_graph.py`)는 바꾸지 않았다. 스크립트가 실행하는 동안만 일부 함수와 변수를 바꿔치기하고 끝나면 되돌린다.


## 1. 전체 흐름

```
main
 ├─ Redis ping (실패하면 종료 코드 2)
 ├─ 시나리오마다 run_scenario(...) 호출
 │     ① 가짜 모델과 계측을 끼워 넣는다 (AGENT_GRAPH, SESSION_LOCK_ENABLED, redis_lock.lock, redis_lock.try_acquire)
 │     ② 스레드 N개를 만들고 Barrier 에서 모아 같은 순간에 run_agent 를 호출
 │     ③ 끝나면 기록을 모아 Result 를 계산
 │     ④ finally 에서 바꿔치기한 것을 되돌리고, 테스트 세션의 대화와 락 키를 지운다
 ├─ print_result 로 출력
 └─ baseline, timeout, (pool 의 확대 실행) 만 judge_* 로 판정 → PASS/FAIL (종료 코드 0/1)
```

실행: `d2-ai-python` 에서 `python -m app.experiments.lock_stress [--scenarios …]`.


## 2. 시나리오와 설정

`main` 이 시나리오마다 `run_scenario` 에 다른 인자를 준다.

| 시나리오 | N | 락 | 기다림 한도 | 락 만료 | 모델 대기 | 판정 |
| -------- | -- | -- | ----------- | -------- | --------- | ---- |
| baseline | 100 | 켬 | 기본(100초) | 기본(150초) | 0.05초 | `judge_baseline` |
| nolock   | 100 | 끔 | 기본 | 기본 | 0.05초 | 안 함(정보용) |
| timeout  | 100 | 켬 | 2초 | 기본 | 0.05초 | `judge_timeout` |
| ttl      | 5   | 켬 | 기본 | 0.3초 | 0.5초 | 안 함(정보용) |
| sweep    | 10, 50, 100, 200 | 켬 | 기본 | 기본 | 0.05초 | 안 함(표만 출력) |
| pool     | 200 | 켬 | 기본 | 기본 | 0.05초 | 확대 실행(풀 300)만 `judge_baseline` |

기본 설정값은 `redis_lock.LOCK_WAIT_SEC`, `LOCK_TTL_SEC` 에서 읽는다. 옵션: `--n`, `--latency`, `--wait-sec`, `--ttl-sec`, `--ttl-latency`, `--ttl-n`, `--sweep`, `--pool-n`, `--pool-size`.


## 3. `run_scenario` — 한 번의 실험

### 3-1. 가짜 모델 (`FakeGraph`)

`ag.AGENT_GRAPH` 를 `FakeGraph` 로 바꾼다. `invoke` 가 불릴 때마다(락 안에서만 불린다):

1. `probe_lock` 안에서 `inside += 1`, `max_inside` 갱신, `calls += 1`
2. 이 요청의 기록에 진입 시각(`enter`)과 `prior`(= 넘겨받은 메시지 수 - 1, 즉 **앞 대화 메시지 수**)를 남김
3. `time.sleep(latency)` 로 모델이 답하는 시간을 흉내
4. `inside -= 1` 후 `"답:" + 질문` 을 AI 응답으로 돌려줌

`max_inside` 가 락의 정답지다. 락이 제대로 동작하면 1 이어야 한다. 요청 번호는 스레드마다 `threading.local()`(`tls.i`)에 넣어 둔다.

### 3-2. 바꿔치기와 계측

| 바꾸는 것 | 바꾼 내용 | 이유 |
| --------- | --------- | ---- |
| `ag.AGENT_GRAPH` | `FakeGraph()` | 돈이 드는 모델 대신 |
| `ag.SESSION_LOCK_ENABLED` | 시나리오의 `lock_on` | nolock 시나리오 |
| `redis_lock.lock` | `lock_with_limits` | 기다림 한도와 락 만료를 시나리오 값으로 주입 |
| `redis_lock.try_acquire` | `counting_try` | `SET NX` 시도 횟수를 센다 |
| 루트 로거 | `_WarnCounter` 핸들러 추가 | "만료됨", "락 없이 계속", "대화 기록을 못 불러옴/저장함" 경고를 센다 |

`lock_with_limits` 는 `fail_open` 인자를 원래대로 넘긴다(`run_agent` 가 `fail_open=True` 로 부르므로 그대로 적용된다). 그래서 연결 풀 부족 같은 Redis 오류가 나면 **실제 코드와 같이** 락 없이 계속된다. `_WarnCounter` 는 루트 로거에 달아서 경고가 화면(stderr)으로 새어 나오지 않게도 한다.

### 3-3. 스레드 실행

- 스레드 N개가 `worker(i)` 를 실행한다. `threading.Barrier(n)` 에서 모두 모인 뒤 같은 순간에 출발한다.
- 출발 시각(`start`)을 적고 `ag.run_agent(f"Q{i}", "gemini", session)` 를 호출한다.
- 결과는 `ok`, `LockTimeout` 이면 `timeout`, 그 밖의 예외는 `error` 로 기록한다.
- `run_agent` 가 요청마다 `[timing]` 줄을 찍으므로, 실행하는 동안은 `contextlib.redirect_stdout(io.StringIO())` 로 출력을 버린다.
- 세션 ID 는 `day33stress:<8자리>:<시나리오>:<N>` 으로 실행마다 새로 만든다.

### 3-4. 집계

모든 스레드가 끝난 뒤:

| 값 | 계산 |
| -- | ---- |
| Redis 전체 명령 | 실행 전후 `INFO stats` 의 `total_commands_processed` 차이에서 마지막 `INFO` 자신 1개를 뺀 값 |
| 대기 시간 | 모델 호출에 진입한 요청의 `enter - start` (정렬해서 p50, p95, 최대) |
| 앞 대화(`priors`) | 진입 시각 순서대로 나열 |
| 선착순 상관 | `spearman(start들, enter들)`. 1 이면 선착순, 0 이면 무관 |
| 저장된 턴 | `cs.load_history` 길이 / 2. 기대값은 `min(성공 수, MAX_TURNS)` |
| 쌍 확인 | 저장된 대화가 `(user Q, assistant "답:Q")` 쌍인가 |
| 저장 순서 | 저장된 질문들이 진입 순서의 마지막 성공 요청들과 같은가 |
| 락 키 | 끝난 뒤 `redis_lock.lock_key("conversation:<세션>")` 가 남았는가 |
| 연결 풀 | `client.connection_pool.max_connections` |

대화 보관이 최근 10턴(`CONVERSATION_MAX_TURNS`)뿐이라 N>10 에서는 저장된 턴 수만으로는 유실을 알 수 없다. 그래서 **모델 호출 구간 동시 실행 최대**, **앞 대화 메시지 수의 순서**(0, 2, 4, … 20 에서 멈춤), **저장 순서**를 함께 본다.

### 3-5. 정리

`finally` 에서 바꿔치기한 것을 모두 원래대로 돌리고, 루트 로거에서 핸들러를 빼고, 테스트 세션의 대화(`cs.clear`)와 락 키를 지운다. 실패해도 다음 시나리오에 영향을 남기지 않는다.


## 4. 보조 함수

| 함수 | 하는 일 |
| ---- | ------- |
| `spearman(xs, ys)` | 값을 순위로 바꾼 뒤 피어슨 상관. 값이 모두 같거나 2개 미만이면 0 |
| `percentile(sorted_vals, p)` | 정렬된 목록에서 p 번째 백분위 값(가장 가까운 위치) |
| `ideal_priors(ok)` | 차례로 처리됐을 때의 앞 대화 목록: `[0, 2, 4, …, 20, 20, …]` (길이 ok) |
| `set_pool(max_connections)` | `rc._client` 를 같은 접속 설정에 연결 풀 크기만 바꾼 새 `redis.Redis` 로 교체하고 이전 것을 돌려줌 |
| `print_result` | 시나리오 하나의 결과를 줄로 출력 |
| `judge_baseline` | 성공 N, 시간 초과/오류 0, 동시 최대 1, 앞 대화 `ideal_priors` 와 같음, 저장 대화 정상, 락 키 안 남음, 락 우회 0 |
| `judge_timeout` | 성공 + 시간 초과 = N(오류 0), 둘 다 1 이상, 동시 최대 1, 모델 호출 = 성공, 앞 대화/저장/락 키 정상 |


## 5. pool 시나리오

1. 기본 풀(redis-py 8.1.0 은 최대 100개)로 N=200 을 한 번 돌린다.
2. `set_pool(args.pool_size)` 로 풀을 300으로 늘린 클라이언트로 바꾸고 다시 돌린다.
3. `finally` 에서 늘린 클라이언트를 닫고 원래 클라이언트로 되돌린다.
4. 판정은 늘린 풀의 실행에만 `judge_baseline` 을 적용한다. 기본 풀 실행은 락이 깨지는 모습을 보는 용도라 판정하지 않는다. 기본 풀에서도 안 깨지면 "이 환경에서는 N 이 더 커야 한다"는 참고 문구만 출력한다.

락이 깨지는 길은 이렇다. 동시에 `SET NX` 를 시도하는 스레드가 풀 크기보다 많으면 연결을 못 얻은 스레드가 `MaxConnectionsError` 를 받는다. `run_agent` 는 `fail_open=True` 라 Redis 오류를 만나면 경고만 남기고 락 없이 진행한다. 대화 읽기와 저장도 같은 풀을 쓰므로 함께 실패할 수 있다(경고만 남고 요청은 성공). 이 흐름은 [DAY33_RESULT.md](DAY33_RESULT.md) 3-3 의 숫자로 확인했다.


## 6. 알아둘 점

- 이 스크립트는 한 프로세스 안의 스레드 경쟁이다. 워커 프로세스를 여러 개 띄운 경쟁이 아니다.
- 가짜 모델이라 `time.sleep` 으로 시간만 흉내 낸다. 실제 모델 응답 시간은 반영되지 않는다.
- 연결 풀 부족은 스레드 수가 풀 크기를 넘을 때 생긴다. baseline(N=100)은 풀 한도(100)와 같은 수라서, 환경에 따라 경계에 걸릴 수 있다.
- 락 코드(`redis_lock.py`), `fail_open` 동작, 연결 풀 설정은 이번에 바꾸지 않았다.
