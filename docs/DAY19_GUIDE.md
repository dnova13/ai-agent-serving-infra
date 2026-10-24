# Day 19 가이드 — Redis 기본 연결과 읽기/쓰기 함수

> 상태: 코드 작성 완료, 내 환경(docker의 redis1)에서의 실행 확인(①~④)은 하지 않음(2026-10-13). 샌드박스에서 확인한 내용은 [DAY19_RESULT.md](DAY19_RESULT.md) 에 적었다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, **6379 = Redis(redis1 컨테이너)**, 9092 = Kafka, 6333 = Qdrant.


## 1. 오늘 목표

4주차는 Redis 로 상태를 다룬다. 지금까지 요청 결과(Day 17)는 FastAPI 프로세스 메모리에 있어서 워커를 껐다 켜면 사라지고, 워커가 여럿이면 처리한 워커에만 있다(Day 18). 이 문제를 풀 공유 저장소가 Redis 다. 오늘은 Day 20(에이전트 상태 저장), Day 21(분산 락) 전에 연결과 기본 동작만 따로 확인한다.

| 확인할 것                                                   | 실험 |
| ----------------------------------------------------------- | ---- |
| Redis 컨테이너가 떠 있고 접속되는가                         | ①    |
| 파이썬에서 저장, 조회, 덮어쓰기, 삭제, 만료 시간이 되는가   | ②    |
| 한글과 중첩 구조(dict, list)가 그대로 돌아오는가            | ②    |
| 서버 `/health` 에서 Redis 연결 상태가 보이는가              | ③    |
| Redis 가 꺼져 있어도 서버는 살아 있는가                     | ④    |
| (선택) `redis-cli` 로 직접 들여다보면 저장된 모양이 어떤가  | ⑤    |


## 2. 개념

| 용어         | 설명                                                                                                                                |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Redis        | 메모리에 키-값을 저장하는 서버. 프로세스 밖에 있어서 FastAPI 워커가 여러 개여도 같은 데이터를 본다(Day 17의 "워커마다 따로" 문제를 푼다) |
| 키(key)      | 값을 찾는 이름. 이 프로젝트는 항상 `agent:` 접두어를 붙인다(예: `agent:state:abc`)                                                  |
| TTL          | 키가 살아 있을 시간(초). 지나면 Redis 가 스스로 지운다. 설정하지 않으면 영구(`-1`)                                                  |
| 직렬화       | 파이썬 dict 를 문자열(JSON)로 바꿔 저장하고 꺼낼 때 되돌리는 것. Redis 의 문자열 값에는 JSON 문자열을 넣는다                        |

**Day 17의 메모리 저장과 비교.** 메모리(`OrderedDict`)는 프로세스가 죽으면 사라지고 워커마다 따로이지만, Redis 는 프로세스와 별개로 살아 있고 모든 워커가 같은 곳을 본다. 대신 네트워크 호출이라 메모리보다 느리고, Redis 가 꺼지면 접근할 수 없다(④ 에서 서버가 어떻게 되는지 본다).

```
[FastAPI 워커 w1 :8000] ─┐
[FastAPI 워커 w2 :8001] ─┼─ redis_client(저장, 조회, 삭제, TTL) ──> [Redis :6379]
[FastAPI 워커 w3 :8002] ─┘
```


## 3. 만든/바꾼 파일

| 파일                                                    | 내용                                                                                                                                                       |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `d2-ai-python/app/services/redis_client.py` (신규)      | 연결(`get_client`), 키 이름(`make_key`), 상태 확인(`ping`), 저장(`set_json`, TTL 선택), 조회(`get_json`), 삭제(`delete`), `exists`, 남은 시간(`ttl_sec`)     |
| `d2-ai-python/app/checks/redis_check.py` (신규)       | 위 함수를 차례로 시험하고 PASS/FAIL 을 출력하는 확인 스크립트. 테스트 키는 `agent:day19:check:...` 아래에만 만들고 끝나면 지운다                            |
| `d2-ai-python/app/routers/health.py` (수정)             | `/health` 응답에 `redis` 항목 추가(`connected`, `latencyMs` 또는 `error`). Redis 가 꺼져 있어도 200 이고 `status` 는 `ok`                                   |
| `d2-ai-python/requirements.txt`, `app/requirements.txt` | `redis==8.1.0` 추가                                                                                                                                        |

| 환경변수                     | 기본값      | 뜻                                          |
| ---------------------------- | ----------- | ------------------------------------------- |
| `REDIS_HOST`                 | `localhost` | Redis 주소 (`.env.example` 에 이미 있음)    |
| `REDIS_PORT`                 | `6379`      | Redis 포트                                  |
| `REDIS_DB`                   | `0`         | Redis DB 번호                               |
| `REDIS_CONNECT_TIMEOUT_SEC`  | `1`         | 접속 시도 제한 시간(꺼져 있을 때 `/health` 가 오래 멈추지 않게) |
| `REDIS_SOCKET_TIMEOUT_SEC`   | `2`         | 명령 응답 제한 시간                         |

**설계 메모.**

- 동기(sync) 클라이언트를 썼다. 에이전트가 `asyncio.to_thread` 로 도는 스레드에서도 그대로 쓸 수 있어서다. async 함수에서 직접 부르면 그동안 이벤트 루프가 멈추므로, `/health` 는 `asyncio.to_thread` 로 감싸 불렀다.
- `decode_responses=True` 로 문자열로 받고, 값은 JSON 으로 저장한다. `ensure_ascii=False` 라서 한글이 `\uXXXX` 가 아니라 그대로 저장된다(⑤ 에서 `redis-cli` 로 볼 때 읽기 쉽다).
- 키가 없거나 만료되면 `get_json` 은 `None`, `ttl_sec` 도 `None` 이다. 영구 키는 `-1` 이다(Redis 의 `-2`=없음 만 `None` 으로 바꿨다).


## 4. 준비

1. Redis 컨테이너가 떠 있는지 본다: `docker ps` 에서 `redis1` 이 보여야 한다. 없으면 프로젝트 루트에서 `docker compose up -d redis`.
2. 가상환경을 켠 상태로 `redis` 패키지를 설치한다. `d2-ai-python` 에서: `pip install -r requirements.txt` (또는 `pip install redis==8.1.0`).


## 5. 실행과 확인

### ① Redis 접속 확인

```bash
docker exec redis1 redis-cli ping
```

기대(확인 전 예상): `PONG`.

```bash
cd d2-ai-python
python -m app.services.redis_client
```

기대: `접속 localhost:6379 db=0` 다음 줄에 `{'connected': True, 'latencyMs': ...}`.

### ② 읽기/쓰기 함수 확인

```bash
cd d2-ai-python
python -m app.checks.redis_check
```

기대: 10개 항목이 `[PASS]` 이고 마지막이 `RESULT: PASS`. 만료 시간 확인 때문에 약 4초 걸린다. 한 항목이라도 `[FAIL]` 이면 그 줄의 괄호 안 값을 기록한다.

| 항목 | 보는 것                                                       |
| ---- | ------------------------------------------------------------- |
| 1    | 연결(ping)                                                    |
| 2    | 한글, 중첩 dict, list, bool, null 이 저장 후 조회에서 그대로 |
| 3    | 같은 키를 다시 쓰면 덮어씀                                    |
| 4    | 없는 키는 `None`, `exists` 가 키 유무에 맞게 True/False       |
| 5    | TTL 없이 저장하면 `ttl_sec` 이 `-1`                           |
| 6    | `ttl_sec=3` 이면 남은 시간이 0~3초, 4초 뒤에는 조회 `None`    |
| 7    | `delete` 는 있던 키 True, 이미 없는 키 False                  |

### ③ 서버 `/health` 에서 Redis 상태

서버(8000)를 켜고(`source run.sh` 또는 `uvicorn app.main:app --port 8000`):

```bash
curl -s localhost:8000/health
```

기대: 응답에 `"redis":{"connected":true,"latencyMs":...}` 가 들어 있다. 기존 `status`, `kafka_consumer` 항목은 그대로다.

### ④ Redis 를 끄면 서버는 어떻게 되나

```bash
docker stop redis1
curl -s -w "\n%{http_code} %{time_total}s\n" localhost:8000/health
docker start redis1
```

기대(확인 전 예상): 응답 코드 `200`, `status` 는 `ok`, `redis` 는 `{"connected":false,"error":"ConnectionError: ..."}`. 접속 제한 시간이 1초라서 응답이 1초쯤 걸릴 수 있다. `docker start redis1` 후 다시 부르면 `connected: true` 로 돌아오는지도 본다.

### ⑤ (선택) `redis-cli` 로 저장된 모양 보기

TTL 을 길게 잡은 키를 하나 만들어 직접 본다. `d2-ai-python` 에서:

```bash
python - <<'EOF'
from app.services import redis_client as rc
rc.set_json(rc.make_key("day19", "demo"), {"question": "환불은 며칠 안에 되나요?"}, ttl_sec=300)
EOF
docker exec redis1 redis-cli get agent:day19:demo
docker exec redis1 redis-cli ttl agent:day19:demo
docker exec redis1 redis-cli del agent:day19:demo
```

기대: `get` 에 `{"question": "환불은 며칠 안에 되나요?"}` 가 한글 그대로 나온다. `ttl` 은 300 이하의 숫자, `del` 은 `1`.


## 6. 확인한 것과 하지 않은 것

| 구분         | 내용                                                                                                                                                                                                                 |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 확인함       | 확인 스크립트 10개 항목이 가짜 Redis(fakeredis 2.39.0)와 실제 `redis-server` 7.0.15 에서 통과했다. Redis 가 꺼진 상태에서 `ping` 이 예외 없이 `connected: false` 를 돌려주고, `/health` 가 200 으로 `redis` 항목을 보여 주는 것도 확인했다. 단 **모두 내 코드를 점검하는 샌드박스 환경**에서이고 사용자 환경이 아니다 |
| 확인하지 않음 | 사용자 환경의 `redis1` 컨테이너(`redis:7-alpine`)에서의 실행 전부(①~⑤), 위 `docker exec` 명령, `redis==8.1.0` 이 사용자 파이썬(3.11.9)에서 설치되는지, 워커 여러 개가 같은 키를 읽는 동작(Day 20에서)                   |


## 7. 한계

| 항목             | 내용                                                                                                                                      |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 동기 클라이언트  | async 함수에서 직접 부르면 이벤트 루프를 막는다. 부를 곳이 async 면 `asyncio.to_thread` 로 감싸야 한다. 필요하면 Day 20 이후 `redis.asyncio` 로 바꾼다 |
| 영속성           | 기본 compose 설정의 Redis 는 `docker-data/redis` 에 덤프를 남기지만, 마지막 저장 이후 데이터는 컨테이너가 죽으면 사라질 수 있다(저장 주기 설정은 확인하지 않음) |
| 인증 없음        | 로컬 개발용이라 비밀번호 없이 쓴다                                                                                                        |
| 아직 쓰는 곳 없음 | 요청 결과(Day 17 메모리 저장)는 아직 그대로다. Redis 로 옮기는 것은 Day 20                                                                |


## 8. 체크리스트

- [x] 코드 작성 (가짜 Redis, 실제 redis-server 로 동작 확인)
- [ ] ① 접속 확인 (`PONG`, `python -m app.services.redis_client`)
- [ ] ② `python -m app.checks.redis_check` 10개 통과
- [ ] ③ `/health` 에 `redis` 항목
- [ ] ④ Redis 를 껐을 때 `/health` 동작
- [ ] (선택) ⑤ `redis-cli` 로 저장된 모양
- [ ] 결과를 `DAY19_RESULT.md` 에 기록
