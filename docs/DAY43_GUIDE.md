# Day 43 가이드 — 코드 리팩토링, 주석 정리 등 코드 정리 뒤 동작 확인 (확인 스크립트와 Spring 테스트)

**전체 목적:** Day 43 에서 코드의 폴더 구조와 주석, 로그 출력 방식을 바꿨다(바꾼 내용은 [DAY43_REPORT.md](DAY43_REPORT.md)). 동작은 바꾸지 않았으므로, 정리한 뒤에도 Day 35 의 통합 재검증과 Spring 테스트가 예전과 같이 통과하는지 확인한다.

**이 가이드로 알 수 있는 것:**

| 알고 싶은 것                                                                                 | 어디서 |
| -------------------------------------------------------------------------------------------- | ------ |
| 확인 스크립트(`app/checks/`)를 옮긴 뒤에도 Redis 만으로 도는 점검 7개가 통과하는가           | 1      |
| 서버 코드(`app/services/`)가 옮긴 뒤에도 켜지고, Spring → Kafka → 워커 → Redis 가 이어지는가 | 2      |
| Spring 테스트가 주석만 바뀐 뒤에도 통과하는가                                                | 3      |

## 1. 바뀐 것 (실행할 때 알아야 하는 것만)

| 바뀐 것   | 내용                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 폴더      | `app/services/` 의 확인 스크립트는 `app/checks/` 로, 실험 스크립트는 `app/experiments/` 로 옮겼다                                                             |
| 실행 명령 | `python -m app.services.system_verify` 는 이제 `python -m app.checks.system_verify` 다. 실험은 `python -m app.experiments.<이름>` 이다                        |
| import    | 코드 안의 `from services import ...` 는 모두 `from app.services import ...` 로 바뀌었다. 이제 모든 것을 `d2-ai-python` 폴더에서 실행한다(`dlq_check` 도 같다) |
| 서버 실행 | 서버도 `d2-ai-python` 폴더에서 `uvicorn app.main:app --port 8000` 으로 켠다. 예전의 `app` 폴더로 이동해 `uvicorn main:app` 하는 방식은 더 쓰지 않는다         |
| 로그      | `agent_graph.py` 의 `[timing]` 줄은 지금처럼 나온다. `>>> [DEBUG]` 줄은 기본으로 나오지 않고, 워커를 `AGENT_LOG_LEVEL=DEBUG` 로 띄우면 나온다                 |
| 옮긴 파일 | `claude/`, `d1-ai-agent/`, `agent_core.py` 등은 `archive/` 에 있다                                                                                            |

## 2. 준비

- 파이썬 가상환경(`venv`)이 있어야 한다. 아래 파이썬 명령은 모두 가상환경을 켠 터미널에서 실행한다.
- Docker 가 켜져 있어야 한다.
- 프로젝트 루트는 `ai-agent-serving-infra` 폴더다. 아래 명령의 `d2-ai-python`, `d3-was-spring` 은 그 아래 폴더다.

## 3. 실험

### ① 1단계: Redis 만 있으면 되는 점검 7개 (`--only offline`)

**목적:** 옮긴 확인 스크립트가 `python -m app.checks.<이름>` 으로 실행되고 통과하는지 본다. 서버는 켜지 않는다.

```
# 프로젝트 루트에서
docker compose up -d redis
```

Redis 컨테이너가 이미 떠 있으면 건너뛴다.

```
cd d2-ai-python
python -m app.checks.system_verify --only offline
```

몇 분 걸린다(`lock_stress` 가 가장 오래 걸린다).

**확인 전 예상:**

| 항목                                                                                                                               | 예상                                                                 |
| ---------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `redis_check`, `result_check`, `ttl_check`, `lock_check`, `selfcorrect_check`, `dlq_check`, `lock_stress`(baseline, timeout, N=50) | 7개 모두 PASS                                                        |
| `lock_check`                                                                                                                       | langchain 등이 없으면 일부 항목을 건너뛰지만 종료 코드는 0 이라 PASS |
| 마지막 줄                                                                                                                          | `== 결과: PASS`                                                      |

**실패했을 때:**

- `ModuleNotFoundError` 가 나오면 옮긴 파일의 import 경로가 맞지 않는 것이다. 오류 줄을 그대로 기록한다.
- 항목 하나가 FAIL 이면 그 스크립트만 따로 실행해 어느 항목인지 본다. 예: `python -m app.checks.ttl_check`.

### ② 2단계: 서버를 켜고 end-to-end 확인 (`--only live`)

**목적:** 옮긴 서버 코드(`app/services/`, `app/routers/`, `app/main.py`)로 워커와 Spring 이 평소처럼 켜지고, 요청이 Spring → Kafka → 워커 → Redis 로 끝까지 가는지 본다. 가짜 모델을 쓰므로 비용이 들지 않는다.

**준비 (Day 34 에서 파티션 시험을 하고 왔다면 평소 설정으로 되돌린다):**

1. 실험 워커를 끈다: `bash scripts/bench-workers.sh stop` (끝에 `없음` 이 나와야 한다). Spring 도 실험용 인자로 띄웠다면 끈다.
2. 컨테이너를 켠다(프로젝트 루트).
    ```
    docker compose up -d
    docker compose up -d prometheus grafana kafka-exporter redis-exporter
    ```
3. `agent-workers` 그룹의 lag 가 0 인지 본다.
    ```
    docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --describe
    ```
    0 이 아니면 워커를 모두 끄고 offset 을 최신으로 리셋한다.

**실행:** 터미널 3개가 필요하다.

터미널 A, Spring 을 **인자 없이** 켠다.

```
cd d3-was-spring
./gradlew bootRun
```

로그에 기동 완료가 보일 때까지 기다린다.

터미널 B, 워커 1대를 평소 설정으로 가짜 모델을 켜서 띄운다(가상환경을 켠 터미널).

```
export KAFKA_WORKER_ID=w1
export FAKE_LLM_ENABLED=true
export FAKE_LLM_DELAY_SEC=1
uvicorn app.main:app --reload --port 8000
```

로그에 `파티션 배정 [0, 1, 2]` 와 `Kafka 접속 완료, 수신 대기` 가 보이면 된다(토픽 `agent.requests`, 파티션 3개).

터미널 C, 확인한다(가상환경을 켠 터미널).

```
cd d2-ai-python
python -m app.checks.system_verify --only live
```

**확인 전 예상:**

| 항목                                               | 예상                                     |
| -------------------------------------------------- | ---------------------------------------- |
| 워커 `/health`, 워커 `/metrics`                    | PASS                                     |
| Spring 상태와 지표                                 | PASS                                     |
| end-to-end 가짜 모델 요청(접수 202 부터 DONE 까지) | PASS                                     |
| 같은 세션 대화 이어가기, 세션 삭제 404             | PASS                                     |
| 입력 검증(빈 question 은 400)                      | PASS                                     |
| Prometheus 대상(spring, kafka, redis), Grafana     | 컨테이너가 켜져 있으면 PASS, 아니면 SKIP |
| 컨슈머 그룹 lag                                    | PASS(합계 0)                             |
| 마지막 줄                                          | `== 결과: PASS`                          |

**실패했을 때:**

- `워커 localhost:8000 /health http=0. 워커가 안 떠 있다` 가 나오면 코드 문제가 아니다. 터미널 B 의 워커가 떠 있는지 본다. 이때 나머지 항목은 SKIP 으로 건너뛴다.
- 워커를 시작할 때 `ModuleNotFoundError` 가 나오면 옮긴 코드의 import 가 맞지 않는 것이다. 오류 줄을 그대로 기록한다.
- 워커 `/health` 의 `topic` 이 `agent.requests` 가 아니면 Day 34 실험 설정이 남아 있는 것이다(준비 1을 다시 한다).
- 접수는 되는데 DONE 이 안 되면 워커 로그와 `FAKE_LLM_ENABLED=true` 를 확인한다.
- Prometheus 대상에서 `fastapi@host.docker.internal:8001` 이 down 인 것은 정상이다(8001 워커를 안 켠 것).

**로그를 바꾼 것 확인(선택):** 워커 터미널에서 요청 하나를 보낸 뒤 `[timing] LLM 호출(agent_node) …초` 줄이 예전처럼 찍히는지 본다. `>>> [DEBUG]` 줄은 보이지 않는 것이 정상이다. 보려면 워커를 끄고 `export AGENT_LOG_LEVEL=DEBUG` 를 더한 뒤 다시 띄운다.

### ③ Spring 테스트

**목적:** Java 파일의 주석만 바뀐 뒤에도 테스트가 통과하는지 본다.

```
cd d3-was-spring
./gradlew test
```

서버는 켜지 않아도 된다(②의 Spring 은 꺼도 된다).

**확인 전 예상:**

| 테스트                                                   | 예상                                  |
| -------------------------------------------------------- | ------------------------------------- |
| `AgentRequestMessageTest`(메시지 JSON 모양)              | PASS                                  |
| `WasApplicationTests.contextLoads`(Spring 컨텍스트 기동) | PASS. Kafka 설정에 따라 시간이 걸린다 |
| 마지막 줄                                                | `BUILD SUCCESSFUL`                    |

**실패했을 때:** `contextLoads` 만 실패하면서 Kafka 접속 오류가 보이면 환경 문제일 수 있다(Kafka 컨테이너가 꺼져 있는지 본다). 메시지 JSON 테스트가 실패하면 기록한다.

### ④ 결과 기록

1. 실패한 항목이 있으면 항목, 증상, 원인, 고친 곳을 `DAY43_REPORT.md` 3절에 적는다.
2. 고친 뒤 같은 항목을 다시 실행해 PASS 를 확인한다.
3. 모두 PASS 면 `DAY43_REPORT.md` 3절의 "확인하지 않은 것"을 실행 결과로 바꾼다.

## 4. 체크리스트

- [ ] ① 1단계: 7개 PASS
- [ ] ② 평소 설정으로 되돌림(워커, Spring, lag 0)
- [ ] ② 2단계: 워커, Spring, end-to-end, 세션, 입력 검증 PASS
- [ ] ② `[timing]` 줄이 예전처럼 나옴
- [ ] ③ `./gradlew test` BUILD SUCCESSFUL
- [ ] `DAY43_REPORT.md` 3절 갱신
