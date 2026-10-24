# Day 34 코드 가이드 — 파티션 수와 워커 수에 따른 처리량(TPS) 측정 도구 (가짜 모델 → 실제 모델)

> 실행 방법과 확인 항목은 [DAY34_1_GUIDE.md](DAY34_1_GUIDE.md), 결과는 [DAY34_1_RESULT.md](DAY34_1_RESULT.md). 이 문서는 **새로 만든 두 파일이 무엇을 어떤 순서로 하는지**를 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000~8005 = FastAPI 워커**(워커 수만큼, 8000 부터), 9092 = Kafka(호스트), 6379 = Redis.

**Day 34 의 흐름:** 같은 측정 도구로 두 번 잰다. 먼저 가짜 모델로 구조만 보고, 다음에 실제 모델로 같은 결론이 나오는지 확인한다.

| 순서 | 문서                         | 모델            | 구동한 것                                | 보려는 것                                          |
| ---- | ---------------------------- | --------------- | ---------------------------------------- | -------------------------------------------------- |
| 1    | [Day 34.1](DAY34_1_GUIDE.md) | 가짜(고정 2초)  | Spring → Kafka → 워커 → Redis            | 처리량이 파티션 수와 워커 수 중 무엇으로 정해지는가 |
| 2    | [Day 34.2](DAY34_2_GUIDE.md) | 실제 Claude     | 위 경로 + Qdrant 검색 + Gemini 임베딩     | 같은 결론이 실제 모델에서도 나오는가               |

**목적:** 같은 요청 60건을 파티션 수(P)와 워커 수(W)를 바꿔 가며 처리하고 전부 끝날 때까지 걸린 시간으로 처리량(TPS)을 비교한다. P 와 W 는 사람이 바꾸고, 이 도구는 워커를 띄우는 일(`bench-workers.sh`)과 측정(`partition_bench.py`)을 맡는다.

**Day 34 의 코드 변경:** 새 파일 `scripts/bench-workers.sh`, `d2-ai-python/app/experiments/partition_bench.py` 두 개. 기존 코드(Spring, 워커, Kafka 설정)는 바꾸지 않았다. Spring 의 토픽 이름과 파티션 수는 실행 인자로, 워커의 토픽과 그룹은 환경변수로 바꾼다(기존 설정 키를 그대로 쓴다).


## 1. 전체 흐름

```
(터미널 A)  ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3'
              └ Spring 이 시작할 때 KafkaTopicConfig 가 이 토픽을 파티션 3개로 만든다

(터미널 B)  bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-p3w3     워커 3대 시작 (새 그룹)
            bash scripts/bench-workers.sh status                                파티션 배정 확인
            python .../partition_bench.py --label p3w3 --workers ...:8000,...    측정
              1) 각 워커 /health 로 토픽, 그룹, 맡은 파티션 확인
              2) 요청 60건을 Spring(8080)에 동시 전송
              3) 결과가 모두 DONE 이 될 때까지 폴링
              4) 전체 시간, TPS, 워커별·파티션별 건수 출력
            bash scripts/bench-workers.sh stop                                  워커 종료
```

경로는 Spring → Kafka(`agent.bench.p<P>`) → 워커(그룹 `bench-...`) → Redis 결과 → 측정 도구가 조회, 전 구간을 거친다.


## 2. `scripts/bench-workers.sh`

`start <워커 수> <토픽> <그룹> [모델 대기 초, 기본 2]` / `status` / `stop`.

### 2-1. start

1. **이전 상태 정리:** `/tmp/day34-workers.pids` 에 번호가 남아 있으면 살아 있는지 검사한다. 살아 있는 프로세스가 있으면 "먼저 stop" 하고 끝낸다. 죽은 번호만 있으면 비운다.
2. **워커 i 시작(1부터 W 까지):** 포트 `8000 + i - 1`. 포트가 이미 쓰이고 있으면 `stop` 을 부르고 끝낸다. `d2-ai-python` 에서 아래 환경변수로 `uvicorn app.main:app --port <포트>` 를 백그라운드(`nohup`)로 띄운다. 로그는 `scripts/k6/out/day34-worker-<i>.log`, 프로세스 번호는 pids 파일에 한 줄씩 기록한다.

   | 환경변수 | 값 | 뜻 |
   | -------- | -- | -- |
   | `KAFKA_WORKER_ID` | `w<i>` | 결과의 `worker` 에 찍힌다 |
   | `KAFKA_TOPIC` | 인자 | 읽을 토픽 |
   | `KAFKA_GROUP` | 인자 | 소비자 그룹. 실험마다 새 이름을 쓴다 |
   | `FAKE_LLM_ENABLED` | `true` | 가짜 모델 켬 |
   | `FAKE_LLM_DELAY_SEC` | 인자(기본 2) | 가짜 모델 대기 시간 |
   | `FAKE_LLM_JITTER` | `0` | 대기 시간 흔들림 없음 |

   `--reload` 는 붙이지 않는다(부하 중 파일 감시가 측정을 흔든다).
3. **생존 확인:** 4초 기다린 뒤 기록한 프로세스가 모두 살아 있는지 본다. 죽은 워커가 있으면 `FAIL` 과 로그 확인, 가상환경 안내를 출력하고 `stop` 후 종료한다(가상환경을 안 켠 터미널에서 `ModuleNotFoundError` 로 죽는 경우를 잡는다).

프로세스 번호는 워커 본체(uvicorn)의 것이어야 `stop` 이 끌 수 있다. 처음 버전은 `cd ... && 변수=값 nohup uvicorn ... &` 를 한 줄로 묶어 껍데기의 번호가 기록됐고, 그래서 `stop` 이 본체를 못 껐다(결과 문서 4절 4번). 지금은 `cd` 와 실행을 줄을 나눠 쓴다.

### 2-2. status

8000~8007 포트의 `/health` 를 읽어 포트, `worker_id`, `partitions`, `topic`, `group` 을 한 줄씩 출력한다. 응답이 없는 포트는 건너뛴다.

### 2-3. stop

1. pids 파일의 번호에 `kill`, 2초 뒤 `kill -9`, 파일 비움.
2. 기록으로 못 끈 워커(이전 버전이 남긴 것 등)는 8000~8007 포트를 쓰는 **python 프로세스**를 찾아 같은 방식으로 끈다. 명령어에 `python` 이 들어 있는 것만 끈다.
3. 남은 포트를 출력한다(없으면 `없음`).


## 3. `d2-ai-python/app/experiments/partition_bench.py`

표준 라이브러리만 쓴다. 옵션: `--label`(필수), `--workers`(쉼표로 구분한 주소), `--spring`, `--n`(기본 60), `--provider`(기본 `fake`), `--concurrency`(기본 20), `--poll-sec`(기본 0.5), `--timeout-sec`(기본 600).

### 3-1. 보조 함수

| 함수 | 하는 일 |
| ---- | ------- |
| `http_json(method, url, body, timeout)` | JSON 요청 하나. `(HTTP 코드, 본문)` 을 돌려준다. 연결 실패는 코드 0 |
| `read_workers(hosts)` | 워커마다 `/health` 를 읽어 **`kafka_consumer` 항목**(상태, 토픽, 그룹, 파티션, 처리 건수)을 꺼내 목록으로 돌려준다. 못 읽거나 `kafka_consumer` 가 null 이면 `FAIL` 로 끝낸다(처음 버전은 이 중첩을 놓쳐 `토픽 None` 이 나왔다. 결과 문서 4절 3번) |
| `percentile(sorted_vals, p)` | 정렬된 목록의 p 번째 백분위 값 |

### 3-2. `main` 의 단계

1. **워커 상태 확인:** `read_workers` 결과에서 토픽과 그룹이 모든 워커에서 같은지, 배정된 파티션이 겹치지 않는지, 파티션이 하나라도 배정됐는지, 이미 처리 중인 워커가 없는지 본다. 하나라도 어긋나면 이유를 출력하고 종료한다(측정하지 않는다). 시작 시점의 워커별 처리 건수(`received + failed`)를 기록해 둔다.
2. **전송:** `ThreadPoolExecutor`(동시 연결 `--concurrency`)로 `POST <spring>/api/agent/requests` 를 N번 보낸다. 질문은 5개를 돌려 쓴다. 응답이 202 이고 `requestId` 가 있으면 접수로 센다. 접수 시간과 응답 시간(p50, 최대)을 출력한다.
3. **완료 대기:** 접수된 `requestId` 마다 `GET <첫 번째 워커>/api/v1/agent/results/{id}` 를 `--poll-sec` 간격으로 반복한다. 결과는 Redis 에 있어 아무 워커에게 물어도 된다. 상태가 `DONE` 또는 `FAILED` 이면 완료로 기록하고 완료 시각을 남긴다. 404 나 처리 중이면 계속 기다린다. 5초마다 진행(`완료 n/N`)을 출력한다. `--timeout-sec` 가 지나면 남은 건수를 "못 기다림"으로 센다.
4. **집계:**

   | 값 | 계산 |
   | -- | ---- |
   | 전체 시간 | 첫 전송 시각부터 마지막 완료 시각까지 |
   | TPS | 성공(DONE) 건수 / 전체 시간 |
   | 건당 처리 시간 | 결과의 `elapsedMs`(평균, p95, 최대) |
   | 파티션별, 워커별 건수 | 결과의 `partition`, `worker` 를 센다 |
   | 놀고 있던 워커 | 처리 건수가 0 인 워커 |
   | 이론 하한 | (가장 많이 맡은 워커의 건수) x (건당 평균 처리 시간) |
   | 고르게 나뉘었다면 | ceil(성공 건수 / min(파티션 수, 워커 수)) x 평균 처리 시간 |
   | 하한 대비 | 전체 시간 / 이론 하한. 1 에 가까울수록 처리 외의 지연이 작다 |

5. **교차 확인:** 끝난 뒤 `/health` 를 다시 읽어 워커별 처리 건수 증가분이 결과의 `worker` 집계와 같은지 대조한다.
6. **출력과 판정:** `SUMMARY` 한 줄(탭으로 구분: 라벨, P, W, N, ok, fail, wall, tps, top_worker, idle)을 출력한다. 접수 거절, 실패, 못 기다림, 집계 불일치 중 하나라도 있으면 `FAIL`, 없으면 `PASS`(측정이 끝까지 정상으로 끝났다는 뜻이며 처리량이 기대대로인지는 판정하지 않는다).


## 4. 알아둘 점

- **P 는 이 도구가 바꾸지 않는다.** 파티션 수는 줄일 수 없으므로(Day 18) 실험마다 새 토픽(`agent.bench.p1`, `p3`, `p6`)을 쓴다. Spring 은 실행 인자(`--app.kafka.topics.requests`, `--app.kafka.topics.requests-partitions`)로 토픽과 파티션 수를 바꿔 띄운다.
- **그룹은 실험마다 새 이름을 쓴다.** 같은 이름을 다시 쓰면 이전 실험에서 읽은 위치부터 이어 읽어 다른 실험의 요청이 섞인다. 같은 조합을 다시 재려면 토픽과 그룹을 지우고 시작하거나 그룹 이름을 새로 붙인다.
- **새 그룹은 토픽 끝부터 읽는다**(`KAFKA_OFFSET_RESET` 기본 `latest`). 그래서 워커가 파티션을 받은 뒤에 요청을 보내야 한다. 측정 도구가 파티션이 배정되지 않았으면 시작하지 않는 이유다.
- **60건이 파티션에 나뉘는 모양은 실행마다 다르다.** Spring 이 메시지 키로 `requestId`(무작위)를 쓰고 파티션이 키의 해시로 정해지기 때문이다. 가장 많이 받은 워커가 전체 시간을 정한다.
- 가짜 모델(`provider=fake`)이라 비용이 들지 않는다. 실제 모델의 응답 시간과 호출 한도는 반영되지 않는다.
