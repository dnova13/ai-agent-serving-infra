# Day 34.1 가이드 — Kafka 파티션 수와 워커 수에 따른 처리량(TPS) 실험

> 포트: **8080 = Spring WAS**, **8000~8005 = FastAPI 워커(워커 수만큼)**, 9092 = Kafka(호스트), 6379 = Redis.
> 앞선 결과: [DAY33_RESULT.md](DAY33_RESULT.md). 파티션과 워커의 관계는 [DAY18_RESULT.md](DAY18_RESULT.md)(워커 1~4대, 파티션 3개), 워커 한 대가 한 건씩 처리하는 구조는 [DAY25_GUIDE.md](DAY25_GUIDE.md) 의 한계 절.

**전체 목적:** 같은 요청 60건을 파티션 수(P)와 워커 수(W)를 바꿔 가며 처리하고, 전부 끝날 때까지 걸린 시간으로 처리량(TPS)을 비교한다. "워커를 늘리면 빨라지는가, 파티션도 같이 늘려야 하는가"를 숫자로 본다.

**구성:** Spring → Kafka → FastAPI 워커 → Redis 경로는 실제로 구동하고, AI 모델은 실제 모델이 있다고 가정해 고정 2초를 기다렸다 답하는 가짜 모델로 대신한다. 가짜 모델은 검색 도구를 부르지 않아 Qdrant 와 Gemini 임베딩은 쓰지 않는다.

## 1. 오늘 목표

| 확인할 것                                                          | 실험                 |
| ------------------------------------------------------------------ | -------------------- |
| 워커 1대의 처리량(기준값)                                          | ① P1 W1              |
| 파티션이 1개면 워커를 늘려도 빨라지지 않는다                       | ② P1 W3              |
| 파티션과 워커가 같으면 워커 수만큼 빨라진다                        | ③ P3 W3              |
| 워커보다 파티션이 많아도(워커당 2개) 더 빨라지지 않는다            | ④ P6 W3              |
| 파티션과 워커를 같이 늘리면 더 빨라진다                            | ⑤ P6 W6              |
| 건이 파티션에 고르게 안 나뉘면 가장 바쁜 워커가 전체 시간을 정한다 | ③④⑤ 의 파티션별 건수 |

## 2. 개념

### 2-1. 파티션 수와 워커 수가 처리량을 정한다

- 한 파티션은 같은 그룹 안에서 **한 워커만** 읽는다. 워커 한 대는 **한 번에 한 건**을 처리한다(Day 25). 그래서 동시에 일할 수 있는 워커는 최대 `min(P, W)` 대다.
- 모델 대기 시간이 d 초면 워커 1대의 처리량은 약 1/d. 이 실험은 d = 2초(가짜 모델) → 1대 약 0.5건/초.
- Spring 은 메시지 키로 `requestId`(무작위)를 쓴다. 키의 해시로 파티션이 정해지므로 60건이 파티션에 **정확히 같은 수로 나뉘지는 않는다**. 가장 많이 받은 파티션의 워커가 마지막에 끝난다.

### 2-2. 확인 전 예상 (d=2초, N=60)

| 번호 | P   | W   | 일하는 워커           | 고르게 나뉠 때 시간 | 예상 TPS | 예상 이유                                                           |
| ---- | --- | --- | --------------------- | ------------------- | -------- | ------------------------------------------------------------------- |
| ①    | 1   | 1   | 1                     | 120초               | 0.5      | 기준                                                                |
| ②    | 1   | 3   | 1 (나머지 2대는 놂)   | 120초               | 0.5      | 파티션 1개는 한 워커만 읽음                                         |
| ③    | 3   | 3   | 3                     | 40초                | 1.2~1.5  | 이상은 1.5. 고르지 않아 건수가 20±5 로 갈리므로 마지막 워커가 늦음  |
| ④    | 6   | 3   | 3 (워커당 파티션 2개) | 40초                | 1.2~1.5  | ③ 과 같은 3대가 일함. 파티션 수만 늘어서는 빨라지지 않음            |
| ⑤    | 6   | 6   | 6                     | 20초                | 2.0~2.5  | 이상은 3.0. 파티션당 건수 편차가 상대적으로 커서 이상보다 더 벌어짐 |

(예상의 범위는 "60건을 무작위로 나눌 때 가장 많이 받는 쪽이 평균보다 3~6건 더 받는다"는 계산이다. 실제 해시 결과는 실행마다 다르다.)

### 2-3. 실험 방법

- 파티션 수는 줄일 수 없으므로(Day 18) 실험마다 **새 토픽**을 쓴다: `agent.bench.p1`, `agent.bench.p3`, `agent.bench.p6`. Spring 이 시작할 때 토픽을 파티션 수에 맞게 만든다(`KafkaTopicConfig`).
- 워커는 실험마다 **새 그룹**(`bench-<이름>`)으로 띄운다. 새 그룹은 토픽 끝부터 읽으므로(`OFFSET_RESET=latest`) 앞선 실험의 요청이 섞이지 않고 `agent-workers` 그룹의 LAG 도 건드리지 않는다.
- 요청은 `scripts` 가 아니라 `d2-ai-python/app/experiments/partition_bench.py` 가 보낸다. 60건을 한꺼번에 Spring(8080)으로 보내고, 결과(Redis)가 모두 DONE 이 될 때까지 기다려 전체 시간, TPS, 워커별·파티션별 건수를 출력한다.
- 워커는 `scripts/bench-workers.sh` 가 한 번에 W대 띄운다(가짜 모델 대기 2초, 흔들림 없음).

## 3. 바뀐 것

| 파일                                                     | 내용                                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------------- |
| `d2-ai-python/app/experiments/partition_bench.py` (새 파일) | 요청 N건 전송 → 완료 대기 → TPS, 워커별·파티션별 건수, 이론 하한 시간 출력 |
| `scripts/bench-workers.sh` (새 파일)                     | 워커 W대 시작/상태/종료                                                    |
| 기존 코드                                                | 바꾸지 않음                                                                |

## 4. 준비

1. 실행 중인 워커(8000, 8001 등)와 Spring(8080)을 모두 끈다.
   Kafka, Redis, Qdrant 컨테이너는 켜둔다(Qdrant는 이 시험에서 쓰지 않는다. 켜져 있어도 상관없다.).
2. 파이썬 가상환경을 켠 터미널에서 프로젝트 루트로 간다.
3. Redis 확인: `docker exec redis1 redis-cli ping` → `PONG`.
4. 실행 순서는 **P 가 같은 것끼리 묶는다**(Spring 재시작을 줄이려고): P1(①②) → P3(③) → P6(④⑤).

## 5. 실험

각 실험은 **같은 5단계**다. 표의 값만 바뀐다.

1. Spring 을 해당 P 의 토픽으로 띄운다(P 가 바뀔 때만). `d3-was-spring` 에서:

    ```
    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p<P> --app.kafka.topics.requests-partitions=<P>'
    ```

    `<P>` 는 자리표시자(파티션 수)다. 아래 중 해당하는 한 줄을 그대로 복사해 실행한다.

    ```
    # P=1 (①②)
    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p1 --app.kafka.topics.requests-partitions=1'

    # P=3 (③)
    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3'

    # P=6 (④⑤)
    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p6 --app.kafka.topics.requests-partitions=6'
    ```

    해당 실험의 한 줄만 실행한다. 로그에 기동이 끝나면 다음 단계로.

    **IntelliJ 에서 실행할 때:** 터미널 명령 대신 실행 설정에 같은 값을 넣는다.
    1. 우측 상단 실행 설정 드롭다운 → **Edit Configurations...** → Spring Boot 설정(WAS 애플리케이션)을 고른다.
    2. **Program arguments** 칸에 아래를 넣는다. 칸이 안 보이면 **Modify options → Program arguments** 를 켠다.
        ```
        --app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3
        ```
        (P 에 맞게 `p1`/`1`, `p3`/`3`, `p6`/`6` 으로 바꾼다.)
    3. Apply 후 Run. P 가 바뀔 때마다 이 칸의 값을 바꾸고 다시 실행한다.
    4. **실험이 끝나면 이 칸을 비우고 저장한다.** 비우지 않으면 평소 실행도 실험 토픽으로 뜬다.
    - 터미널과 IntelliJ 로 동시에 띄우면 8080 포트가 겹치므로 한쪽만 켠다.
    - 기동 뒤 `docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic agent.bench.p3` (P 에 맞게 p1, p3, p6) 로 `PartitionCount` 가 P 와 같은지 확인한다. 이 인자 방식이 먹히는지는 작성 시점에 실행해 보지 못했다.

2. 워커를 띄운다(프로젝트 루트):
    ```
    bash scripts/bench-workers.sh start <W> agent.bench.p<P> bench-<이름>
    ```
    `<W>`, `<P>`, `<이름>` 은 자리표시자다. 아래 표의 "워커 시작" 열에 실험별로 완성된 명령이 있으니 그대로 복사한다(예: ③ 은 `bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-p3w3`).
    ```
    bash scripts/bench-workers.sh start 1 agent.bench.p1 bench-p1w1   # ①
    bash scripts/bench-workers.sh start 3 agent.bench.p1 bench-p1w3   # ②
    bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-p3w3   # ③
    bash scripts/bench-workers.sh start 3 agent.bench.p6 bench-p6w3   # ④
    bash scripts/bench-workers.sh start 6 agent.bench.p6 bench-p6w6   # ⑤
    ```
    해당 실험의 한 줄만 실행한다(앞의 숫자 = 워커 수, 두 번째 = 토픽, 세 번째 = 그룹 이름).
3. 10~20초 기다린 뒤 `bash scripts/bench-workers.sh status`. 워커마다 `partitions` 가 배정돼 있고, 파티션 합계가 P 와 같아야 한다(워커가 파티션보다 많으면 `[]` 인 워커가 생긴다).
4. 측정:
    ```
    python d2-ai-python/app/experiments/partition_bench.py --label p1w1 --workers localhost:8000 --n 60   # ①
    python d2-ai-python/app/experiments/partition_bench.py --label p1w3 --workers localhost:8000,localhost:8001,localhost:8002 --n 60   # ②
    python d2-ai-python/app/experiments/partition_bench.py --label p3w3 --workers localhost:8000,localhost:8001,localhost:8002 --n 60   # ③
    python d2-ai-python/app/experiments/partition_bench.py --label p6w3 --workers localhost:8000,localhost:8001,localhost:8002 --n 60   # ④
    python d2-ai-python/app/experiments/partition_bench.py --label p6w6 --workers localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005 --n 60   # ⑤
    ```
    해당 실험의 한 줄만 실행한다. `--workers` 에는 띄운 워커의 주소를 모두 쓴다.
5. 끝나면
    ```
    bash scripts/bench-workers.sh stop
    ```
    으로 워커를 끈다. (P 가 같은 다음 실험은 1번을 건너뛰고 2번부터.)

| 번호 | 이름 | P   | W   | Spring 재시작         | 워커 시작                           | 측정 `--workers`                               |
| ---- | ---- | --- | --- | --------------------- | ----------------------------------- | ---------------------------------------------- |
| ①    | p1w1 | 1   | 1   | 필요 (`p1`, 파티션 1) | `start 1 agent.bench.p1 bench-p1w1` | `localhost:8000`                               |
| ②    | p1w3 | 1   | 3   | 불필요                | `start 3 agent.bench.p1 bench-p1w3` | `localhost:8000,localhost:8001,localhost:8002` |
| ③    | p3w3 | 3   | 3   | 필요 (`p3`, 파티션 3) | `start 3 agent.bench.p3 bench-p3w3` | 위와 같음                                      |
| ④    | p6w3 | 6   | 3   | 필요 (`p6`, 파티션 6) | `start 3 agent.bench.p6 bench-p6w3` | 위와 같음                                      |
| ⑤    | p6w6 | 6   | 6   | 불필요                | `start 6 agent.bench.p6 bench-p6w6` | `localhost:8000` ~ `8005` 6개                  |

①은 120초쯤, ②도 120초쯤 걸린다(파티션 1개라 한 워커가 60건을 처리). 나머지는 20~50초다.

**기대(확인 전 예상):** 2-2 의 표. 출력 마지막의 `SUMMARY` 줄(`P=.. W=.. wall=.. tps=..`)을 5개 모아 비교한다. 각 실행의 마지막 줄은 `== 결과: PASS`(측정이 끝까지 정상으로 끝났다는 뜻이며 TPS 가 기대대로인지는 판정하지 않는다).

**함께 볼 것:** 출력의 "파티션별 건수"(고르게 나뉘었는지), "놀고 있던 워커"(②에서 2대, ④에서 0대), "이론 하한 시간"(가장 많이 맡은 워커의 건수 × 건당 시간)과 실제 전체 시간의 비율.

**실패했을 때:**

- `FAIL: 배정된 파티션이 없다` → 워커가 그룹에 들어가는 중이다. 몇 초 뒤 다시 실행한다.
- `FAIL: 같은 파티션이 두 워커에 배정돼 있다` → 리밸런싱 중이거나 이전 워커가 남아 있다. `bench-workers.sh stop` 후 `lsof -iTCP:8000-8005 -sTCP:LISTEN` 로 남은 프로세스를 확인한다.
- `FAIL: 이미 처리 중인 워커가 있다` → 이전 실험의 요청이 남아 있다. 워커를 껐다 다시 새 그룹으로 띄운다.
- 접수 거절이 있으면 Spring 로그를 본다(Kafka 연결, 토픽 이름).

## 6. 정리

- 워커를 모두 끄고(`bench-workers.sh stop`), Spring 을 인자 없이 다시 띄우면 원래 토픽(`agent.requests`, 파티션 3)으로 돌아간다.
- 실험 토픽은 남아 있다. 지우려면 Kafka 컨테이너에서 `kafka-topics --bootstrap-server kafka:29092 --delete --topic agent.bench.p1` (p3, p6 도 같게). 지우기 전에 결과를 다 기록한다.
- 평소 워커(`agent-workers` 그룹, `agent.requests`)의 LAG 는 이번 실험이 건드리지 않았지만, 다음 실험 전에 0 인지 한 번 확인한다.

## 7. 한계 (미리 알아둘 것)

- 모델은 `provider=fake`(2초 대기)다. 실제 모델은 호출마다 시간이 다르고 외부 한도(429)가 있어 워커를 늘려도 모델 쪽에서 막힐 수 있다.
- 워커와 Kafka, Spring, Redis 가 한 PC 에서 돈다. 가짜 모델은 대기만 하므로 CPU 경쟁은 작지만 6대를 띄우면 PC 자원에 영향이 있을 수 있다.
- 각 조합을 **한 번씩** 잰다. 해시 때문에 파티션별 건수는 실행마다 다르므로 같은 조합을 다시 재면 TPS 가 달라질 수 있다.
- N=60 은 파티션 6개에는 작은 표본이라 건수 편차가 크다.
- Kafka 브로커는 1대다. 파티션이 브로커에 분산되는 효과는 보지 못한다.
- 워커 한 대가 한 건씩 처리하는 구조 자체는 바꾸지 않는다(워커 안에서 동시에 여러 건을 처리하는 방법은 다루지 않는다).
- Grafana 대시보드 확인은 이번에 하지 않는다. 워커 로그와 `/health` 만 본다.

## 8. 체크리스트

- [x] ① P1 W1: TPS 가 0.5 근처(기준값)
- [x] ② P1 W3: ① 과 비슷, 놀고 있는 워커 2대
- [x] ③ P3 W3: ① 의 약 2.5~3배
- [x] ④ P6 W3: ③ 과 비슷
- [x] ⑤ P6 W6: ③ 보다 빠름
- [x] DAY34_1_RESULT.md 작성

## 9. 실행 중 만난 문제 (트러블슈팅)

실험 ① 을 시작하다 만난 것들이다. 결과 문서(`DAY34_1_RESULT.md`)를 쓸 때 "실행하며 발견해 고친 것"으로 옮긴다.

| 번호 | 증상                                                                                                                              | 원인                                                                                                                                                                                                                                                    | 조치                                                                                                                                                                                                                                    |
| ---- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `bench-workers.sh start` 직후 측정하자 `FAIL: 워커 localhost:8000 의 /health 를 읽지 못했다 (http=0)`                             | 가상환경이 안 켜진 터미널에서 시작했다. pyenv 기본 파이썬이 쓰여 워커가 `ModuleNotFoundError: No module named 'prometheus_fastapi_instrumentator'` 로 시작하자마자 죽었다(`scripts/k6/out/day34-worker-1.log` 에서 확인). 프롬프트에 `(venv)` 가 없었다 | `source d2-ai-python/venv/bin/activate` 로 가상환경을 켠 터미널에서 다시 시작. 가이드 4절에 활성화 안내를 넣어야 한다. 시작 스크립트는 시작 4초 뒤 워커가 살아 있는지 확인해 죽었으면 `FAIL` 과 로그, 가상환경 안내를 출력하도록 고쳤다 |
| 2    | 가상환경을 켜고 다시 시작하자 `이미 떠 있는 워커가 있다. 먼저 stop`                                                               | 1번에서 죽은 워커의 프로세스 번호가 `/tmp/day34-workers.pids` 에 남아 있었다. 스크립트는 번호가 있으면 무조건 막았다                                                                                                                                    | `bash scripts/bench-workers.sh stop` 으로 비운 뒤 시작. 스크립트는 죽은 번호는 자동으로 정리하고 살아 있는 워커가 있을 때만 막도록 고쳤다                                                                                               |
| 3    | 워커가 정상으로 떴는데(로그에 `파티션 배정 [0]`, `Kafka 접속 완료`) 측정이 `토픽 None, 그룹 None`, `배정된 파티션이 없다` 로 실패 | `partition_bench.py` 의 버그. 워커의 `/health` 는 상태를 `kafka_consumer` 항목 아래에 담아 주는데 스크립트는 바깥 단계에서 읽었다. 가짜 서버로 시험할 때 응답을 평평하게 만들어 이 차이를 놓쳤다                                                        | `read_workers` 가 `kafka_consumer` 안을 읽도록 수정. 워커를 껐으면(`kafka_consumer` 가 null) 따로 `FAIL` 을 낸다                                                                                                                        |

**교훈:** 워커를 시작한 직후 바로 측정하지 말고 `bench-workers.sh status` 로 `partitions` 가 배정된 것을 먼저 본다. 측정 스크립트를 가짜 서버로 시험할 때는 실제 응답 모양(`/health` 의 중첩)과 같게 만들어야 한다.
