# Day 34.2 가이드 — 실제 모델(Claude)로 동기·비동기 부하 테스트와, Kafka 파티션 수와 워커 수에 따른 처리량(TPS) 실험

**전체 목적:** Day 26·27·34.1 의 결론이 가짜 모델(고정 2~5초)에서만 확인됐다. 같은 시험을 실제 Claude 로 소량만 돌려 결론이 유지되는지, 달라지는 점이 있는지 본다.

**구성:** 실제 운영 구성에 가장 가깝게 Spring → Kafka → FastAPI 워커 → Redis 를 구동하고, AI 모델로 실제 Claude 를 쓴다. 에이전트가 검색 도구로 Qdrant 와 Gemini 임베딩을 쓴다.

## 1. 무엇을 재는가

| 번호 | 원래 Day                  | 실험                               | 질문 수(예상) | 확인할 것                                                    |
| ---- | ------------------------- | ---------------------------------- | ------------- | ------------------------------------------------------------ |
| ①    | [26](DAY26_RESULT.md)     | 동기, VU 20, 75초 (Spring 경유)    | 약 32건       | 실제 응답 시간에서도 504 가 나는가, 200 비율은 얼마인가      |
| ②    | [27](DAY27_RESULT.md)     | 비동기, VU 20, 70초, 결과 조회까지 | 약 32건       | 접수는 빠르고 완료까지의 시간만 늘어나는가, 실패 건이 있는가 |
| ③    | [34.1](DAY34_1_RESULT.md) | P3 W3, 60건                        | 60건          | 파티션을 3개로 줄여도 워커 3대 처리량이 ⑤ 와 같은가          |
| ④    | [34.1](DAY34_1_RESULT.md) | P3 W6, 60건                        | 60건          | 파티션보다 워커가 많으면 남는 워커가 놀고 처리량이 ③ 과 같은가 |
| ⑤    | [34.1](DAY34_1_RESULT.md) | P6 W3, 60건                        | 60건          | 워커 3대 기준 처리량                                         |
| ⑥    | [34.1](DAY34_1_RESULT.md) | P6 W6, 60건                        | 60건          | 워커를 6대로 늘리면 실제 모델에서도 처리량이 느는가          |

### 1-1. 실험별 목적과 목표 (원래 Day 에서 무엇을 알아냈고, 이번에 무엇을 다시 보는가)

| 번호 | 원래 Day 의 목적과 결론 (가짜 모델)                                                                                                                         | 이번 목적                                                | 이번 목표 (이렇게 나오면 결론이 유지된 것)                                                                          |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| ①    | Day 26: 동기 방식(Spring 이 FastAPI 응답을 기다림)은 FastAPI 가 한 번에 한 건만 처리해서 VU 20 만으로 504 가 난다. 가짜 5초 기준 200 은 8건, 504 는 22건    | 실제 모델의 응답 시간에서도 동기 방식이 무너지는지 본다  | 504 가 나고 `http_req_duration` p(95) 가 Spring 제한(30초)에 붙는다. 200 비율이 가짜 때(25%)와 얼마나 다른지 적는다 |
| ②    | Day 27: 비동기 방식(Spring 이 Kafka 에 넣고 바로 202)은 접수가 항상 빠르고, 처리는 뒤에서 한 건씩 한다. 504 가 없다. 완료까지의 시간만 대기 줄만큼 늘어난다 | 실제 모델에서도 접수는 빠르고 실패가 없는지 본다         | 접수 `p(95)` 가 수 ms~수백 ms 로 유지되고 504 가 없다. 완료까지 시간은 길어져도 모두 DONE 이 된다                   |
| ③    | Day 34.1: P3 W3 은 파티션과 워커가 3개, 3대라 일하는 워커가 3대이고 약 1.40건/초(가짜 2초)                                                                   | 파티션을 3개로 줄여도 실제 모델의 워커 3대 처리량이 ⑤ 와 같은지 본다 | ⑤ 와 비슷하다(일하는 워커가 3대로 같다). 실패 건이 없다 |
| ④    | Day 34.1: 일하는 워커 수는 `min(파티션, 워커)` 다. P1 W3 은 워커가 3대여도 파티션이 1개라 한 대만 일해 P1 W1 과 같은 0.49건/초였다                          | 파티션 3개에 워커 6대일 때 3대가 놀고 처리량이 늘지 않는지 본다 | ③ 과 비슷하다. 워커 6대 중 3대는 0건 처리한다 |
| ⑤    | Day 34.1: 처리량은 `min(파티션, 워커)` 로 정해진다. P6 W3 은 일하는 워커가 3대라 약 1.1~1.3건/초(가짜 2초)                                                  | 실제 모델의 워커 3대 처리량 기준값을 잰다                | 건당 시간 d 초일 때 약 3/d 건/초에 가깝다. 실패 건이 없다                                                           |
| ⑥    | Day 34.1: P6 W6 은 6대가 일해 약 2.43건/초. 워커를 늘리면(파티션도 충분할 때) 처리량이 는다                                                                 | 워커를 6대로 늘리면 실제 모델에서도 처리량이 느는지 본다 | ⑤ 보다 처리량이 늘어난다(이론상 약 2배). 안 늘어나면 Claude 호출 한도나 Gemini 임베딩 한도가 원인인지 따로 본다     |

공통으로 보는 것: 실제 모델이라 응답 시간이 흔들리고(가짜는 고정), 검색 도구가 Gemini 임베딩을 부르며, 호출 한도(429)가 변수로 들어온다는 점. 가짜 모델 결과와 **같은 점과 다른 점**을 모두 적는다.

합계 약 304건. 실제 에이전트는 질문 1건에 모델을 약 2번 부르고 검색 도구를 쓰면 임베딩도 부른다(아래 주의).
Day 28 (한계점)은 접수 서버의 한계를 보는 시험이라 모델이 필요 없고, 수만 건이라 하지 않는다.

## 2. 주의

- **임베딩은 Gemini 다.** 질문이 환불·배송 FAQ 라 검색 도구(`search_vector_db`)를 쓰고, 이때 Gemini 임베딩이 호출된다. Claude 만 쓰는 시험이 아니다. 임베딩 쪽이 429 를 내면 그 건은 실패로 기록되므로, 실패 건은 원인(Claude 인지 Gemini 인지)을 로그로 나눠 적는다.
- **Claude 호출 한도**(분당)는 계정 등급마다 다르고 확인하지 못했다. ③~⑥ 은 60건을 한꺼번에 접수하므로 한도에 먼저 걸릴 수 있다. 실패가 몰리면 건수를 줄여 다시 한다.
- 모델 응답은 실행마다 다르다. 숫자는 한 번의 측정이고, 가짜 모델처럼 같은 값이 나오지 않는다.

## 3. 먼저 한 건만 (스모크)

실제 모델 워커를 띄운 뒤(4절의 ①·② 와 같은 방식, `FAKE_LLM_ENABLED` 없이) 한 건을 보내 확인한다.

    curl -s -X POST localhost:8080/api/agent/sync -H 'Content-Type: application/json' \
      -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"claude"}'

답이 오고 `status` 가 200 이면 진행한다. 오류면 워커 로그에서 Claude(Anthropic) 쪽인지 Gemini 임베딩 쪽인지부터 본다.

## 4. 실행

공통: 가상환경을 켠 터미널, Spring 은 인자 없이 평소대로, 워커는 **`FAKE_LLM_ENABLED` 를 주지 않는다**.

### ① 동기 (Day 26)

    cd d2-ai-python && uvicorn app.main:app --port 8000      # 새 터미널, 가짜 설정 없이
    k6 run -e MODE=sync -e PROVIDER=claude -e MAX_VUS=20 -e POLL_MAX_SEC=60 scripts/k6/ramping.js

### ② 비동기 (Day 27)

같은 워커(평소 토픽 `agent.requests`, 그룹 `agent-workers`)로 한다.

    k6 run -e MODE=async -e PROVIDER=claude -e POLL=1 -e MAX_VUS=20 -e POLL_MAX_SEC=120 scripts/k6/ramping.js

①②는 질문이 5개를 돌아가며 나온다. 끝난 뒤 `agent-workers` LAG 이 0 인지 본다.

```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --describe
```

---

### ③④ 파티션 3개 (Day 34.1) — 위에서부터 그대로 복사해서 실행

토픽 `agent.bench.p3` 은 Day 34.1 때 만들어져 있으면 그대로 쓰고, 그룹 이름만 새로 쓴다(새 그룹은 토픽 끝부터 읽어서 옛 요청이 섞이지 않는다). 터미널은 가상환경을 켠 상태, 위치는 프로젝트 루트.

**1) ①② 에서 쓴 워커(8000)와 Spring 을 끈다.** 각 터미널에서 Ctrl+C. 워커가 남았는지 확인:

    bash scripts/bench-workers.sh stop

**2) Spring 을 P3 토픽으로 다시 띄운다.** 터미널로 실행하면:

    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3'

- IntelliJ 로 실행하면 실행 설정(Edit Configurations → Program arguments, 안 보이면 Modify options → Program arguments)에 아래 한 줄을 넣고 Run

    ```

    --app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3
    ```

기동이 끝나면 파티션이 3개인지 확인:

    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic agent.bench.p3

출력의 `PartitionCount: 3` 을 본다.

**3) ③ P3 W3 (워커 3대, 실제 모델)**

    bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-real-p3w3 real

10~20초 기다린 뒤:

    bash scripts/bench-workers.sh status

워커 3대에 `partitions` 가 하나씩 배정돼야 한다. 그다음 측정:

    python d2-ai-python/app/experiments/partition_bench.py --label real-p3w3 --provider claude --n 60 --workers localhost:8000,localhost:8001,localhost:8002

끝나면 워커를 끈다:

    bash scripts/bench-workers.sh stop

**4) ④ P3 W6 (워커 6대, 실제 모델)** — Spring 은 그대로 둔다(같은 P3).

    bash scripts/bench-workers.sh start 6 agent.bench.p3 bench-real-p3w6 real

10~20초 기다린 뒤:

    bash scripts/bench-workers.sh status

파티션 3개가 워커 3대에만 배정되고 나머지 3대는 `partitions` 가 비어 있어야 한다. 그다음 측정:

    python d2-ai-python/app/experiments/partition_bench.py --label real-p3w6 --provider claude --n 60 --workers localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005

끝나면 워커를 끈다:

    bash scripts/bench-workers.sh stop

### ⑤⑥ 파티션 6개 (Day 34.1) — 위에서부터 그대로 복사해서 실행

토픽 `agent.bench.p6` 은 Day 34.1 때 만들어져 있으면 그대로 쓰고, 그룹 이름만 새로 쓴다. ③④ 를 마친 뒤 이어서 한다. 터미널은 가상환경을 켠 상태, 위치는 프로젝트 루트.

**1) ③④ 에서 쓴 Spring 을 끄고 P6 토픽으로 다시 띄운다.** 터미널로 실행하면:

    ./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p6 --app.kafka.topics.requests-partitions=6'

- IntelliJ 로 실행하면 Program arguments 에 아래 한 줄을 넣고 Run

    ```

    --app.kafka.topics.requests=agent.bench.p6 --app.kafka.topics.requests-partitions=6
    ```

기동이 끝나면 파티션이 6개인지 확인:

    docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic agent.bench.p6

출력의 `PartitionCount: 6` 을 본다.

**2) ⑤ P6 W3 (워커 3대, 실제 모델)**

    bash scripts/bench-workers.sh start 3 agent.bench.p6 bench-real-p6w3 real

10~20초 기다린 뒤:

    bash scripts/bench-workers.sh status

워커 3대의 `partitions` 를 합치면 6 이어야 한다. 그다음 측정:

    python d2-ai-python/app/experiments/partition_bench.py --label real-p6w3 --provider claude --n 60 --workers localhost:8000,localhost:8001,localhost:8002

끝나면 워커를 끈다:

    bash scripts/bench-workers.sh stop

**3) ⑥ P6 W6 (워커 6대, 실제 모델)** — Spring 은 그대로 둔다(같은 P6).

    bash scripts/bench-workers.sh start 6 agent.bench.p6 bench-real-p6w6 real

10~20초 기다린 뒤:

    bash scripts/bench-workers.sh status

워커 6대에 `partitions` 가 하나씩 배정돼야 한다. 그다음 측정:

    python d2-ai-python/app/experiments/partition_bench.py --label real-p6w6 --provider claude --n 60 --workers localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005

**4) 끝나면 정리**

    bash scripts/bench-workers.sh stop

Spring 을 끄고 인자 없이 다시 띄운다(터미널은 `cd d3-was-spring && ./gradlew bootRun`, IntelliJ 는 Program arguments 칸을 **비우고 저장**한 뒤 Run).

## 5. 예상 (실행 전, 계산)

| 번호 | 예상                                                                                                                                         |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| ①    | 건당 응답이 가짜(5초)보다 길면 200 은 더 줄고 504 비율이 더 높다. 짧으면(약 3초) 200 이 늘 수 있다. 건당 시간은 아직 모른다                  |
| ②    | 접수 `p(95)` 는 가짜 때와 비슷(수 ms~수백 ms). 완료까지 시간만 길어진다. 20 VU 가 한 워커에 몰리므로 대기 시간이 건당 시간의 배수로 늘어난다 |
| ③④   | 일하는 워커가 3대로 같으므로 ③ 은 ⑤ 와 비슷하다(약 60/3 x d). ④ 는 워커 6대 중 3대가 놀아 ③ 과 비슷하다                                       |
| ⑤⑥   | 건당 시간이 d 초면 ⑤ 는 약 60/3 x d, ⑥ 은 약 60/6 x d. 6대에서 Claude 호출 한도 때문에 이론값에 못 미칠 수 있다(미확인)                      |

## 6. 결과에 적을 것

- ①: `http_reqs`, 200/504/502 건수, 성공 건 응답 시간(min/avg/max), 가짜 때(8건/32건)와 비교
- ②: 접수 `p(95)`, 완료까지 시간(min/med/p95/max), 실패 건수와 원인, LAG
- ③④: 전체 시간, TPS, 워커별·파티션별 건수, 놀고 있던 워커(0건 처리) 수, 실패 건수와 원인, 가짜 때(P3 W3 1.40건/초)와 비교
- ⑤⑥: ③④ 와 같은 항목, 가짜 때(P6 W3 1.13~1.29, P6 W6 2.43건/초)와 비교
- 가짜 모델 결과와 **다른 점**을 따로 적는다. 같다는 것도 하나의 결과다.

## 7. 결과를 적는 곳

실제 모델로 잰 결과는 **두 곳**에 적는다.

| 실험 | 원래 Day 결과 문서에 추가                                                      | Day 34.2 결과 문서                   |
| ---- | ------------------------------------------------------------------------------ | ------------------------------------ |
| ①    | [`DAY26_RESULT.md`](DAY26_RESULT.md) 에 "실제 모델(Claude)로 재측정" 절을 추가 | `DAY34_2_RESULT.md` 에 6개 실험 전체 |
| ②    | [`DAY27_RESULT.md`](DAY27_RESULT.md) 에 같은 절을 추가                         | 위와 같음                            |
| ③④   | `DAY34_1_RESULT.md` 에 같은 절을 추가                                          | 위와 같음                            |
| ⑤⑥   | `DAY34_1_RESULT.md` 에 같은 절을 추가                                          | 위와 같음                            |

- 원래 Day 문서의 절에는 그 Day 의 가짜 모델 결과와 나란히 비교하는 표를 둔다. 가짜 모델 쪽 기존 내용은 고치지 않는다.
- `DAY34_2_RESULT.md` 는 6개 실험을 한곳에 모으고, 가짜 모델과 다른 점, 실패 원인(Claude 인지 Gemini 임베딩인지)을 적는다.
- Day 35 이후 실제 모델로 잰 결과도 같은 방식으로 해당 Day 결과 문서에 "실제 모델" 절을 추가한다.

## 8. 정리

- 실제 모델 워커 종료, `bench-workers.sh stop`, Spring 인자 없이 다시 시작(IntelliJ Program arguments 비우기)
- `agent-workers` LAG 0 확인
