# Day 18 코드 가이드 — 같은 그룹에 워커를 여러 개 띄우기: 워커 ID, 파티션 배정, 리밸런싱, 시간 로그

> 실행하는 방법과 확인 항목은 [DAY18_GUIDE.md](DAY18_GUIDE.md), 결과는 [DAY18_RESULT.md](DAY18_RESULT.md). 이 문서는 워커가 여러 개일 때 코드가 무엇을 기록하고 어떻게 나눠 처리하는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **9092 = Kafka**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**(8002 이상도 같은 방식). 워커마다 `KAFKA_WORKER_ID` 를 따로 주고 포트를 다르게 띄운다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 18 커밋 4개(`740b253`, `f3b849a`, `123ce79`, `1f4eb00`) 시점의 것이다.

**전체 목적:** 같은 consumer group 에 워커를 여러 개 띄우면 파티션이 나뉘어 병렬로 처리된다. 이 Day 에서 추가된 장치(어느 워커가 어느 파티션을 맡았는지의 기록, 리밸런싱 알림, 건당 단계별 시간 로그, 파티션 3개, 여러 워커를 한 번에 보는 E2E 스크립트)가 코드에서 어떻게 만들어지는지 따라간다.


## 1. 전체 흐름

```
Spring: 토픽 agent.requests 파티션 3개 (application.properties: requests-partitions=3, KafkaTopicConfig 가 보장)
   key=requestId → 파티션 0/1/2 중 하나

워커 N대 (같은 group agent-workers, 서로 다른 KAFKA_WORKER_ID, 서로 다른 포트)
   consumer.subscribe([TOPIC], listener=_RebalanceLogger)
      on_partitions_revoked  → 맡은 파티션을 내놓음 (partitions = [])
      on_partitions_assigned → partitions 갱신, rebalances += 1, 로그 "[w1] 파티션 배정 [1] (리밸런싱 1번째)"
   메시지 → agent_handler → store.set_processing(requestId, worker, partition, offset) → run_agent → set_done
   처리 후 commit: CommitFailedError(리밸런싱으로 파티션을 잃음)면 경고만 남기고 계속

E2E 스크립트: FASTAPI="localhost:8000 localhost:8001" 로 두 워커의 /health 를 합산하고 결과를 차례로 물어 읽음
```

| 단계 | 하는 일                          | 파일                                                       | 함수 / 클래스                                    |
| ---- | -------------------------------- | ---------------------------------------------------------- | ------------------------------------------------ |
| A    | 워커 ID, 리밸런싱 알림           | `d2-ai-python/app/services/kafka_worker.py`                | `WORKER_ID`, `_RebalanceLogger`, `KafkaWorker._run`, `snapshot` |
| B    | 결과에 워커, 파티션, offset 기록 | `d2-ai-python/app/services/result_store.py`, `agent_handler.py` | `set_processing(request_id, worker, partition, offset)` |
| C    | 단계별 시간 로그                 | `d2-ai-python/app/services/agent_graph.py`                 | `search_vector_db`, `agent_node`, `tool_node`, `run_agent` |
| D    | 파티션 수 3                      | `d3-was-spring/src/main/resources/application.properties`  | `app.kafka.topics.requests-partitions=3`         |
| E    | 여러 워커 E2E                    | `scripts/day17_e2e.sh`                                     | `read_health`, 단계 4) 결과 조회 루프            |


## 2. 주요 코드

### 2-1. 워커 구분 (`kafka_worker.py`)

| 부분                              | 설명                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKER_ID`                       | `KAFKA_WORKER_ID` 환경변수, 없으면 `worker-<프로세스 번호>`. 로그와 `/health` 에서 워커를 구분한다                                       |
| `_RebalanceLogger`                | `ConsumerRebalanceListener` 를 구현한다. 회수 때 `partitions = []`, 배정 때 `partitions` 갱신, `rebalances += 1` 과 로그를 남긴다         |
| `subscribe(..., listener=...)`    | `[OLD]` 는 토픽을 `AIOKafkaConsumer(TOPIC, ...)` 생성자에 넣었는데, 그러면 리밸런싱 알림을 받을 수 없다. 그래서 `subscribe` 로 구독한다   |
| `snapshot()` 추가 필드            | `worker_id`, `partitions`(워커가 파티션보다 많으면 빈 목록인 워커가 생긴다), `rebalances`                                                |
| `CommitFailedError` 처리           | 처리하는 사이 리밸런싱으로 파티션이 다른 워커에게 넘어가면 commit 이 거절된다. 경고 로그만 남기고 계속하며, 새로 맡은 워커가 같은 메시지를 다시 받아 처리한다(같은 요청이 두 번 처리될 수 있다, 코드 주석) |
| consumer 를 닫을 때                | `self.partitions = []` 로 비운다                                                                                                         |

### 2-2. 결과에 남기는 값

`set_processing` 이 `worker`, `partition`, `offset` 인자를 받아 저장한다(`[OLD]` 는 `request_id` 만 받았다). `agent_handler` 의 시작과 완료 로그에도 `worker=` 가 붙는다. 이후 `set_done` 은 이 필드를 지우지 않는다(Day 23 에서 확인하는 항목).

### 2-3. 단계별 시간 로그 (`agent_graph.py`)

`[timing]` 로그가 세 곳에 붙었다.

| 로그                                              | 측정 대상                                              |
| ------------------------------------------------- | ------------------------------------------------------ |
| `[timing] 임베딩+Qdrant 검색 N초`                 | `search_vector_db` 안의 검색(질문 임베딩 + Qdrant)      |
| `[timing] LLM 호출(agent_node) N초, 도구 요청 M개` | `agent_node` 의 모델 호출 1번                          |
| `[timing] 도구 <이름> N초`                         | `tool_node` 에서 도구 1개 실행                         |
| `[timing] 그래프 전체(run_agent) N초`             | `run_agent` 안의 그래프 한 번 실행 전체                |

건당 시간이 어디에 쓰이는지(LLM 이 대부분인지) 보려는 것이다. 모두 `print` 로 남기며, `[OLD]` 코드를 주석으로 남겼다.

### 2-4. 파티션 수

`KafkaTopicConfig` 는 기존 토픽의 파티션 수가 설정값보다 작으면 늘린다. `requests-partitions` 를 1에서 3으로 바꾼 것이 이 Day 의 Spring 쪽 변경 전부다(같은 커밋에서 한글 주석이 `\uXXXX` 이스케이프로 저장된 부분도 있다). 파티션 수는 줄일 수 없다는 점은 코드가 아니라 Kafka 의 성질이다.

### 2-5. E2E 스크립트의 변경 (`scripts/day17_e2e.sh`)

| 변경                           | 설명                                                                                                                                  |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `FASTAPI` 에 주소를 여러 개     | `HOSTS=($FASTAPI)`. 공백으로 구분한 주소를 모두 본다                                                                                   |
| `read_health`                  | 모든 워커의 `/health` 를 읽어 `received`, `failed`, `skipped`, `committed` 를 합산하고, 처리 중인 워커 수(`BUSY`)와 워커별 `partitions` 를 모은다. 하나라도 못 읽으면 실패 |
| 완료 조건                      | 처리 합계가 접수 건수 이상이고 `BUSY == 0`                                                                                              |
| 결과 조회                      | 주소를 차례로 물어 `200` 이 오는 곳에서 읽는다(Day 17~22 의 저장소는 처리한 워커의 메모리에만 있었기 때문). Day 23 이후에는 첫 주소에서 200 이 온다 |
| 출력 추가                      | 건별 `worker=`, `partition=`, 워커별/파티션별 건수, **동시 처리 배율** = 건당 처리 시간(`elapsedMs`)의 합 / 전체 완료 시간. 1.0 이면 한 건씩, 워커가 나눠 처리하면 1 보다 커진다 |


## 3. 결과를 읽는 순서

1. 워커를 띄울 때마다 로그의 `파티션 배정 [..] (리밸런싱 n번째)` 와 `/health` 의 `partitions` 를 본다. 기존 워커의 파티션이 회수되고 다시 나뉘는지 본다.
2. E2E 의 워커별 처리 건수와 파티션별 건수를 본다. 워커 수가 파티션 수(3)를 넘으면 일하지 않는 워커가 생긴다.
3. 동시 처리 배율이 1.0 에서 얼마나 올라가는지 본다. 같은 파티션에 요청이 몰리면 워커가 여럿이어도 배율이 낮을 수 있다.
4. `[timing]` 로그로 건당 시간 중 LLM 호출이 차지하는 비중을 본다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 워커를 띄우거나 스크립트를 실행하지 않았다. 워커 1~4개 비교와 리밸런싱 로그는 DAY18_RESULT 를 따른다                     |
| 현재 코드와의 차이    | `[timing]` 로그와 `kafka_worker.py` 는 이후 Day 에 더 바뀌었다(Day 20~22 의 대화와 락, Day 23 의 Redis). 이 문서는 Day 18 커밋 시점이다   |
| 중복 처리             | `CommitFailedError` 뒤 같은 요청이 두 번 처리될 수 있다는 것은 코드 주석을 옮긴 것이고, 실제로 재현하지 않았다                            |
