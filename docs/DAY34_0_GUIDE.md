# Day 34 가이드 — 파티션 수와 워커 수에 따른 처리량(TPS) 측정 (가짜 모델 → 실제 모델)

> 포트: **8080 = Spring WAS**, **8000~8005 = FastAPI 워커**(워커 수만큼, 8000 부터), 9092 = Kafka(호스트), 6379 = Redis.
> 측정 도구(`scripts/bench-workers.sh`, `partition_bench.py`)의 코드 설명은 [DAY34_0_CODE_GUIDE.md](DAY34_0_CODE_GUIDE.md).

**전체 목적:** 같은 측정 도구로 처리량을 두 번 잰다. 먼저 가짜 모델로 처리량이 파티션 수와 워커 수 중 무엇으로 정해지는지 보고, 다음에 실제 모델(Claude)로 같은 결론이 나오는지 확인한다.


## 1. Day 34 의 흐름

| 순서 | 문서                         | 모델           | 구동한 것                             | 보려는 것                                           |
| ---- | ---------------------------- | -------------- | ------------------------------------- | --------------------------------------------------- |
| 1    | [Day 34.1](DAY34_1_GUIDE.md) | 가짜(고정 2초) | Spring → Kafka → 워커 → Redis         | 처리량이 파티션 수와 워커 수 중 무엇으로 정해지는가 |
| 2    | [Day 34.2](DAY34_2_GUIDE.md) | 실제 Claude    | 위 경로 + Qdrant 검색 + Gemini 임베딩 | 같은 결론이 실제 모델에서도 나오는가                |


## 2. 문서

| 문서                                           | 내용                                                    |
| ---------------------------------------------- | ------------------------------------------------------- |
| [DAY34_0_CODE_GUIDE.md](DAY34_0_CODE_GUIDE.md) | 측정 도구 두 파일이 무엇을 어떤 순서로 하는지           |
| [DAY34_1_GUIDE.md](DAY34_1_GUIDE.md)           | 가짜 모델 실행 방법과 예상                              |
| [DAY34_1_RESULT.md](DAY34_1_RESULT.md)         | 가짜 모델 결과                                          |
| [DAY34_2_GUIDE.md](DAY34_2_GUIDE.md)           | 실제 모델(Claude) 실행 방법과 예상(동기·비동기, 파티션) |
| [DAY34_2_RESULT.md](DAY34_2_RESULT.md)         | 실제 모델(Claude) 결과                                  |


## 3. 가짜 모델과 실제 모델로 띄우는 법

측정 순서(Spring 실행 → 워커 시작 → 파티션 배정 확인 → 측정 → 워커 종료)는 같고, 아래 두 인자만 다르다.

| 항목                                 | 가짜 모델 (Day 34.1)                                         | 실제 모델 (Day 34.2)                                                  |
| ------------------------------------ | ------------------------------------------------------------ | --------------------------------------------------------------------- |
| `bench-workers.sh start` 의 넷째 인자 | 모델 대기 초(생략하면 2)                                     | `real`                                                                |
| 워커 환경변수                        | `FAKE_LLM_ENABLED=true`, `FAKE_LLM_DELAY_SEC`, `FAKE_LLM_JITTER=0` | 가짜 모델 변수를 주지 않는다                                          |
| `partition_bench.py` 의 `--provider` | 생략(기본 `fake`)                                            | `claude`                                                              |
| 그룹 이름 예                         | `bench-p3w3`                                                 | `bench-real-p3w3`                                                     |
| 모델 호출                            | 없음(비용 없음)                                              | 요청마다 Claude 와 검색 도구의 Gemini 임베딩을 실제로 부른다(비용 듦) |

예: 파티션 3개, 워커 3대. Spring 은 두 경우 모두 같은 인자로 띄운다.

```
./gradlew bootRun --args='--app.kafka.topics.requests=agent.bench.p3 --app.kafka.topics.requests-partitions=3'
```

가짜 모델:

```
bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-p3w3
python d2-ai-python/app/experiments/partition_bench.py --label p3w3 --n 60 --workers localhost:8000,localhost:8001,localhost:8002
```

실제 모델:

```
bash scripts/bench-workers.sh start 3 agent.bench.p3 bench-real-p3w3 real
python d2-ai-python/app/experiments/partition_bench.py --label real-p3w3 --provider claude --n 60 --workers localhost:8000,localhost:8001,localhost:8002
```

끝나면 `bash scripts/bench-workers.sh stop` 으로 워커를 끈다. 조합별 전체 명령은 각 Day 의 가이드에 있다.
