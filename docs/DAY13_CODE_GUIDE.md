# Day 13 코드 가이드 — aiokafka Producer / Consumer 단독 테스트와 docker-compose

> 실행하는 방법과 확인 항목은 [DAY13_GUIDE.md](DAY13_GUIDE.md), 개념은 [CONCEPTS_KAFKA.md](CONCEPTS_KAFKA.md). 이 문서는 두 스크립트와 `docker-compose.yml` 이 어떻게 맞물리는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **9092 = Kafka(호스트에서 접속)**, 29092 = Kafka(컨테이너 안에서 접속), 8090 = kafka-ui, 6333 = Qdrant, 6379 = Redis. 서버(8000, 8080)는 쓰지 않는다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 13 커밋(`912e026`) 시점의 것이다.

**전체 목적:** 에이전트나 Spring 과 연결하기 전에, Kafka 로 메시지가 들어가고(Producer) 나오는지(Consumer)를 가장 작은 코드로 확인하고, 뒤 Day 가 그대로 이어 쓰는 메시지 모양과 브로커 설정을 확정한다.


## 1. 전체 흐름

```
kafka_producer_test.py ──(JSON, key=requestId)──▶ Kafka 9092 / 토픽 agent.requests ──▶ kafka_consumer_test.py
   send_and_wait → partition, offset                                           getone → partition, offset, key, 내용
```

| 단계 | 하는 일                         | 파일                                                    | 함수                                   |
| ---- | ------------------------------- | ------------------------------------------------------- | -------------------------------------- |
| A    | 인프라 구성                     | `docker-compose.yml` (프로젝트 루트)                    | 서비스 qdrant, redis, zookeeper, kafka, kafka-ui |
| B    | 메시지 보내기                   | `d2-ai-python/app/checks/kafka_producer_test.py`      | `make_message`, `main`                 |
| C    | 메시지 받기                     | `d2-ai-python/app/checks/kafka_consumer_test.py`      | `main`                                 |


## 2. 주요 코드

### 2-1. 메시지 모양 (`make_message`)

```python
{"requestId": str(uuid.uuid4()), "question": "...", "provider": "gemini", "createdAt": datetime.now(timezone.utc).isoformat()}
```

이 4개 필드는 Day 14 의 Spring `AgentRequestMessage` 도 그대로 쓴다(코드 주석). 이 모양이 이후 모든 Day 에서 Spring 과 FastAPI 사이의 약속이다.

### 2-2. Producer

```python
producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"))
await producer.start()
meta = await producer.send_and_wait(topic, value=msg, key=msg["requestId"])
```

| 부분                         | 설명                                                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `value_serializer`          | Kafka 는 바이트만 다룬다. dict 를 JSON 으로 만들고 UTF-8 로 인코딩한다. `ensure_ascii=False` 라 한글이 그대로 들어간다 |
| `key=requestId`             | 같은 key 는 같은 파티션으로 간다(순서 보장 단위). 이 시점 토픽은 파티션이 1개라 차이가 없고, Day 18 에서 파티션이 늘면서 의미가 생긴다 |
| `send_and_wait`             | 브로커가 저장했다는 응답이 올 때까지 기다리고 `partition`, `offset` 을 돌려준다                                       |
| `finally: producer.stop()`  | 남은 배치를 비우고 연결을 닫는다                                                                                     |
| 토픽을 만들지 않음           | `KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'` 라서 처음 보낼 때 토픽이 생긴다(파티션 1개)                                |

### 2-3. Consumer

```python
consumer = AIOKafkaConsumer(topic, bootstrap_servers=BOOTSTRAP, group_id=group,
    auto_offset_reset="earliest", enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")))
msg = await asyncio.wait_for(consumer.getone(), timeout=timeout)
```

| 부분                              | 설명                                                                                                              |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `group_id`                        | 같은 그룹은 메시지를 나눠 받고, 그룹이 다르면 각자 처음부터 받는다(그룹마다 읽은 위치를 따로 저장)                    |
| `auto_offset_reset="earliest"`    | 그룹이 처음이면 토픽의 처음부터 읽는다                                                                              |
| `enable_auto_commit=True`         | 읽은 위치를 자동 저장한다. 같은 그룹으로 다시 실행하면 이어서 받는다. Day 16 에서 이 값을 수동 commit 으로 바꾼다        |
| `asyncio.wait_for(getone(), ...)` | 새 메시지가 `--timeout` 안에 안 오면 끝낸다(무한 대기 방지)                                                          |

### 2-4. `docker-compose.yml`

| 부분                                   | 설명                                                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 서비스                                 | qdrant(6333, 6334), redis(컨테이너 이름 `redis1`, 6379), zookeeper, kafka, kafka-ui                                                     |
| 리스너를 둘로 나눔                     | `EXTERNAL://localhost:9092`(호스트의 파이썬과 스프링), `INTERNAL://kafka:29092`(컨테이너 안의 kafka-ui). 광고 주소를 하나로 두면 kafka-ui 가 컨테이너 안에서 `localhost` 로 접속하려다 실패한다는 것이 파일 머리의 설명이다 |
| `kafka-ui` 호스트 포트 8090             | 8080 은 Spring Boot 기본 포트라 비워 두려고 옮겼다                                                                                    |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` | 그룹 첫 리밸런스 지연을 0 으로 줄여 로컬 실험 대기 시간을 줄인다                                                                     |
| 데이터 저장                            | Docker 볼륨 대신 `docker-data/` 아래 폴더에 바인드 마운트. 내용은 `.gitignore` 로 커밋하지 않는다(`.gitkeep` 만 커밋)                    |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` | 브로커 1대라서                                                                                                                       |


## 3. 결과를 읽는 순서

1. Producer 출력의 `partition=0 offset=0, 1, 2` 처럼 offset 이 늘어나는지 본다.
2. Consumer 출력의 `partition`, `offset`, `key`, `requestId` 가 Producer 가 보낸 것과 같은지 본다.
3. 같은 `--group` 으로 다시 실행하면 새 메시지만 오는지, 다른 `--group` 이면 처음부터 오는지 본다(DAY13_GUIDE 6번).
4. kafka-ui(`http://localhost:8090`)에서 토픽, 메시지, 그룹의 offset 을 눈으로 확인한다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                           |
| --------------------- | -------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 스크립트나 컨테이너를 실행하지 않았다. 실제 출력은 DAY13_RESULT 의 기록을 따른다                |
| 현재 compose 와의 차이 | 이후 Day 에 `docker-compose.yml` 이 바뀌었는지는 이번에 비교하지 않았다. 이 문서는 Day 13 커밋의 파일을 읽은 것이다 |
