# Kafka 개념 정리

> Day 13(aiokafka 단독 테스트)에서 정리한 내용. 코드와의 대응은 `app/checks/kafka_producer_test.py`, `kafka_consumer_test.py` 기준이다.
> 아래 "확인하지 않은 값"으로 표시한 항목은 일반적인 기본값이며 이 프로젝트의 설정으로 확인하지 않았다.

## 1. Kafka 가 뭔가

Kafka 는 **메시지를 순서대로 쌓아 두는 분산 로그 저장소**다. 보내는 쪽(Producer)이 토픽에 메시지를 쓰고, 받는 쪽(Consumer)이 자기 속도로 읽는다. 둘은 서로 모르고 Kafka 만 안다. 메시지 큐처럼 쓸 수 있지만, 읽어도 메시지가 지워지지 않는다는 점이 전통적인 MQ 와 다르다.

이 프로젝트에서 쓰는 이유는 LLM 호출이 1~38초까지 걸리고 불안정하기 때문이다. WAS 가 LLM 응답을 기다리면 스레드가 묶인다. 그래서 WAS 는 요청을 Kafka 에 넣고 바로 `202 Accepted` 를 돌려주고, FastAPI Consumer 가 자기 속도로 꺼내 처리한다.

```
[Spring WAS] --메시지--> [Kafka 토픽] --읽기--> [FastAPI Consumer] --> 에이전트
  (Day 14)                                       (Day 15~16)
```

## 2. MQ 와의 차이

| 구분 | 전통 MQ (RabbitMQ 등) | Kafka |
| --- | --- | --- |
| 기본 모델 | 큐에 넣고 꺼내면 사라짐 | 로그에 쌓고, 읽어도 남아 있음 |
| 읽은 위치 | 브로커가 메시지 단위로 관리 (ack 하면 삭제) | Consumer 그룹이 offset 으로 관리 |
| 다시 읽기 | 어려움 | 가능 (offset 을 되돌리거나 새 그룹으로 읽음) |
| 여러 소비자 | 한 메시지는 보통 한 소비자만 처리 | 그룹이 다르면 각자 전부 받음 |
| 순서 보장 | 큐 단위 | 파티션 단위 |
| 확장 | 큐나 소비자 추가 | 파티션을 늘려 병렬 처리 |
| 삭제 | 처리되면 삭제 | 보관 기간이나 크기 기준으로 삭제 |
| 강점 | 라우팅, 우선순위, 요청/응답 | 대용량, 재처리, 이벤트 이력 |

선입선출(순서)은 지켜진다. 다만 꺼낼 때 지우지 않는다.

### 무겁다는 것

| 구분 | Kafka | RabbitMQ |
| --- | --- | --- |
| 운영 구성 | 무거움. 브로커 외에 ZooKeeper(또는 KRaft), 파티션, 복제, 리스너 설정을 알아야 함 | 가벼움. 브로커 하나로 시작 가능 |
| 메모리 | 큼 | 상대적으로 적음 |
| 처리 성능 | 대용량에 강함 | 중간 규모까지 충분 |
| 학습량 | 많음 (파티션, offset, 그룹, 리밸런싱) | 적음 |

이 프로젝트의 부하는 LLM 호출이 병목이라 초당 수십 건 수준이다. Kafka 의 처리량 강점이 필요한 규모가 아니다. "성능 때문에 Kafka" 가 아니라 **재처리, 파티션 확장 실험, 이력 보존**을 직접 다뤄 보려고 선택했다. 이 규모에서는 과할 수 있다는 점은 보고서의 "실험의 한계"(Day 40)에 적는다.

## 3. 핵심 용어

| 용어 | 뜻 | Day 13 코드/출력 |
| --- | --- | --- |
| 브로커 | Kafka 서버 | `docker-compose.yml` 의 `kafka` 컨테이너 |
| 토픽 | 메시지 종류별 이름 붙은 로그 | `agent.requests` (`DEFAULT_TOPIC`) |
| 파티션 | 토픽을 나눈 배열. 순서 보장은 파티션 안에서만 | 출력의 `partition=0` (자동 생성 토픽은 1개) |
| offset | 파티션 안의 메시지 번호 | `offset=0, 1, 2` |
| key | 같은 key 는 같은 파티션으로 감 | `key=msg["requestId"]` |
| Consumer group | 읽는 팀. 같은 그룹은 나눠 받고, 다른 그룹은 각자 전부 받음 | `group_id="day13-test"` |
| commit | 그룹의 읽은 위치를 저장 | `enable_auto_commit=True` |

## 4. 읽어도 지워지지 않는다: 배열과 책갈피

파티션은 **뒤에만 추가되는 배열**이다. offset 은 그 배열의 인덱스이고, 그룹의 책갈피는 "다음에 읽을 인덱스"다. 읽을 때 바뀌는 것은 책갈피 숫자뿐이고 메시지는 건드리지 않는다.

```python
log = []                          # 파티션 (뒤에만 추가, 수정/삭제 없음)
log.append("질문1")                # offset 0   <- Producer
log.append("질문2")                # offset 1
log.append("질문3")                # offset 2

pos = {"day13-test": 0}           # 그룹별 책갈피 (Kafka 의 __consumer_offsets)

msg = log[pos["day13-test"]]      # 읽기 (배열에서 지우지 않음)   <- Consumer
pos["day13-test"] += 1            # 책갈피만 1 증가 (commit)
```

| 코드 비유 | 실제 Kafka |
| --- | --- |
| 배열 `log` | 파티션 (디스크에 이어 쓰는 로그 파일) |
| 인덱스 | offset |
| `pos` 딕셔너리 | 내부 토픽 `__consumer_offsets` 에 저장되는 그룹별 위치 |
| `pos += 1` | commit |
| `log.append` | `send_and_wait` |

### 동작 그림

```
① Producer 가 3건을 쌓음
 번호   [ 0 ]   [ 1 ]   [ 2 ]
 내용   질문1    질문2    질문3         그룹 day13-test: 기록 없음

② 새 그룹이 earliest 로 시작 -> 0번부터
 번호   [ 0 ]   [ 1 ]   [ 2 ]
          ▲

③ 읽을 때마다 책갈피만 이동 (메시지는 그대로)
 0번 읽음   [ 0 ]   [ 1 ]   [ 2 ]      책갈피 = 1
                     ▲
 1번 읽음   [ 0 ]   [ 1 ]   [ 2 ]      책갈피 = 2
                             ▲
 2번 읽음   [ 0 ]   [ 1 ]   [ 2 ]      책갈피 = 3
                                     ▲

④ 같은 그룹으로 다시 실행: 책갈피 3, 읽을 게 없음 -> 시간 초과

⑤ 다른 그룹(g2): 그룹마다 책갈피가 따로
 번호        [ 0 ]   [ 1 ]   [ 2 ]
 day13-test  ──────────────────▲   (3)
 g2          ▲                     (0, 처음부터 읽음)
```

일반 큐는 꺼내면 메시지가 사라지고, Kafka 는 책갈피만 움직인다.

```
일반 큐:  [질문1][질문2][질문3] -> [질문2][질문3] -> [질문3] -> []
Kafka:    [질문1][질문2][질문3]  (읽어도 그대로, 책갈피만 이동)
```

### 왜 지우지 않게 설계했나

1. **재처리:** 처리 실패나 버그 수정 후 책갈피를 되돌려 같은 요청을 다시 처리한다.
2. **여러 소비자:** 그룹이 다르면 같은 메시지를 각자 전부 받는다.
3. **속도:** 항상 파일 끝에 이어 쓰고 순서대로 읽으므로 디스크를 효율적으로 쓴다.

## 5. 쌓이면 언제 지우나 (보관 정책)

지우는 기준은 "읽었는지"가 아니라 "오래됐는지/커졌는지"다.

| 항목 | 내용 |
| --- | --- |
| 보관 기간 `log.retention.hours` | 일반적인 기본값은 168시간(7일). 지난 구간을 삭제 (확인하지 않은 값) |
| 보관 크기 `log.retention.bytes` | 설정하면 파티션 크기가 넘을 때 오래된 구간부터 삭제 |
| 삭제 단위 | 메시지가 아니라 세그먼트 파일 단위 |
| 로그 컴팩션 | 같은 key 의 최신 값만 남기고 지움 |
| 수동 삭제 | 토픽 삭제, 또는 특정 offset 앞까지 삭제 |

- 읽지 않은 메시지도 보관 기간이 지나면 지워진다. 컨슈머가 오래 멈춰 있으면 읽지 못한 메시지가 사라질 수 있다.
- 보관을 무기한으로 두거나 크기 제한 없이 대용량을 받으면 디스크가 찬다. 운영에서는 보관 설정과 디스크 사용량을 같이 본다.
- 로컬 초기화는 `docker compose down` 후 `docker-data/kafka` 와 `docker-data/zookeeper-*` 의 내용을 지운다. 한쪽만 지우면 기동에 실패할 수 있다.

### Redis 와 비교

| 방식 | Redis | Kafka |
| --- | --- | --- |
| 용량이 차면 | `maxmemory` 초과 시 정책(`allkeys-lru`, `volatile-ttl`, `allkeys-lfu` 등)으로 키 삭제. `noeviction` 이면 쓰기가 실패 | 시간이나 크기를 넘은 오래된 세그먼트를 자동 삭제 |
| 규칙 지정 단위 | **키 단위** TTL (`EXPIRE`) | **토픽 단위** 보관 설정. 메시지별 TTL 은 없음. 소비자 그룹은 삭제 규칙을 가질 수 없음 |
| 수동 삭제 | `DEL`, 스트림은 `XDEL`, `XTRIM` | 토픽 삭제, `deleteRecords` |

- Redis 는 메모리가 차면 정책으로 지우는 반응형이고, Kafka 는 보관 기간과 크기 기준의 주기적 삭제다.
- Redis Streams 의 엔트리는 `maxmemory` 정책으로 지워지지 않고 키 전체가 지워진다. 엔트리는 `XADD ... MAXLEN`, `XTRIM` 으로 직접 관리해야 한다.

## 6. 토픽과 파티션

토픽 하나는 파티션 여러 개로 나뉜다. 파티션은 **0번부터** 시작하고, offset 도 파티션마다 0부터 따로 센다. 책갈피도 파티션마다 따로 있다.

```
토픽 A (파티션 3개)              토픽 B (파티션 2개)
  파티션 0: [0][1][2]...           파티션 0: [0][1]...
  파티션 1: [0][1]...              파티션 1: [0][1][2]...
  파티션 2: [0][1][2][3]...
```

토픽 A 의 파티션 0 과 토픽 B 의 파티션 0 은 별개의 배열이다.

### 왜 나누나

- **병렬 처리:** 한 파티션은 같은 그룹 안에서 한 번에 소비자 하나만 읽는다. 소비자를 늘리려면 파티션이 있어야 한다. 처리량의 상한은 파티션 수다.
- **쓰기 분산:** Producer 의 쓰기 부하도 나뉜다.

```
파티션 0  ──▶ 소비자 A ┐
파티션 1  ──▶ 소비자 B ├ 같은 그룹: 3명이 나눠 처리
파티션 2  ──▶ 소비자 C ┘
```

### 어느 파티션으로 가나

key 를 주면 해시로 파티션을 정한다. 같은 key 는 같은 파티션이다.

| key | 효과 |
| --- | --- |
| `requestId` (요청마다 다름) | 파티션에 골고루 퍼짐 |
| `userId` | 같은 사용자의 요청은 같은 파티션이라 순서가 지켜짐 |

### 순서와 병렬의 맞교환

순서는 파티션 안에서만 보장된다. 파티션이 1개면 전체 순서가 보장되지만 병렬 처리를 못 한다.

### 주의

- 파티션 수는 늘릴 수는 있어도 줄일 수는 없다.
- 늘리면 key 해시 결과가 달라져서 같은 key 가 다른 파티션으로 갈 수 있다.
- 자동 생성된 토픽은 파티션이 1개다 (Day 13 의 `agent.requests`).

### 토픽을 나누는 기준

성격이 다른 메시지는 토픽을 나누고, 같은 종류를 병렬로 처리하려면 파티션을 나눈다.

| 토픽(예시) | 용도 | 파티션 |
| --- | --- | --- |
| `agent.requests` | WAS 가 보내는 에이전트 요청 | 병렬 실험 때 늘림 (Day 18, 34) |
| `agent.responses` | 처리 결과 (필요하다면) | 요청보다 적어도 됨 |
| `agent.requests.dlq` | 실패한 요청 (Day 32) | 1개면 충분 |

위 구성은 예시이고, 실제 토픽 구성은 Day 16~17 에서 정한다.

## 7. Day 13 코드 설명

### Producer (`kafka_producer_test.py`)

```python
producer = AIOKafkaProducer(
    bootstrap_servers=BOOTSTRAP,                       # localhost:9092
    value_serializer=lambda v: json.dumps(v, ...).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)
await producer.start()
meta = await producer.send_and_wait(topic, value=msg, key=msg["requestId"])
await producer.stop()
```

| 부분 | 설명 |
| --- | --- |
| `bootstrap_servers` | 최초 접속 주소. 여기서 클러스터 정보를 받는다 |
| `value_serializer` | Kafka 는 바이트만 다룬다. dict 를 JSON 문자열로 바꾸고 UTF-8 바이트로 변환. Spring 도 같은 JSON 을 보낸다 |
| `send_and_wait` | 브로커가 저장했다고 응답할 때까지 기다림. 반환값에 partition 과 offset 이 있어 `[sent] partition=0 offset=0` 이 출력됨 |
| `key=requestId` | key 를 해시해 파티션을 정함. 지금은 파티션이 1개라 차이가 안 보임 |
| `start()/stop()` | `stop()` 이 보내지 못한 메시지를 비우고 연결을 닫음. `try/finally` 로 감싼 이유 |

### Consumer (`kafka_consumer_test.py`)

```python
consumer = AIOKafkaConsumer(
    topic,
    bootstrap_servers=BOOTSTRAP,
    group_id=group,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)
msg = await asyncio.wait_for(consumer.getone(), timeout=timeout)
```

| 부분 | 설명 |
| --- | --- |
| `group_id` | 읽은 위치는 그룹 단위로 저장됨. 같은 그룹이면 이어서, 다른 그룹이면 처음부터 |
| `auto_offset_reset="earliest"` | 이 그룹에 저장된 위치가 없을 때 토픽의 처음부터 읽음 (`latest` 는 지금부터) |
| `enable_auto_commit` | 읽은 offset 을 주기적으로 저장 |
| `getone()` + `wait_for` | 메시지 하나를 기다림. 시간 제한으로 무한 대기를 막음 |
| `key=b'...'` 출력 | `key_deserializer` 를 지정하지 않아 key 가 바이트로 출력됨. value 는 JSON 으로 복원됨 |

### 실행에서 본 로그의 정체

| 로그 | 의미 |
| --- | --- |
| Producer: `Topic agent.requests is not available during auto-create initialization` (2번) | 토픽이 없어서 브로커가 자동 생성하는 중. 클라이언트가 재시도해 해결됨. 처음에만 나옴 |
| Consumer: `GroupCoordinatorNotAvailableError (Error 15)` (6번) | 그룹 offset 을 저장하는 내부 토픽 `__consumer_offsets` 가 처음 만들어지는 중. 재시도로 해결됨. 처음에만 나옴 |
