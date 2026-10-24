# Day 13 가이드 — aiokafka 단독 테스트

> 상태: 완료(2026-10-10). 선택 항목(실행 중 Consumer 의 STABLE 표시)과 그룹 상세 화면은 확인하지 않음. 결과는 `DAY13_RESULT.md`. 결과는 `DAY13_RESULT.md` 에 따로 적는다.

## 1. 오늘 목표

에이전트나 WAS 와 연결하기 전에 **Kafka 만 따로** 동작을 확인한다. 나중에 연동이 안 될 때 Kafka 문제인지 연동 문제인지 가르기 위해서다.

| 확인할 것 | 방법 |
| --- | --- |
| Kafka 가 뜨고 파이썬에서 접속된다 | Producer 가 연결에 성공 |
| 메시지가 토픽에 들어간다 | Producer 출력의 partition / offset |
| 메시지를 받는다 | Consumer 출력 |
| 읽은 위치가 저장된다 | Consumer 를 다시 실행하면 이어서 받는지 |
| Spring(Day 14)과 주고받을 메시지 모양 | 아래 3장 JSON 스키마 |

## 2. 만든 파일

| 파일 | 역할 | 실행 |
| --- | --- | --- |
| `ai-agent-serving-infra/docker-compose.yml` (레포 루트) | qdrant, redis, zookeeper, kafka, kafka-ui. 데이터는 `ai-agent-serving-infra/docker-data/` 에 저장 | `docker compose up -d kafka kafka-ui` |
| `ai-agent-serving-infra/docker-data/` | 컨테이너 데이터 폴더 (내용은 git 제외) | - |
| `ai-agent-serving-infra/.gitignore` | `docker-data/*` 제외 추가 | - |
| `app/checks/kafka_producer_test.py` | JSON 메시지를 토픽에 보냄 | `python -m app.checks.kafka_producer_test` |
| `app/checks/kafka_consumer_test.py` | 토픽에서 받아 partition / offset / 내용 출력 | `python -m app.checks.kafka_consumer_test` |
| `requirements.txt`, `app/requirements.txt` | `aiokafka==0.12.0` 추가 | `pip install aiokafka==0.12.0` |

파이썬 스크립트는 `d2-ai-python` 에서 가상환경을 켜고 실행한다. 컨테이너와 스크립트 실행은 직접 한다.

## 3. 메시지 스키마 (Spring 과 공유)

토픽 이름은 `agent.requests`, 값은 JSON, key 는 `requestId` 문자열이다.

| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `requestId` | string (UUID) | 요청 추적용 id. Day 17 의 202 응답에 담아 돌려줄 값 |
| `question` | string | 사용자 질문 |
| `provider` | string | `gemini` 등 (`get_model(provider)` 에 전달) |
| `createdAt` | string (ISO-8601) | 생성 시각 |

Day 14 의 Spring Producer 도 같은 모양으로 보낸다. 필드를 바꾸면 양쪽을 같이 고친다.

## 4. docker-compose 설명

레포 루트 `docker-compose.yml` 이 이 프로젝트의 **유일한** compose 파일이다. `d1-ai-agent/docker-compose.yml` 은 더 이상 쓰지 않는다. 서비스는 qdrant, redis, zookeeper, kafka, kafka-ui 이고 d1 에서 바꾼 점은 네 가지다.

| 변경 | 이유 |
| --- | --- |
| 데이터를 Docker named volume 대신 `ai-agent-serving-infra/docker-data/` 하위 폴더에 저장 (바인드 마운트) | 데이터가 어디 있는지 눈에 보이고, 폴더만 지우면 초기화된다 |
| Kafka 리스너를 INTERNAL(`kafka:29092`)과 EXTERNAL(`localhost:9092`)로 분리 | d1 처럼 광고 주소가 `localhost:9092` 하나뿐이면 컨테이너 안의 kafka-ui 가 자기 자신(localhost)으로 접속하려다 실패한다. 호스트(파이썬, 스프링)는 EXTERNAL, 컨테이너끼리는 INTERNAL |
| kafka-ui 호스트 포트 8080 → 8090 | 8080 은 Spring Boot 기본 포트 |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` | 로컬 실험에서 컨슈머 그룹 시작 대기 시간 단축 |

### 데이터 폴더

| 폴더 | 컨테이너 안 경로 | 들어 있는 것 |
| --- | --- | --- |
| `docker-data/qdrant` | `/qdrant/storage` | Qdrant 컬렉션 (벡터, 페이로드) |
| `docker-data/redis` | `/data` | Redis 덤프 |
| `docker-data/kafka` | `/var/lib/kafka/data` | 토픽, 메시지, consumer offset |
| `docker-data/zookeeper-data` | `/var/lib/zookeeper/data` | 브로커, 토픽 메타데이터 |
| `docker-data/zookeeper-log` | `/var/lib/zookeeper/log` | ZooKeeper 트랜잭션 로그 |

- `docker-data/` 의 내용은 `.gitignore` 로 커밋하지 않는다(`.gitkeep` 만 커밋).
- `docker compose down` 은 컨테이너만 지우고 데이터는 남는다. 초기화하려면 `down` 한 뒤 해당 하위 폴더의 **내용**을 지운다. Kafka 는 `docker-data/kafka` 와 `docker-data/zookeeper-*` 를 같이 지운다. 한쪽만 지우면 브로커 id / 클러스터 id 가 어긋나 기동에 실패할 수 있다.
- 바인드 마운트라 폴더 권한 문제가 생길 수 있다. 기동 직후 컨테이너가 죽으면 `docker compose logs kafka` 에 `Permission denied` 가 있는지 본다.

### 주의: 기존 Qdrant 데이터

d1 compose 로 적재해 둔 Qdrant 데이터(`documents-12` 등)는 옛 named volume 에 있고 `docker-data/qdrant` 에는 없다. Day 7~12 코드를 다시 쓰려면 `qdrant_ingest` 등으로 다시 적재한다. 옛 컨테이너가 떠 있으면(이름, 포트 충돌) 먼저 내린다. 옛 볼륨은 지우지 않았다.

## 5. 실행 순서

```bash
# 0) 옛 d1 컨테이너가 떠 있으면 먼저 내린다: (d1-ai-agent 에서) docker compose down
# 1) ai-agent-serving-infra 에서 Kafka 기동
docker compose up -d kafka kafka-ui
docker compose ps                      # kafka, zookeeper1, kafka-ui 가 Up 인지 확인 (기동에 10~30초)

# 2) d2-ai-python 에서 가상환경을 켜고 aiokafka 설치
pip install aiokafka==0.12.0

# 3) Producer: 3건 전송
python -m app.checks.kafka_producer_test

# 4) Consumer: 받기
python -m app.checks.kafka_consumer_test
```

옵션:

| 스크립트 | 옵션 | 기본값 | 설명 |
| --- | --- | --- | --- |
| producer | `--count` | 3 | 보낼 메시지 수 |
| producer | `--topic` | agent.requests | 토픽 이름 |
| consumer | `--group` | day13-test | consumer group id. 바꾸면 처음부터 다시 읽음 |
| consumer | `--max` | 3 | 받을 최대 건수 |
| consumer | `--timeout` | 15 | 새 메시지를 기다리는 최대 초 |
| 공통 | 환경변수 `KAFKA_BOOTSTRAP` | localhost:9092 | 접속 주소 |

kafka-ui(`http://localhost:8090`)에서 토픽 `agent.requests`, 메시지 내용, consumer group 의 offset 을 눈으로 확인할 수 있다.

## 6. 실행하며 확인할 개념

| 개념 | 확인 방법 |
| --- | --- |
| 토픽 자동 생성 | 토픽을 만들지 않았는데 Producer 가 보내면 생긴다 (`AUTO_CREATE_TOPICS_ENABLE`). 이때 파티션은 1개 |
| partition / offset | Producer 출력에서 offset 이 0, 1, 2 로 늘어난다 |
| 읽은 위치 저장 | Consumer 를 같은 `--group` 으로 다시 실행하면 이미 받은 건은 안 오고 새 건만 온다 |
| group 이 다르면 | `--group g2` 로 실행하면 처음부터 다시 받는다 (그룹마다 offset 을 따로 저장) |
| earliest | `auto_offset_reset="earliest"` 라서 새 group 은 토픽의 처음부터 읽는다 |
| key | 같은 key 는 같은 파티션. 파티션이 1개라 지금은 차이가 안 보이고 Day 18 에서 의미가 생긴다 |

## 6-1. 추가 확인 명령어 (그룹, 데이터 폴더, kafka-ui)

Producer 와 Consumer 가 동작하는 것은 확인했다. 아래는 그룹별 책갈피, 데이터 폴더, UI 표시를 확인하는 명령어다. 개념은 [CONCEPTS_KAFKA.md](CONCEPTS_KAFKA.md) 를 본다. 터미널 위치는 각 항목에 적었다.

### ① 다른 그룹으로 처음부터 읽기 (`d2-ai-python`, 가상환경 켠 상태)

```bash
python -m app.checks.kafka_consumer_test --group g2 --max 100 --timeout 10
```

| 기대 | 의미 |
| --- | --- |
| offset 0 부터 끝까지 전부 수신, 마지막에 `총 N건 수신` | 그룹마다 책갈피가 따로. `day13-test` 는 이미 다 읽었지만 `g2` 는 처음부터 읽음 |

`--max 100` 이라 한 번에 다 받고, 더 없으면 10초 기다린 뒤 끝난다.

### ② 같은 그룹(g2)으로 한 번 더 실행

```bash
python -m app.checks.kafka_consumer_test --group g2 --max 100 --timeout 10
```

| 기대 | 의미 |
| --- | --- |
| `시간 초과, 새 메시지 없음` | `g2` 의 책갈피도 저장되어 이어서 읽음 |

### ③ 데이터 폴더 확인 (`ai-agent-serving-infra`)

```bash
ls docker-data/kafka
ls docker-data/zookeeper-data
```

| 기대 | 의미 |
| --- | --- |
| 파일이나 폴더가 보임 | 바인드 마운트가 걸려 데이터가 호스트 폴더에 저장됨 |
| 비어 있음 | 마운트가 안 걸린 것. 컨테이너 설정 확인 필요 |

관찰된 결과(Day 13): `docker-data/kafka` 에 `__consumer_offsets-0~49`, `agent.requests-0`, `meta.properties`, 체크포인트 파일이 있었고 `docker-data/zookeeper-data` 에 `version-2` 가 있었다. 자세한 해석은 [DAY13_RESULT.md](DAY13_RESULT.md) 3-7.

### ④ kafka-ui 에서 확인 (브라우저 `http://localhost:8090`)

| 메뉴 | 볼 것 |
| --- | --- |
| Topics → `agent.requests` → Messages | offset 0 부터 끝까지 메시지가 모두 보이는지 (읽은 메시지도 남아 있음) |
| Consumers | `day13-test`, `g2` 두 그룹이 보이는지 |
| Consumers → 그룹 클릭 | Current offset 과 End offset 이 같고 Lag 이 0 인지 |

관찰된 표시 (Day 13 실행, kafka-ui v0.7.2): 목록 화면의 Consumer Lag 은 안 읽은 메시지가 있으면 숫자(예: 3), 다 읽으면 `N/A` 로 보였다. 둘 다 State 는 EMPTY, Members 는 0 이었다. 상세 화면에서 Current / End offset 으로 확정한다.

### ⑤ (선택) 실행 중인 Consumer 를 UI 에서 보기

터미널 두 개를 쓴다.

```bash
# 터미널 B (d2-ai-python): 60초 동안 대기하는 Consumer
python -m app.checks.kafka_consumer_test --group live --max 100 --timeout 60
```

```bash
# 터미널 A (d2-ai-python): Consumer 가 켜져 있는 동안 Producer 실행
python -m app.checks.kafka_producer_test --count 2
```

| 기대 | 의미 |
| --- | --- |
| 터미널 B 에 쌓여 있던 메시지가 출력되고 이어서 새 2건도 출력 | Producer 와 Consumer 가 따로 동작함. 새 그룹 `live` 라 처음부터 읽음 |
| Consumer 가 켜져 있는 60초 동안 Consumers 목록에서 `live` 가 STABLE, Members 1 | 실행 중인 Consumer 는 Members 로 보임 |

### 결과 기록

위 명령어의 터미널 출력과 UI 화면은 [DAY13_RESULT.md](DAY13_RESULT.md) 에 정리한다.

## 7. 알려진 한계와 주의

| 항목 | 내용 |
| --- | --- |
| 단일 브로커, 파티션 1개 | 복제와 병렬 소비는 실험하지 않는다. 멀티 컨슈머는 Day 18 에서 파티션을 늘려 다룬다 |
| 에이전트 미연결 | Consumer 는 출력만 한다. 에이전트 호출은 Day 16 |
| `aiokafka==0.12.0` | 버전은 계획 단계에서 정한 값이다. 설치가 안 되거나 충돌하면 `pip install aiokafka` 로 설치한 뒤 `pip freeze | grep aiokafka` 의 값으로 고친다 |
| 코드 검증 | 문법 확인만 했고 실제 Kafka 에 붙여 실행하지는 않았다 |

## 8. 체크리스트

- [x] 옛 d1 컨테이너를 내림
- [x] `docker compose up -d kafka kafka-ui` 후 컨테이너가 Up (kafka-ui 접속과 Producer/Consumer 연결로 확인)
- [x] `docker-data/kafka`, `docker-data/zookeeper-*` 에 파일이 생김 (6-1 ③)
- [x] `pip install aiokafka==0.12.0` 성공
- [x] Producer 전송, offset 이 증가하며 출력
- [x] Consumer 수신
- [x] Consumer 재실행 시 이어서 받음 (새 건 없음 -> 시간 초과 메시지)
- [x] `--group` 을 바꾸면 처음부터 다시 받음 (6-1 ①, 30건 수신)
- [x] 같은 새 그룹으로 재실행하면 새 건 없음 (6-1 ②, 0건 수신)
- [x] kafka-ui 에서 토픽, 메시지, 그룹 offset 확인 (6-1 ④). Messages 탭(offset 10~29)과 그룹 목록의 Lag 표시를 확인함. 그룹 상세 화면(Current = End, Lag 0)은 확인하지 않음
- [ ] (선택, 하지 않음) 실행 중 Consumer 를 UI 에서 STABLE 로 확인 (6-1 ⑤)
- [x] 결과를 `DAY13_RESULT.md` 에 기록
