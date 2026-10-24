# Day 15 가이드 — FastAPI lifespan 에 Kafka Consumer 연동

> 상태: 완료(2026-10-11), 실행 결과는 [DAY15_RESULT.md](DAY15_RESULT.md). 아래는 작성 당시 안내다. 로그 출력 설정은 실행하며 고쳤다(RESULT 4장).

## 1. 오늘 목표

FastAPI 서버가 켜질 때 Kafka Consumer 가 같이 켜지고, 꺼질 때 같이 꺼지게 한다. Day 14 의 Spring WAS 가 토픽에 넣은 요청을 FastAPI 쪽에서 받는 것까지 확인한다. **에이전트는 아직 호출하지 않고 받았다는 로그만 남긴다**(에이전트 연결은 Day 16).

```
[클라이언트] -POST-> [Spring WAS :8080] -> [Kafka agent.requests] -> [FastAPI :8000 의 Consumer 워커] -> 로그
                                                                         (서버가 켜지면 시작, 꺼지면 종료)
```

## 2. 만든 파일

| 파일 | 역할 |
| --- | --- |
| `app/services/kafka_worker.py` (신규) | `KafkaWorker`: 백그라운드 태스크로 Consumer 를 돌린다. 재접속, 종료 처리, 상태 노출 |
| `app/schemas/models.py` (끝에 추가) | `AgentRequestMessage`: Spring 이 보내는 JSON 의 파이썬 모델 |
| `app/main.py` (수정) | `lifespan` 에서 워커를 시작/종료. 기존 `FastAPI(...)` 는 `[OLD]` 주석으로 남김 |
| `app/routers/health.py` (수정) | `/health` 에 `kafka_consumer` 상태 추가. 기존 코드는 `[OLD]` 주석으로 남김 |

새로 설치할 패키지는 없다(`aiokafka` 는 Day 13 에서 설치).

## 3. 동작 방식

| 시점 | 일어나는 일 |
| --- | --- |
| 서버 시작 | `lifespan` 의 `yield` 앞에서 `KafkaWorker.start()` 호출. 태스크만 만들고 **Kafka 접속을 기다리지 않고** 바로 반환 |
| 워커 태스크 | Kafka 접속 시도 → 성공하면 `state=running` 으로 메시지 대기 → 한 건씩 JSON 해석 후 handler 호출 |
| Kafka 접속 실패/끊김 | `state=error`, 경고 로그, `KAFKA_RETRY_SEC`(기본 5초) 뒤 재시도. 서버는 계속 동작 |
| 해석 못 하는 메시지 | 로그만 남기고 건너뜀(`skipped` 증가). 한 건 때문에 워커가 멈추지 않게 함 |
| 서버 종료 | `yield` 뒤에서 `KafkaWorker.stop()`: 태스크 취소 → `consumer.stop()` 으로 그룹에서 정상 탈퇴 |

### 환경변수 (모두 선택)

| 이름 | 기본값 | 의미 |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | 브로커 주소 (Day 13 과 같음) |
| `KAFKA_TOPIC` | `agent.requests` | 토픽 |
| `KAFKA_GROUP` | `agent-workers` | 컨슈머 그룹. Day 13 의 `day13-test`, Day 14 의 `day14` 와 다른 새 그룹 |
| `KAFKA_OFFSET_RESET` | `earliest` | 이 그룹이 처음일 때 어디서부터 읽을지 |
| `KAFKA_RETRY_SEC` | `5` | 접속 실패 후 재시도 간격(초) |
| `KAFKA_CONSUMER_ENABLED` | `true` | `false` 면 워커를 만들지 않는다 (Kafka 없이 다른 API 만 쓸 때) |

## 4. 설계 판단

| 항목 | 선택 | 이유와 대가 |
| --- | --- | --- |
| 워커 실행 방식 | 같은 프로세스의 asyncio 백그라운드 태스크 | 구성이 단순하다. 대가: 에이전트 처리가 오래 걸리면(Day 16) 같은 이벤트 루프를 쓰는 HTTP 요청에 영향을 줄 수 있다. 워커를 별도 프로세스로 분리하는 방안은 Day 18~ 에서 병렬 처리와 함께 검토 |
| Kafka 가 꺼져 있을 때 | 서버는 기동, 워커는 재시도 | 서버 기동이 Kafka 에 묶이지 않는다. Day 14 의 Spring 은 반대로 Kafka 가 꺼진 채 기동하면 멈췄다 |
| `/health` | `status` 는 서버 기준 `ok`, 워커 상태는 `kafka_consumer` 에 따로 | Kafka 가 꺼져도 서버 자체는 살아 있으므로 `ok`. 워커 상태를 보고 싶을 때 `state` 를 본다 |
| commit | 자동 commit (Day 13 과 동일) | 오늘은 로그만 남기므로 영향이 작다. **처리 후 commit 으로 바꿀지는 Day 16 에서 결정** |
| 메시지 모델 | pydantic `AgentRequestMessage` | `createdAt`(`2026-...Z`, 소수점 6자리)을 pydantic v2 가 바로 `datetime` 으로 파싱한다. Day 14 에서 남긴 "파이썬에서 파싱" 항목을 이것으로 해소 |
| handler | `KafkaWorker(handler=...)` 로 주입 | Day 16 에서 로그 handler 를 에이전트 호출 handler 로 교체하기 쉽게 하려고 |

## 5. 주의: 새 그룹은 토픽의 처음부터 읽는다

`agent-workers` 는 처음 쓰는 그룹이고 `KAFKA_OFFSET_RESET=earliest` 라서, 처음 켜면 토픽에 쌓인 **모든 메시지**(Day 13 의 테스트 메시지, Day 14 의 요청 포함)를 한 번에 받는다. 오늘은 로그만 남기므로 문제없다. Day 16 에서 에이전트를 붙이면 이 메시지 전부가 LLM 호출이 되므로, 그때 시작 위치와 오래된 메시지 처리 방식을 정해야 한다.

## 6. 실행과 확인

Kafka 컨테이너가 떠 있어야 한다(`ai-agent-serving-infra` 에서 `docker compose up -d kafka kafka-ui`). Spring WAS 는 ④에서 켠다.

### ① FastAPI 실행 (`d2-ai-python`, 가상환경 켠 상태)

```bash
uvicorn app.main:app --reload --port 8000
```

(또는 `d2-ai-python` 의 `./run.sh`)

기대: 로그에 `INFO:     [kafka_worker] 워커 시작 요청 topic=agent.requests group=agent-workers ...` → `Kafka 접속 완료, 수신 대기` → 토픽에 있던 메시지마다 `[recv] partition=0 offset=... requestId=... question=...`. (처음 작성할 때는 로그 설정이 빠져 INFO 로그가 안 보였고, 실행하며 고쳤다.) 서버 기동 자체는 Kafka 접속을 기다리지 않는다.

### ② 상태 확인

```bash
curl -s http://localhost:8000/health
```

기대: `{"status":"ok","kafka_consumer":{"state":"running", ... "received": N, "skipped": M ...}}`. `received` 가 ①에서 본 `[recv]` 줄 수와 같은지 본다. `skipped` 가 0 이 아니면 로그의 `해석 불가 메시지 건너뜀` 줄을 본다(Day 13 의 첫 메시지에 필드가 다를 가능성).

### ③ 재시작하면 이어서 받는지

`Ctrl+C` 로 서버를 끄고 다시 켠다. 기대: 끌 때 `워커 종료 received=... skipped=...` 가 찍히고 오류 없이 종료. 다시 켜면 이미 받은 메시지는 다시 나오지 않는다(자동 commit 으로 위치가 저장됨).

### ④ Spring → Kafka → FastAPI 연결 (Spring WAS 도 켠 상태, `d3-was-spring` 에서 `./gradlew bootRun`)

```bash
curl -i -X POST http://localhost:8080/api/agent/requests \
  -H 'Content-Type: application/json' \
  -d '{"question":"FastAPI 연결 테스트","provider":"gemini"}'
```

기대: Spring 이 `202` 와 `requestId` 를 돌려주고, 곧바로 FastAPI 로그에 같은 `requestId` 의 `[recv]` 가 찍힌다. 두 로그의 requestId 를 대조한다.

### ⑤ Kafka 없이 서버 켜기 → Kafka 켜기 (재접속 확인)

`docker compose stop kafka` 후 FastAPI 를 켠다. 기대: 서버는 기동하고 `curl -s localhost:8000/health` 가 `state: error` 와 `last_error` 를 보여 준다. 5초 간격 경고 로그가 반복된다(aiokafka 자체의 `Unable connect` 오류 로그도 같이 찍힌다). 이 상태에서 `docker compose start kafka` 하면 **서버를 다시 켜지 않아도** `state: running` 으로 바뀌는지 본다.

### ⑥ 워커 끄고 서버만 쓰기 (선택)

`KAFKA_CONSUMER_ENABLED=false uvicorn app.main:app --port 8000` 후 `/health` 의 `kafka_consumer` 가 `null` 인지 본다.

### 선택 확인 항목 (하지 않아도 Day 15 완료)

| 항목 | 방법 | 확인할 것 |
| --- | --- | --- |
| 서버가 돌던 중 Kafka 껐다 켜기 | 서버를 켠 채 `docker compose stop kafka`, 잠시 뒤 `docker compose start kafka` | 로그에 접속 실패 경고가 뜨고, 켜면 `Kafka 접속 완료` 로 돌아오는지. `/health` 의 `state` 가 `error` → `running` |
| Kafka 꺼진 동안의 `/health` | Kafka 를 끈 상태에서 `curl -s localhost:8000/health` | `state` 가 `error`, `last_error` 에 사유 |
| 워커 끄기 | `KAFKA_CONSUMER_ENABLED=false` (⑥) | `kafka_consumer` 가 `null` |
| 코드 수정 시 자동 재시작 | `--reload` 로 켠 채 `kafka_worker.py` 를 저장 | 종료 로그 후 다시 시작되는지 |
| kafka-ui 그룹 화면 | `http://localhost:8090` Consumers 에서 `agent-workers` | 상태와 Lag |

## 7. 작성 중 확인한 것과 확인하지 않은 것

| 구분 | 내용 |
| --- | --- |
| 확인함 | `aiokafka==0.12.0`, `pydantic==2.13.5` 에서 `ConsumerRecord`, `KafkaConnectionError` import 가 되고, `createdAt` 문자열이 datetime 으로 파싱됨 |
| 확인함 | Kafka 가 없는 환경에서 임시 앱(워커와 같은 lifespan 코드)을 띄웠을 때: 시작 0.02초, 재시도 반복, `/health` 200 과 `state: error`, 종료 시 `워커 종료` 로그와 정상 종료 |
| 확인하지 않음 | 실제 Kafka 에서의 수신, 재시작 후 이어받기, Kafka 를 나중에 켰을 때의 재접속, 실제 `main.py` 전체 기동(다른 라우터의 의존성 때문에 작성 환경에서 띄우지 못함) |
| 확인하지 않음 | 서버를 `--reload` 로 돌릴 때 코드 수정 시 워커 재시작 동작 |

## 8. 알려진 한계

| 항목 | 내용 |
| --- | --- |
| handler 예외 | 지금은 handler 에서 예외가 나면 워커가 재접속 루프로 들어간다. 자동 commit 이라 그 메시지가 건너뛰어질 수 있다. 실패 처리 정책은 Day 16 에서 정한다 |
| 해석 못 한 메시지 | 건너뛰기만 하고 따로 보관하지 않는다(dead letter 없음) |
| 같은 프로세스 | 에이전트 처리가 길어지면 HTTP 응답에 영향. Day 16~18 에서 확인 |
| 로그 소음 | Kafka 가 꺼져 있으면 aiokafka 가 재시도마다 오류 로그를 낸다 |
| 새 그룹 | 처음 켜면 토픽 전체를 읽는다(5장) |

## 9. 체크리스트

- [x] `uvicorn app.main:app --reload --port 8000` 기동, Kafka 접속 로그
- [x] 기존 메시지 `[recv]` 로그, `/health` 의 `received` 와 일치
- [x] `Ctrl+C` 종료 시 `워커 종료` 로그, 오류 없음
- [x] 재시작 후 이미 받은 메시지는 다시 나오지 않음
- [x] Spring → FastAPI: 같은 requestId 가 양쪽 로그에 보임
- [x] Kafka 없이 서버 기동 → 재시도 로그, Kafka 켜면 접속 완료로 복구 (`/health` 의 `error` 표시는 사용자 환경에서 확인하지 않음. 작성 환경 임시 앱에서만 확인)
- [ ] (선택) `KAFKA_CONSUMER_ENABLED=false` 에서 `kafka_consumer: null` (시험하지 않음)
- [x] 결과를 `DAY15_RESULT.md` 에 기록
