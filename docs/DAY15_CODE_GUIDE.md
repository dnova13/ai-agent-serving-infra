# Day 15 코드 가이드 — FastAPI 서버 안에서 Kafka Consumer 워커 돌리기

> 실행하는 방법과 확인 항목은 [DAY15_GUIDE.md](DAY15_GUIDE.md), 결과는 [DAY15_RESULT.md](DAY15_RESULT.md). 이 문서는 서버가 켜질 때 워커가 시작되고 메시지를 받는 순서를 설명한다.
> 파일 경로는 `d2-ai-python/app/` 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **8000 = FastAPI 워커 1**, **9092 = Kafka**, 8080 = Spring WAS(메시지를 넣는 쪽).
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 15 커밋(`b39badd`) 시점의 것이며, 이 Day 의 handler 는 받았다는 로그만 남기고 에이전트는 부르지 않는다.

**전체 목적:** Spring WAS 가 `agent.requests` 토픽에 넣은 메시지를 FastAPI 서버가 백그라운드 태스크로 받는 구조(lifespan 으로 시작과 종료, Kafka 가 꺼져도 서버는 뜨는 재접속 루프, 해석할 수 없는 메시지 건너뛰기, `/health` 상태 노출)를 코드 순서대로 따라간다.


## 1. 전체 흐름

```
uvicorn 시작 → main.py lifespan (yield 앞)
   ├─ kafka_worker.ENABLED 면 KafkaWorker() 생성, app.state.kafka_worker 에 보관
   └─ await worker.start()  → asyncio.create_task(_run) 만 하고 바로 돌아옴 (Kafka 접속을 기다리지 않음)

_run (백그라운드 태스크)
   while True:
     consumer.start() → state=running → async for record in consumer: _handle(record)
     접속 실패(KafkaConnectionError, OSError) → state=error, last_error 기록, RETRY_SEC(5초) 뒤 다시
     finally: consumer.stop()

_handle(record)
   JSON 해석 + AgentRequestMessage 검증 → 실패하면 skipped += 1, 경고 로그 후 return
   성공하면 handler(log_only_handler) 호출 → received += 1

서버 종료 → lifespan (yield 뒤) worker.stop() → 태스크 취소 → finally 에서 consumer.stop() (그룹에서 정상 탈퇴)
GET /health → {"status":"ok","kafka_consumer": worker.snapshot()}
```

| 단계 | 하는 일                         | 파일                                              | 함수 / 클래스                                   |
| ---- | ------------------------------- | ------------------------------------------------- | ----------------------------------------------- |
| A    | 워커 본체                       | `d2-ai-python/app/services/kafka_worker.py`       | `KafkaWorker` (`start`, `stop`, `snapshot`, `_run`, `_handle`), `log_only_handler` |
| B    | 서버 시작/종료에 연결           | `d2-ai-python/app/main.py`                        | `lifespan`                                      |
| C    | 메시지 모양 검증                | `d2-ai-python/app/schemas/models.py`              | `AgentRequestMessage`                           |
| D    | 상태 노출                       | `d2-ai-python/app/routers/health.py`              | `health_check`                                  |


## 2. 주요 코드

### 2-1. 시작과 종료 (`lifespan`)

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    worker = None
    if kafka_worker.ENABLED:
        worker = kafka_worker.KafkaWorker()
        app.state.kafka_worker = worker
        await worker.start()
    yield
    if worker is not None:
        await worker.stop()
```

| 부분                              | 설명                                                                                                              |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `yield` 앞/뒤                     | 앞은 서버 시작 때, 뒤는 서버 종료 때 실행된다                                                                       |
| `start()` 는 접속을 기다리지 않음 | 태스크만 만들고 돌아오므로 Kafka 가 꺼져 있어도 서버(/health 등)는 뜬다                                              |
| `KAFKA_CONSUMER_ENABLED=false`    | 워커를 만들지 않는다. `/health` 의 `kafka_consumer` 는 null 이 된다                                                  |
| `app.state.kafka_worker`          | `/health` 가 워커 상태를 읽으려고 앱에 달아 둔다                                                                    |

### 2-2. 워커의 재접속 루프 (`_run`)

| 부분                                   | 설명                                                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `state`                                | `stopped` → `connecting` → `running`, 실패하면 `error`                                                                                  |
| `except CancelledError: raise`         | 서버 종료(`stop()` 의 cancel)는 재시도하지 않고 위로 올린다                                                                             |
| `KafkaConnectionError`, `OSError`      | `state=error`, `last_error` 를 남기고 경고 로그. 5초(`KAFKA_RETRY_SEC`) 뒤 다시 시도한다                                              |
| 그 밖의 예외                           | 같은 방식으로 `error` 로 두고 재시도(`logger.exception`)                                                                               |
| `finally: await consumer.stop()`       | 시작에 실패했어도 내부 연결을 정리한다. 정상 종료 때는 그룹에서 탈퇴해 다른 워커가 바로 이어받는다(Day 18)                              |
| `last_error` 의 이름 중복 방지          | `[OLD]` 는 `"KafkaConnectionError: KafkaConnectionError: ..."` 처럼 이름이 두 번 붙었다. `[NEW]` 는 메시지가 이미 예외 이름으로 시작하면 다시 붙이지 않는다 |
| 설정                                   | `KAFKA_BOOTSTRAP`(localhost:9092), `KAFKA_TOPIC`(agent.requests), `KAFKA_GROUP`(agent-workers), `KAFKA_OFFSET_RESET`(earliest), `KAFKA_RETRY_SEC`(5) |
| 로거 설정                              | uvicorn 은 자기 로거만 설정하고 루트 로거는 WARNING 이라 INFO 로그가 안 나온다. 그래서 `kafka_worker` 로거에 직접 INFO 와 출력 핸들러를 걸었다(`[OLD]`/`[NEW]` 주석) |

### 2-3. 메시지 처리 (`_handle`)

```python
message = AgentRequestMessage.model_validate(json.loads(record.value.decode("utf-8")))
...
await self._handler(message, record)
self.received += 1
```

| 부분                          | 설명                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 해석 불가 메시지를 건너뜀     | `ValueError`(JSON 오류와 pydantic 검증 오류 모두 포함), `UnicodeDecodeError` 이면 `skipped += 1` 후 return. 한 건 때문에 워커가 멈추면 뒤의 메시지가 모두 막히기 때문이다(파일 머리 설명) |
| `AgentRequestMessage`         | `requestId`, `question`, `provider`(기본 gemini), `createdAt`(datetime). Spring 이 보내는 `...Z` 형식의 시각을 pydantic v2 가 바로 파싱한다 |
| handler 를 바꿀 수 있게 함    | `KafkaWorker(handler=...)` 로 받는다. 이 Day 는 `log_only_handler`(수신 로그만)를 쓰고, Day 16 에서 에이전트 호출로 교체한다           |
| `snapshot()`                  | `state`, `topic`, `group`, `received`, `skipped`, `last_error` 를 돌려준다                                               |

### 2-4. `/health`

```python
return {"status": "ok", "kafka_consumer": worker.snapshot() if worker is not None else None}
```

서버가 살아 있으면 Kafka 가 꺼져 있어도 `status` 는 `ok` 이고, 워커 상태는 `kafka_consumer` 에 따로 보인다.


## 3. 결과를 읽는 순서

1. 서버 로그에서 `워커 시작 요청`, `Kafka 접속 완료, 수신 대기` 순서로 나오는지 본다.
2. Spring(8080)으로 요청을 보낸 뒤 `[recv] partition=... offset=... requestId=...` 로그가 나오는지 본다.
3. `GET /health` 의 `kafka_consumer.state` 가 `running`, `received` 가 늘어나는지 본다.
4. Kafka 를 끄고 서버를 켜면 `/health` 가 `ok` 이면서 `state` 가 `error` 인지, Kafka 를 켜면 5초 안팎에 `running` 으로 돌아오는지 본다.


## 4. 확인하지 않은 것

| 항목                  | 내용                                                                                                                                         |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                  | 이 문서를 쓰면서 서버나 Kafka 를 실행하지 않았다. 실제 로그와 캡처는 DAY15_RESULT 를 따른다                                                      |
| 현재 코드와의 차이    | Day 16 이후 `KafkaWorker` 에 `failed`, `committed`, 수동 commit 등이 더해졌고, Day 18 에서 워커 ID 와 파티션 정보가 추가됐다. 이 문서는 Day 15 커밋 시점이다 |
| `OFFSET_RESET`        | 이 Day 는 `earliest` 였고 Day 16 에서 `latest` 로 바뀐다(DAY16_CODE_GUIDE 2-3)                                                                  |
