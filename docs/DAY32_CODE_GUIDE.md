# Day 32 코드 가이드 — 실패한 메시지가 DLQ 에 보관되고 다시 처리되기까지

> 실행 방법과 확인 항목은 [DAY32_GUIDE.md](DAY32_GUIDE.md). 이 문서는 **코드가 어떤 순서로 무엇을 하는지**를 파일 위치와 함수 단위로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다. 괄호의 숫자는 작성 시점(2026-10-18)의 줄이다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, 9092 = Kafka(호스트) / 29092(컨테이너 안), 6379 = Redis, 6333 = Qdrant.
> 상태: 코드를 읽고 설명한 문서다. 가짜 producer 로 하는 확인(`dlq_check`)은 샌드박스에서 통과했다. 실제 Kafka 로 하는 실행은 [DAY32_GUIDE.md](DAY32_GUIDE.md) 5절의 ②③④ 이고, 결과는 `DAY32_RESULT.md` 에 따로 기록한다.

**목적:** Day 16 에서 "처리 실패한 메시지는 건너뛴다"까지 만들었다. 건너뛴 메시지는 로그 한 줄 외에 남지 않았다. Day 32 는 건너뛰는 메시지를 `agent.requests.dlq` 토픽에 원인과 함께 보관하고, 원인을 고친 뒤 다시 처리할 수 있게 한다.

**Day 32 의 코드 변경:** `kafka_worker.py`(수정), `metrics.py`(수정, 지표 1개), 새 파일 `dlq_tool.py`(확인과 재처리 명령), `dlq_check.py`(가짜 producer 로 하는 확인).


## 1. 전체 흐름

```
Kafka agent.requests ──▶ 워커 _run 의 async for ──▶ _handle(record)
                                                    │
                       ┌────────────────────────────┼────────────────────────────────┐
                       │ JSON/형식이 깨짐            │ 에이전트(handler)가 예외         │ 성공
                       ▼                            ▼                                ▼
          _send_to_dlq(reason="parse_error")  _send_to_dlq(reason="handler_error")   received += 1
                       │ (broker 응답까지 기다림)    │
                       └──────────────┬─────────────┘
                                      ▼
                          consumer.commit()   ← DLQ 에 보관된 뒤에야 commit

운영자:  python -m app.checks.dlq_tool list            DLQ 내용 표로 보기
         python -m app.checks.dlq_tool replay ...      handler_error 를 agent.requests 로 다시 보냄
```

| 단계 | 하는 일                                                  | 파일                                              | 함수 (줄)                               |
| ---- | -------------------------------------------------------- | ------------------------------------------------- | --------------------------------------- |
| A    | DLQ 설정 (토픽 이름, 켜고 끄기)                          | `d2-ai-python/app/services/kafka_worker.py`       | `DLQ_TOPIC`, `DLQ_ENABLED` (61~62)      |
| B    | producer 를 consumer 와 같은 수명으로 열고 닫기          | `d2-ai-python/app/services/kafka_worker.py`       | `_run` (149)                            |
| C    | DLQ 로 보내기(봉투 만들기, 전송, 지표)                   | `d2-ai-python/app/services/kafka_worker.py`       | `_send_to_dlq` (221)                    |
| D    | 보내는 두 지점                                           | `d2-ai-python/app/services/kafka_worker.py`       | `_handle` (250)                         |
| E    | DLQ 지표                                                 | `d2-ai-python/app/services/metrics.py`            | `DLQ`                                   |
| F    | DLQ 읽기, 재처리 대상 고르기, 재전송                     | `d2-ai-python/app/checks/dlq_tool.py`           | `read_all`, `parse_entry`, `select_replayable`, `build_replay_value`, `cmd_list`, `cmd_replay` |
| G    | 가짜 producer 로 하는 확인 (항목 A~K)                    | `d2-ai-python/app/checks/dlq_check.py`          | `main`                                  |


## 2. 주요 코드

### 2-1. 설정과 producer 수명 (A, B)

**목적:** DLQ 로 보낼 producer 를 워커가 Kafka 에 접속해 있는 동안만 들고 있게 한다.

```python
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "agent.requests.dlq")
DLQ_ENABLED = os.getenv("KAFKA_DLQ_ENABLED", "true").lower() == "true"
...
# _run 의 접속 루프 안
producer = None
try:
    await consumer.start()
    if DLQ_ENABLED:
        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
        await producer.start()
        self._producer = producer
    self.state = "running"
    async for record in consumer:
        await self._handle(record)
        ...commit...
finally:
    self._producer = None
    if producer is not None:
        await producer.stop()      # 오류는 무시하고 로그만
    await consumer.stop()
```

| 항목                | 내용                                                                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_DLQ_ENABLED` | `false` 면 producer 를 만들지 않고 `_send_to_dlq` 가 아무것도 하지 않는다(Day 31 까지의 동작과 같다)                                   |
| 수명                | consumer 와 같은 접속 수명이다. 접속이 끊겨 `_run` 이 재접속하면 producer 도 새로 만든다. producer 시작에 실패하면 consumer 와 함께 재접속 루프로 간다 |
| `self._producer`    | 접속 중에만 값이 있다. `finally` 에서 `None` 으로 되돌리고 닫는다                                                                       |
| `self.dlq_sent`     | 지금까지 DLQ 에 보관한 건수. `snapshot()` 의 `dlq_topic`(꺼져 있으면 null), `dlq_sent` 로 `/health` 에서 볼 수 있다                                       |

### 2-2. _send_to_dlq: 봉투 만들기와 전송 (C)

**목적:** 원본 메시지를 그대로 담고, 왜 실패했는지와 어디서 왔는지를 붙여 DLQ 토픽에 보낸다.

```python
async def _send_to_dlq(self, record, reason, error, message=None):
    if not DLQ_ENABLED: return                                           # (a)
    if self._producer is None: raise RuntimeError("DLQ producer 가 없다")  # (b)
    envelope = {
        "reason": reason,                                                # handler_error | parse_error
        "error": f"{type(error).__name__}: {error}",
        "requestId": message.requestId if message is not None else None,
        "worker": WORKER_ID,
        "failedAt": datetime.now(timezone.utc).isoformat(),
        "source": {"topic": record.topic, "partition": record.partition, "offset": record.offset},
        "originalValue": record.value.decode("utf-8", errors="replace"),  # (c)
    }
    key = record.key if record.key is not None else (message.requestId.encode("utf-8") if message is not None else None)
    await self._producer.send_and_wait(DLQ_TOPIC, value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"), key=key)   # (d)
    self.dlq_sent += 1
    metrics.DLQ.labels(reason=reason).inc()                              # (e)
```

| 표시 | 하는 일                                                                                                                                                                       |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a)  | 기능이 꺼져 있으면 보관 없이 반환한다                                                                                                                                         |
| (b)  | 켜져 있는데 producer 가 없으면 **예외를 던진다**. 조용히 넘어가면 메시지를 잃기 때문이다                                                                                      |
| (c)  | 원본을 문자열로 바꿔 담는다. UTF-8 이 아닌 바이트는 대체 문자로 바뀐다(`errors="replace"`). 원본 바이트를 보존하는 방식이 아니라 사람이 읽는 용도다                              |
| (d)  | `send_and_wait` 로 브로커가 받았다는 응답까지 기다린다. 이 줄이 끝나야 다음으로 간다                                                                                          |
| (e)  | 건수와 지표는 **전송에 성공한 뒤에** 센다. 실패하면 늘지 않는다                                                                                                                |

| 봉투 필드         | 의미                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------ |
| `reason`          | `handler_error`(에이전트 처리 실패) / `parse_error`(메시지를 해석하지 못함)                |
| `error`           | `예외 이름: 메시지`                                                                        |
| `requestId`       | 해석에 성공한 메시지만 값이 있다. `parse_error` 는 null                                    |
| `worker`          | 실패한 워커 이름(`KAFKA_WORKER_ID`)                                                        |
| `failedAt`        | 실패 시각(UTC, ISO 8601)                                                                   |
| `source`          | 원래 토픽, 파티션, offset. 원본을 추적하는 주소                                            |
| `originalValue`   | 원본 메시지 문자열                                                                         |

DLQ 메시지의 키는 원래 메시지의 키를 쓰고, 없으면 `requestId` 를 쓰며, 둘 다 없으면 키 없이 보낸다.

### 2-3. _handle: 보내는 두 지점 (D)

```python
try:
    message = AgentRequestMessage.model_validate(json.loads(record.value.decode("utf-8")))
except (ValueError, ValidationError, UnicodeDecodeError) as e:
    self.skipped += 1
    metrics.MESSAGES.labels(result="skipped").inc()
    await self._send_to_dlq(record, "parse_error", e)                    # 지점 1
    return
...
try:
    await self._handler(message, record)
    self.received += 1
except asyncio.CancelledError:
    raise                                                                # 종료는 DLQ 로 보내지 않는다
except Exception as e:                                                   # (Day 32: e 를 받는다)
    self.failed += 1
    metrics.MESSAGES.labels(result="failed").inc()
    await self._send_to_dlq(record, "handler_error", e, message)         # 지점 2
finally:
    self.current_request_id = None
```

| 지점 | 상황                                                       | 보관되는 reason  |
| ---- | ---------------------------------------------------------- | ---------------- |
| 1    | JSON 이 깨졌거나 필수 필드가 없거나 UTF-8 이 아님          | `parse_error`    |
| 2    | 에이전트 호출이 예외로 끝남(LLM 오류, 타임아웃 등)         | `handler_error`  |
| 제외 | `asyncio.CancelledError`(서버 종료)                        | 보내지 않고 그대로 올라감 |

도구 실패(Day 31)는 에이전트 안에서 처리되어 정상 응답이 되므로 이 경로로 오지 않는다.

### 2-4. commit 순서 (B 의 `async for` 안)

`_handle` 은 DLQ 전송이 끝난 뒤에야 반환하고, 그 뒤에 `consumer.commit()` 을 부른다. 즉 순서가 **DLQ 보관 → commit** 이다. 순서를 바꿔 commit 을 먼저 하면 그 사이에 워커가 죽을 때 메시지가 원래 토픽에서도 DLQ 에서도 사라진다.

### 2-5. dlq_tool: 보기와 다시 처리 (F)

**목적:** DLQ 내용을 보고, 원인을 고친 뒤 실패한 요청을 다시 `agent.requests` 로 보낸다.

| 함수                    | 내용                                                                                                                                                                         |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `read_all`              | 그룹 없이(`group_id=None`) 읽기 위치를 저장하지 않고 DLQ 를 처음부터 끝(`end_offsets`)까지 읽는다. 토픽이 없으면 빈 목록. 읽어도 DLQ 에서 사라지지 않는다                     |
| `parse_entry`           | 메시지 바이트를 사전으로 바꾸고 DLQ 안의 위치(`dlqOffset`)를 붙인다. JSON 이 아니면 `reason` 을 `unknown` 으로 둔다                                                           |
| `select_replayable`     | 다시 보낼 것을 고른다. 조건은 `reason == "handler_error"` 이고 `requestId` 가 있는 것뿐이다. `--request-id` 면 그 요청만, `--all` 이면 가능한 전부, 둘 다 없으면 아무것도 고르지 않는다 |
| `build_replay_value`    | `originalValue` 를 JSON 으로 읽어 다시 보낼 바이트로 만든다. `--provider` 를 주면 그 필드만 바꾼다                                                                           |
| `cmd_list`              | 항목을 표로 출력한다                                                                                                                                                         |
| `cmd_replay`            | 고른 항목을 `agent.requests` 로 `send_and_wait`(키는 requestId)로 보낸다. `--dry-run` 이면 대상만 출력한다                                                                    |

```
python -m app.checks.dlq_tool list
python -m app.checks.dlq_tool replay --request-id <requestId> [--provider gemini] [--dry-run]
python -m app.checks.dlq_tool replay --all
```

**재처리 규칙:**

- `parse_error` 는 다시 보내지 않는다. 원본이 깨져 있어 다시 보내도 같은 곳에서 또 실패한다.
- 같은 `requestId` 로 다시 보내므로 결과 저장소(Redis)의 같은 키가 덮어써진다.
- Kafka 토픽은 지워지지 않으므로 `replay` 를 두 번 하면 두 번 보낸다.


## 3. 시간 순서

### 3-1. 처리 실패한 요청 (실험 ② 의 요청 1: provider 가 잘못됨)

```
1. Spring 이 agent.requests 에 메시지 (provider="no-such-provider")
2. 워커 _handle: 해석 성공 → handler 호출 → ValueError("지원하지 않는 provider입니다...")
3. except Exception: failed += 1, MESSAGES{failed}+1
4. _send_to_dlq(record, "handler_error", e, message)
     봉투 구성 → send_and_wait → 브로커 응답 → dlq_sent += 1, DLQ{handler_error}+1
5. _handle 반환 → consumer.commit()
6. 같은 파티션의 다음 메시지(요청 2)는 막히지 않고 정상 처리
7. 운영자: dlq_tool list 로 확인, 원인(provider)을 고쳐 replay --provider fake-agent
```

### 3-2. DLQ 전송이 실패할 때

```
4'. send_and_wait 가 예외 (DLQ 토픽/브로커 문제)
5'. 예외가 _handle 을 빠져나가 async for 를 벗어남 → _run 의 except Exception: state=error
6'. finally: producer, consumer 닫음 → RETRY_SEC 뒤 재접속
7'. 이 메시지는 commit 되지 않았으므로 재접속한 워커가 같은 offset 부터 다시 읽어 처리
```


## 4. 실패 지점별 동작

| 상황                                          | 동작                                                                                                                       |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| 처리 실패                                     | DLQ 에 `handler_error` 보관 → commit                                                                                       |
| 해석 불가(깨진 JSON, 필수 필드 없음)          | DLQ 에 `parse_error` 보관 → commit                                                                                         |
| UTF-8 이 아닌 바이트                          | 대체 문자로 바꿔 `parse_error` 로 보관                                                                                     |
| DLQ 전송 실패                                 | 예외가 올라가 워커가 재접속한다. 메시지는 commit 전이라 다시 읽혀 **중복 처리될 수 있다**. 그동안 `failed` 건수는 처리 시도마다 늘 수 있다 |
| 서버 종료(`CancelledError`)                   | DLQ 로 보내지 않고 그대로 올라간다. commit 도 하지 않아 다음 기동 때 다시 처리                                              |
| `KAFKA_DLQ_ENABLED=false`                     | 보관 없이 건너뛴다(Day 31 까지와 같다)                                                                                      |
| producer 가 없는데 켜져 있음                  | `RuntimeError` 로 올라간다(조용히 잃지 않는다)                                                                              |
| DLQ 토픽이 없음                               | 브로커의 자동 생성 설정에 따라 다르다. 실험 전에 파티션 1개로 직접 만든다(가이드 4절)                                         |
| DLQ 에 해석할 수 없는 메시지가 섞임           | `parse_entry` 가 `reason=unknown` 으로 표시한다                                                                            |


## 5. 설계 선택과 이유

| 선택                                              | 이유                                                                                                                              |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| DLQ 보관을 commit 보다 먼저, 응답까지 기다림      | 보관이 확인되기 전에 commit 하면 워커가 그 사이에 죽을 때 메시지를 잃는다. 대신 보관 직후 commit 전에 죽으면 같은 메시지가 DLQ 에 두 번 들어갈 수 있다(잃는 것보다 중복이 낫다는 선택) |
| 전송 실패를 삼키지 않고 예외로 올림               | 보관하지 못한 채 건너뛰면 DLQ 를 만든 의미가 없다. 재접속 뒤 다시 처리하게 두는 쪽이 안전하다                                        |
| `parse_error` 는 재처리하지 않음                  | 원본이 깨져 있어 다시 보내도 결과가 같다. 사람이 보고 판단할 대상이다                                                              |
| 재처리를 자동이 아닌 명령(`dlq_tool`)으로         | 처리 실패의 원인(provider, 키, 장애)을 고치기 전에 자동으로 다시 보내면 같은 실패가 반복된다                                        |
| 재처리 때 같은 `requestId` 를 유지                | 결과 조회 주소가 그대로이고 Redis 의 결과가 덮어써진다. 요청자는 같은 번호로 계속 조회하면 된다                                    |
| 봉투에 원본을 문자열로 그대로 담음                | 재처리가 원본 메시지를 그대로 되살릴 수 있어야 한다                                                                              |
| DLQ 토픽 파티션 1개                               | 실험 규모에서는 순서와 읽기 단순함이 중요하고 처리량은 필요 없다                                                                  |
| `CancelledError` 는 DLQ 로 보내지 않음            | 서버 종료는 메시지의 잘못이 아니다. 다시 기동해 처리해야 한다                                                                      |
| 도구 실패는 DLQ 대상이 아님                       | Day 31 에서 에이전트 안에서 처리하고 정상 응답으로 끝낸다. 워커 실패와 구분한다                                                    |


## 6. 코드를 따라 읽는 순서

1. `kafka_worker.py` 의 `DLQ_TOPIC`, `DLQ_ENABLED`, `__init__`(`dlq_sent`, `_producer`)
2. `_run` 의 producer 시작/종료와 commit 위치
3. `_send_to_dlq`
4. `_handle` 의 두 호출 지점
5. `dlq_tool.py` 의 `parse_entry` → `select_replayable` → `build_replay_value` → `read_all` → `cmd_replay`
6. `dlq_check.py` 의 항목 B(처리 실패), E(전송 실패), G(종료)
7. [DAY32_GUIDE.md](DAY32_GUIDE.md) 5절 ②③④ 로 실제 Kafka 에서 확인
