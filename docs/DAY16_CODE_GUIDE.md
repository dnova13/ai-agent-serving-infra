# Day 16 코드 가이드 — Kafka 메시지를 에이전트에 연결하고 처리 후 수동 commit

> 실행하는 방법과 확인 항목은 [DAY16_GUIDE.md](DAY16_GUIDE.md), 결과는 [DAY16_RESULT.md](DAY16_RESULT.md). 이 문서는 메시지가 에이전트를 거쳐 commit 되기까지의 순서를 설명한다.
> 파일 경로는 `d2-ai-python/app/` 기준이다. 줄 번호는 적지 않았고 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **9092 = Kafka**, **8000 = FastAPI 워커 1**, 6333 = Qdrant(검색 도구).
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 설명한 코드는 Day 16 커밋(`58a2f1d`) 시점의 것이다.

**전체 목적:** Day 15 의 "받았다는 로그만 남기는 handler" 를 실제 에이전트 호출로 바꾸고, 처리가 끝난 뒤에만 읽은 위치(offset)를 저장해서(수동 commit) 처리 도중 서버가 죽어도 요청이 사라지지 않게 하는 구조를 코드 순서대로 따라간다.


## 1. 전체 흐름

```
Kafka 메시지 → KafkaWorker._run (async for record)
   └─ _handle(record)
        ① JSON 해석 실패 → skipped += 1, return
        ② current_request_id = requestId
        ③ await agent_handler(message, record)
              asyncio.wait_for(asyncio.to_thread(run_agent, question, provider), timeout=AGENT_TIMEOUT_SEC)
        ④ 성공 received += 1 / 예외 failed += 1 (로그 남기고 건너뜀) / CancelledError 는 위로
        ⑤ finally: current_request_id = None
   └─ (_handle 이 끝난 뒤) COMMIT_MODE == "manual" 이면 await consumer.commit(), committed += 1
```

| 단계 | 하는 일                      | 파일                                              | 함수                                  |
| ---- | ---------------------------- | ------------------------------------------------- | ------------------------------------- |
| A    | 에이전트를 부르는 handler    | `d2-ai-python/app/services/agent_handler.py`      | `agent_handler`                       |
| B    | 실패 처리와 commit           | `d2-ai-python/app/services/kafka_worker.py`       | `_run`, `_handle`, `snapshot`         |
| C    | handler 교체                 | `d2-ai-python/app/main.py`                        | `lifespan` 에서 `KafkaWorker(handler=agent_handler)` |


## 2. 주요 코드

### 2-1. 에이전트 호출 (`agent_handler`)

```python
answer = await asyncio.wait_for(
    asyncio.to_thread(run_agent, message.question, message.provider),
    timeout=AGENT_TIMEOUT_SEC,
)
```

| 부분                            | 설명                                                                                                                                  |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `asyncio.to_thread`             | `run_agent` 는 LLM 응답을 기다리며 스레드를 막는 동기 함수라서, 이벤트 루프에서 직접 부르면 그동안 `/health` 같은 HTTP 요청도 멈춘다. 별도 스레드에서 실행한다 |
| `wait_for(..., AGENT_TIMEOUT_SEC)` | 기본 120초. 넘으면 실패로 처리하고 다음 메시지로 넘어간다                                                                             |
| 타임아웃의 한계                 | 코드 주석: 타임아웃이 나도 스레드 안의 호출 자체는 멈추지 못하고 결과만 버려진다                                                        |
| `AGENT_DEBUG_DELAY_SEC`         | 실험용(기본 0). 에이전트를 부르기 전에 이만큼 기다려 "처리 중" 상태를 길게 만들어서 그 사이에 서버를 끄는 실험을 하려는 것이다            |
| 결과는 로그로만                  | 이 Day 에는 결과를 저장하지 않는다. 시작과 완료 로그(`[agent] 시작`, `[agent] 완료 ... elapsed_ms=... answer=앞 200자`)만 남긴다. 저장은 Day 17 |

### 2-2. 실패 처리 (`_handle`)

| 부분                              | 설명                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 일반 예외를 잡아 `failed += 1`     | LLM 오류나 타임아웃이 나도 워커가 재접속 루프로 빠지지 않게 여기서 잡는다. 실패한 메시지는 건너뛴다. 계속 재시도하면 이 메시지 하나 때문에 뒤의 요청이 모두 막히기 때문이다 |
| `CancelledError` 는 다시 올림       | 서버 종료는 여기서 잡지 않는다                                                                                                         |
| 실패한 요청의 보관                  | 코드 주석: 실패 요청을 따로 보관하는 DLQ 는 Day 32 에서 한다                                                                           |
| `current_request_id`               | 처리 중인 요청 id(없으면 None). `/health` 에서 "지금 무엇을 처리 중인지" 보려는 것                                                      |

### 2-3. commit 방식 (`_run`)

```python
enable_auto_commit=(COMMIT_MODE == "auto"),
...
await self._handle(record)
if COMMIT_MODE == "manual":
    await consumer.commit()
    self.committed += 1
```

| 부분                        | 설명                                                                                                                                  |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `[OLD]` 자동 commit          | Day 13, 15: 읽은 위치가 주기적으로 자동 저장된다. 처리 도중 서버가 죽어도 이미 읽은 것으로 기록될 수 있다                                |
| `[NEW]` 수동 commit(기본)    | 처리(성공, 실패로 건너뜀, 해석 불가로 건너뜀 모두)가 끝난 뒤에 이 메시지까지 commit 한다. 처리 중에 서버가 꺼지면 commit 에 닿지 못해, 다음에 같은 메시지를 다시 받는다 |
| `KAFKA_COMMIT_MODE`         | `manual`(기본) 또는 `auto`. 두 방식을 비교하는 실험을 하려고 환경변수로 뺐다                                                              |
| `OFFSET_RESET` 이 `latest` 로 | `[OLD]` earliest. 에이전트(LLM)를 붙이면 새 그룹이 토픽 전체를 읽는 순간 옛 메시지마다 LLM 호출이 나가므로 바꿨다. 이미 commit 된 위치가 있는 그룹(`agent-workers`)은 이 값과 상관없이 저장된 위치부터 이어서 읽는다(코드 주석) |
| `snapshot()` 에 추가된 값    | `failed`, `committed`, `commit_mode`, `current_request_id`                                                                             |

수동 commit 은 "적어도 한 번(at-least-once)" 처리다. 처리는 끝났는데 commit 직전에 죽으면 같은 메시지를 다시 처리한다. 이 문서는 그 중복 처리를 막는 장치(멱등성)는 이 Day 에 없다는 점만 적는다.


## 3. 결과를 읽는 순서

1. `/health` 의 `kafka_consumer` 에서 `received`, `failed`, `committed`, `commit_mode`, `current_request_id` 를 본다.
2. 서버 로그에서 같은 `requestId` 의 `[agent] 시작` 과 `[agent] 완료` 가 쌍으로 나오는지, `elapsed_ms` 가 LLM 시간(수 초)인지 본다.
3. 처리 도중 서버를 껐다 켰을 때 같은 메시지가 다시 처리되는지(manual), 자동 commit 에서는 어떻게 다른지 본다(DAY16_RESULT 의 비교).


## 4. 확인하지 않은 것

| 항목                     | 내용                                                                                                                                  |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                     | 이 문서를 쓰면서 서버나 Kafka, LLM 을 실행하지 않았다. 실험 결과는 DAY16_RESULT 를 따른다                                               |
| 현재 코드와의 차이       | 이후 Day 에 `agent_handler` 는 결과 저장(Day 17), 워커 ID(Day 18), 대화 이어가기(Day 20)로 바뀌었다. `to_thread` 를 쓰는 이유는 그대로다   |
| 중복 처리                | 수동 commit 의 재처리로 같은 요청이 두 번 실행될 수 있다. 이를 막는 장치는 이 Day 에 없다                                                |
