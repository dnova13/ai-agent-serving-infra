# Day 23 가이드 — 결과 저장소를 Redis 로, 전체 파이프라인 재검증

> 상태: 코드 변경과 확인 스크립트(`result_check.py`) 작성 완료. 아래 "샌드박스" 결과는 **코드를 만든 쪽의 샌드박스**(실제 `redis-server`, Kafka/Spring/Gemini 없음)에서 본 것이고, ①~⑦ 은 내 환경에서 실행해 DAY23_RESULT 3번에 기록했다(2026-10-15).
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, **6379 = Redis(redis1 컨테이너)**, 9092 = Kafka, 6333 = Qdrant.
> 앞 단계는 [DAY22_GUIDE.md](DAY22_GUIDE.md), 결과 저장소의 원래 모습은 [DAY17_GUIDE.md](DAY17_GUIDE.md).

**전체 목적:** Day 17 에서 "4주차에 Redis 로 교체한다"고 미뤄 둔 **결과 저장소**(`result_store`)를 Redis 로 옮기고, 옮긴 뒤에도 Spring(8080) → Kafka → 워커 2대(8000, 8001) → 결과 조회까지 전체 흐름이 처음부터 끝까지 동작하는지 다시 확인한다. 옮기면 달라지는 것(워커가 달라도 같은 결과가 보임, 재시작해도 남음)을 눈으로 확인하는 것이 핵심이다.


## 1. 오늘 목표

| 확인할 것                                                                                      | 실험 |
| ---------------------------------------------------------------------------------------------- | ---- |
| 결과 저장소가 Redis 에 저장, 갱신, 만료, 장애 대체를 기대대로 하는가 (스크립트)                | ①    |
| 요청 한 건의 결과가 Redis 에 어떤 키와 값, TTL 로 들어가는가                                   | ②    |
| Spring → Kafka → 워커 2대 → 결과 조회의 전체 흐름이 여전히 PASS 인가                            | ③    |
| 워커 1이 처리한 결과를 워커 2의 주소로 물어도 읽히는가 (Day 17~22 에는 404 였다)                | ④    |
| 두 워커를 껐다 켜도 결과가 남는가                                                               | ⑤    |
| Kafka 경로에서 같은 `sessionId` 로 이어 물으면 이전 대화를 이어가는가 (Day 20 의 재확인)       | ⑥    |
| (선택) Redis 가 꺼졌을 때 답이 막히지 않고 메모리로 대신하는가                                  | ⑦    |


## 2. 바뀐 것

| 파일                          | 바뀐 것                                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `services/result_store.py`    | 결과를 Redis(`agent:result:<requestId>`, JSON, TTL 기본 86400초)에 쓰고 읽는다. Redis 가 안 되면 경고만 남기고 이 프로세스의 메모리로 대신한다. 기존 줄은 `[OLD Day17]` 주석으로 보존 |
| `routers/results.py`          | `async def` 를 `def` 로 바꿨다. 저장소가 Redis(동기)를 부르므로 스레드풀에서 돌게 해서 이벤트 루프를 막지 않는다(`sessions.py` 와 같은 방식)         |
| `services/agent_handler.py`   | `store.set_processing/set_done/set_failed` 를 `asyncio.to_thread` 로 부른다(같은 이유)                                                              |
| `checks/result_check.py`    | 새 확인 스크립트(①). 실제 Redis 에 쓰고 읽어 PASS/FAIL 을 낸다                                                                                      |
| `scripts/day17_e2e.sh`        | 주석과 안내 문구만 바꿨다(로직은 그대로). "결과는 처리한 워커의 메모리에만 있다"는 설명이 더는 맞지 않아서다                                        |

결과 조회 주소(`GET /api/v1/agent/results/{requestId}`)와 응답 모양은 그대로다. 호출하는 쪽(Spring, 스크립트)은 바꿀 것이 없다.


## 3. 개념

| 구분               | Day 17~22 (메모리)                                           | Day 23 (Redis)                                                         |
| ------------------ | ------------------------------------------------------------ | ---------------------------------------------------------------------- |
| 어디에 있나        | 요청을 처리한 워커 프로세스의 메모리                         | Redis (`agent:result:<requestId>`)                                     |
| 다른 워커 주소로 조회 | 404 (그 워커 메모리에 없다)                                  | 200 (같은 Redis 를 본다)                                               |
| 서버 재시작        | 사라진다                                                     | 남는다 (TTL 안에서)                                                    |
| 오래된 결과        | 1000건을 넘으면 오래된 것부터 지움                           | TTL(기본 1일)이 지나면 Redis 가 지움. 정식 정책은 Day 24               |
| Redis 가 꺼지면    | 해당 없음                                                    | 경고만 남기고 그 프로세스 메모리로 대신한다(그 워커 주소에서만 보임)   |
| 키가 없을 때       | 메모리에 없으면 404                                          | Redis 가 되는 동안은 Redis 가 정답이다. 없으면 메모리를 보지 않고 404    |

결과를 읽을 때 Redis 가 되는데 키가 없으면 메모리를 보지 않는 이유는, 메모리 복사본으로 되살리면 TTL 이 지난 결과가 같은 워커에서만 살아 있는 이상한 상태가 되기 때문이다.


## 4. 준비

| 항목             | 방법                                                                                                                              |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 컨테이너         | `docker ps` 에 `redis1`, Kafka, Qdrant 가 있고, Spring(8080)이 떠 있다                                                            |
| 워커             | ①(스크립트)은 서버 없이 Redis 만 있으면 된다. ②, ⑥ 은 8000 한 대면 되고, ③, ④, ⑤, ⑦ 은 **8000 과 8001 두 대**가 필요하다(워커 1이 처리한 결과를 워커 2의 주소로 묻기 때문). **코드가 바뀌었으니 이미 떠 있는 워커는 끄고 다시 띄운다** |
| 공급자           | Gemini 무료는 느리다(DAY22_RESULT 6, 7번). ③, ⑥ 은 `provider` 를 `claude` 로 하면 빠르다. 서버 `.env` 에 `ANTHROPIC_API_KEY` 가 필요하다 |
| 임시 키 정리     | 이 가이드가 만드는 키: `agent:result:*`(요청마다), `agent:conversation:d23-a`. ⑥ 끝에서 지운다                                    |

워커를 띄우는 명령 (터미널마다 하나, 가상환경을 켠 상태. 끌 때는 Ctrl+C)

```bash
# 터미널 1: 8000 = 워커 1
cd d2-ai-python
export KAFKA_WORKER_ID=w1
uvicorn app.main:app --port 8000

# 터미널 2: 8001 = 워커 2 (③, ④, ⑤, ⑦ 에서만)
cd d2-ai-python
export KAFKA_WORKER_ID=w2
uvicorn app.main:app --port 8001
```


## 5. 실행과 확인

### ① 결과 저장소 확인 스크립트

**목적:** 새 `result_store` 가 Redis 에 저장, 갱신, 다른 저장소에서 읽기, 실패 기록, 만료, 장애 대체를 기대대로 하는지 한 번에 본다.

```bash
cd d2-ai-python
python -m app.checks.result_check
```

기대: 12개 항목이 모두 `[PASS]` 이고 마지막이 `== 결과: PASS (12/12)`. Redis 장애 확인(8번 항목) 중에는 `결과를 Redis 에 못 저장함 ... 이 프로세스 메모리에만 둔다` 경고 줄이 나온다(일부러 접속 안 되는 주소로 바꿔서 확인하는 것이라 정상이다).

샌드박스(실제 `redis-server`)에서는 12/12 PASS 였다. 내 환경에서도 12개 항목 모두 PASS 였다(DAY23_RESULT 3-1).

### ② 결과가 Redis 에 들어간 모양

**목적:** 요청 한 건이 Redis 에 어떤 키, 값, TTL 로 저장되는지 `redis-cli` 로 직접 본다.

```bash
# 터미널 3: 요청 한 건 (provider 를 claude 로)
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"claude"}'
# 응답의 requestId 를 아래 <requestId> 에 쓴다

docker exec redis1 redis-cli --scan --pattern 'agent:result:*'
docker exec redis1 redis-cli GET agent:result:<requestId>
docker exec redis1 redis-cli TTL agent:result:<requestId>
```

기대(확인 전 예상): 키 `agent:result:<requestId>`, 값이 `{"requestId": ..., "status": "DONE", "answer": ..., "elapsedMs": ..., "worker": "w1 또는 w2", "partition": ..., "offset": ..., "updatedAt": ...}`, TTL 이 86400 에서 조금 줄어든 값. 요청 직후 바로 조회하면 `"status": "PROCESSING"` 일 수 있다.

### ③ 전체 흐름 (워커 2대)

**목적:** Redis 로 바꾼 뒤에도 Spring → Kafka → 워커 2대 → 결과 조회가 전부 PASS 인지 본다.

```bash
# 프로젝트 루트(ai-agent-serving-infra)에서
FASTAPI="localhost:8000 localhost:8001" PROVIDER=claude bash scripts/day17_e2e.sh 4
```

기대(확인 전 예상): `RESULT: PASS`, `DONE 4건`, 워커별 처리 건수가 `w1` 과 `w2` 로 나뉘어 나온다(파티션 3개를 두 워커가 나눠 갖는다. Day 22 로그에서는 w1=[1], w2=[0,2] 였다). 예상 시간: 한 건 약 3~5초(Claude), 4건이 두 워커에 나뉘면 전체 약 10~30초. `PROVIDER` 를 빼면 Gemini 로 가고 건당 20~50초라 1~3분 걸린다(DAY22_RESULT 6번의 관찰값 기준).

### ④ 다른 워커 주소로 조회

**목적:** 워커 1이 처리한 결과를 워커 2의 주소로 물어도 읽히는지 본다. 메모리 저장소 때는 404 였던 부분이다.

③ 출력의 `[n] DONE ... worker=w1 ...` 줄에서 `worker=w1` 인 requestId 하나와 `worker=w2` 인 것 하나를 고른다.

```bash
# w1 이 처리한 id 를 w2 의 주소(8001)로, w2 가 처리한 id 를 w1 의 주소(8000)로 조회한다
curl -s -o /dev/null -w "%{http_code}\n" localhost:8001/api/v1/agent/results/<w1 이 처리한 requestId>
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/api/v1/agent/results/<w2 가 처리한 requestId>

# 본문까지 보려면 -o 와 -w 를 빼고 호출한다
curl -s localhost:8001/api/v1/agent/results/<w1 이 처리한 requestId>
```

기대(확인 전 예상): 두 번 모두 `200`, 본문의 `worker` 는 요청을 처리한 워커 이름(`w1` 또는 `w2`)이다. Day 17~22 코드에서는 처리하지 않은 워커의 주소가 `404` 였다.

### ⑤ 재시작해도 남는가

**목적:** 워커를 껐다 켜도 결과가 남는지 본다.

1. 두 워커 터미널에서 Ctrl+C 로 서버를 끈다.
2. 두 워커를 4번 준비의 명령으로 **다시 띄운다**(터미널 1 = 8000 `w1`, 터미널 2 = 8001 `w2`). 두 터미널 모두 `Application startup complete` 가 뜰 때까지 기다린다.
3. 새 터미널에서 ③ 에서 본 requestId 를 조회한다. **서버가 꺼진 채로 조회하면 접속이 거부되어 아무것도 나오지 않는다**(결과 저장소와 상관없는 결과다).

```bash
# 2번까지 끝나 서버가 떠 있는 상태에서 실행
curl -s localhost:8000/api/v1/agent/results/<③ 에서 본 requestId>
```

기대(확인 전 예상): `200`, `status DONE`, 재시작 전과 같은 답. TTL(기본 1일)이 지나기 전이면 남는다. 메모리 저장소 때는 `404` 였다.

### ⑥ Kafka 경로에서 이어 묻기

**목적:** 저장소를 바꿔도 같은 `sessionId` 로 이어 묻는 흐름(Day 20), 그리고 그 위의 락(Day 21)이 Spring 경로에서 동작하는지 다시 본다.

```bash
# 먼저 세션을 비운다 (이전에 쓴 d23-a 가 남아 있으면 turns 가 2 보다 커진다)
curl -s -X DELETE localhost:8000/api/v1/agent/sessions/d23-a

# 첫 질문. 결과가 DONE 이 된 뒤(몇 초 뒤)에 두 번째를 보낸다
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"claude","sessionId":"d23-a"}'

curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"방금 말한 기간을 넘기면 어떻게 되나요?","provider":"claude","sessionId":"d23-a"}'

# 두 번째 requestId 의 답, 그리고 저장된 대화(턴 2 이어야 한다)
curl -s localhost:8000/api/v1/agent/results/<두 번째 requestId>
curl -s localhost:8000/api/v1/agent/sessions/d23-a

# 정리
curl -s -X DELETE localhost:8000/api/v1/agent/sessions/d23-a
```

기대(확인 전 예상): 두 번째를 처리한 워커 터미널에 `이전 대화 2개 메시지 session=d23-a` 가 찍히고, 답이 "방금 말한 기간"을 이어서 말한다. 세션 조회는 `turns: 2`. 세션은 어느 워커의 8000 이든 8001 이든 같은 Redis 를 보므로 같은 결과다.

### ⑦ (선택) Redis 가 꺼졌을 때

**목적:** Redis 가 꺼져도 답을 만드는 일이 막히지 않고, 결과는 메모리로 대신하는지 본다.

```bash
docker stop redis1
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"배송은 보통 얼마나 걸리나요?","provider":"claude"}'
# 몇 초 뒤, 요청을 처리한 워커의 주소로 조회 (워커 터미널의 [agent] 완료 로그의 worker 값을 본다)
curl -s localhost:8000/api/v1/agent/results/<requestId>
curl -s localhost:8001/api/v1/agent/results/<requestId>
docker start redis1
```

기대(확인 전 예상): 워커 로그에 `결과를 Redis 에 못 저장함 ... 이 프로세스 메모리에만 둔다` 경고가 나오고 요청은 정상 처리된다. 조회는 처리한 워커의 주소에서만 `200`, 다른 워커 주소는 `404`. 이때 `/health` 의 `redis.connected` 는 `false` 다. 끝나면 `docker start redis1` 로 되살리고, 이 요청의 결과는 Redis 에 없다(메모리에만 있었다).


## 6. 확인한 것과 하지 않은 것

| 구분                | 내용                                                                                                                                           |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 확인함(샌드박스)    | `result_check.py` 12개 항목 PASS(실제 Redis). results 라우터를 FastAPI 로 띄워 조회: 처리 중 200(PROCESSING), 끝난 뒤 200(DONE), 메모리를 비운 뒤에도 200, 없는 id 404 |
| 확인하지 못함       | 내 환경의 Redis(`redis1`)에서의 ①, 실제 Kafka, Spring 8080, 워커 2대, 실제 모델로 한 ②~⑦. 샌드박스에는 Kafka, Spring, aiokafka 가 없어서 `agent_handler` 의 `to_thread` 로 바꾼 부분은 실행하지 못했다 |
| 확인하지 않을 것    | 결과 저장 정책(TTL 값), 결과를 오래 보관하는 방법 (Day 24)                                                                                      |


## 7. 이번에 하지 않은 것 (기록)

Day 23 계획표의 "전체 파이프라인 리팩토링"에서 후보로 본 것 가운데 이번에는 하지 않은 것과 이유다.

| 항목                              | 내용                                                                                                                                                                                                    | 언제                              |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `/run` 의 이벤트 루프 막힘 해소   | `routers/agent.py` 가 `async def` 안에서 동기 `run_agent` 를 직접 불러, 한 워커가 한 번에 한 요청만 처리한다(코드 주석에도 "to_thread 를 쓰지 않는 옛 경로"라고 적혀 있다). `asyncio.to_thread` 로 바꾸면 해소되지만, 계획표 Day 26 이 "동기식 구조로 임시 전환 → 504, 에러율 캡처"라서 이 경로가 동기 기준선일 가능성이 있다(추정). 지금 바꾸면 Day 22 http 테스트의 전제(N 이 워커 수보다 크면 차례 처리)도 달라진다 | Day 26 이후, 동기/비동기 비교를 마친 뒤 |
| `agent_graph.py` 분리             | 449줄에 도구, 모델 선택, 노드, 그래프, 대화 저장, 락이 한 파일에 있다. 모듈로 나눌 수 있지만 동작은 그대로이고, `race_test.py`, `lock_check.py` 와 가이드 문서가 이름과 줄 번호를 인용하고 있어 깨질 위험이 있다. 얻는 것에 비해 비용이 크다고 판단했다 | k6 단계가 끝난 뒤 시간이 남을 때   |


## 8. 체크리스트

- [x] 코드 변경 (`result_store.py`, `routers/results.py`, `agent_handler.py`, `day17_e2e.sh` 주석)
- [x] 확인 스크립트 작성 (`result_check.py`)
- [x] 샌드박스에서 `result_check.py` (12/12 PASS), results 라우터 조회
- [x] ① 내 환경에서 `python -m app.checks.result_check` (12개 항목 PASS. DAY23_RESULT 3-1)
- [x] ② `redis-cli` 로 결과 키, 값, TTL 보기 (DONE, worker w2, TTL 86369. DAY23_RESULT 3-2)
- [x] ③ `day17_e2e.sh` 로 워커 2대 전체 흐름 (Claude 11초, Gemini 29초, 둘 다 4건 DONE, PASS. DAY23_RESULT 3-3)
- [x] ④ 다른 워커 주소로 조회 (두 방향 모두 200. DAY23_RESULT 3-4)
- [x] ⑤ 재시작 뒤 조회 (재시작 전과 같은 본문. DAY23_RESULT 3-5)
- [x] ⑥ Kafka 경로에서 같은 `sessionId` 로 이어 묻기 (DAY23_RESULT 3-6)
- [x] ⑦ (선택) Redis 중단 (DAY23_RESULT 3-7)
- [x] 결과를 `DAY23_RESULT.md` 에 기록 (①~⑦ 까지)
