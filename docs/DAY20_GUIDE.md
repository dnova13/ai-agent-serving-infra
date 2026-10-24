# Day 20 가이드 — 에이전트 대화 상태를 Redis 에 저장하고 이어가기

> 상태: 코드 작성 완료, Spring 단위 테스트 `AgentRequestMessageTest` 는 내 환경에서 통과함(2026-10-13). ② 의 질문 쌍(세션 있음 2번, 세션 없음 1번)과 ③ 세션 분리도 실행해 기대대로 나왔다. ④ Spring → Kafka → FastAPI 경로도 `d20-c` 로 실행해 `sessions` 조회까지 확인했다. ⑧ 워커 두 대로 같은 세션 동시 요청도 실행했다(유실은 보지 못함). 나머지 중 ② 의 `d20-a` `sessions` 조회는 하지 않았고, ⑤~⑦ 은 직접 실행했다고 하나 출력을 받지 못해 결과를 기록하지 않았다. 확인한 결과로 읽지 않는다. 실행하면 [DAY20_RESULT.md](DAY20_RESULT.md) 에 결과를 적는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, **6379 = Redis(redis1 컨테이너)**, 9092 = Kafka, 6333 = Qdrant.


## 1. 오늘 목표

지금까지 에이전트는 요청마다 새 대화로 시작했다("그럼 그 기간이 지나면요?"처럼 앞 질문을 가리키는 질문에는 답할 수 없다). 같은 `sessionId` 로 온 요청이면 앞의 질문과 답을 Redis 에서 불러와 이어서 답하고, 끝나면 이번 질문과 답을 다시 저장한다.

| 확인할 것                                                     | 실험 |
| ------------------------------------------------------------- | ---- |
| Redis, Spring 이 준비됐는가                                   | ①    |
| 같은 세션의 두 번째 질문이 앞의 대화를 받아 답하는가 (FastAPI 직접) | ②    |
| 세션이 없는 요청, 다른 세션은 대화가 섞이지 않는가            | ③    |
| Spring → Kafka → FastAPI 를 거쳐도 `sessionId` 가 전달되는가  | ④    |
| Redis 에 실제로 어떤 모양으로 저장되는가                      | ⑤    |
| (선택) 만료 시간이 지나면 대화가 사라지는가                   | ⑥    |
| (선택) Redis 가 꺼진 동안 요청하면 어떻게 되는가              | ⑦    |
| (선택) 같은 세션에 동시에 두 요청을 보내면                    | ⑧    |


## 2. 개념

| 용어        | 설명                                                                                                                              |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `sessionId` | 한 대화를 가리키는 이름. 클라이언트가 정해서 보낸다(예: `user-1`). 같은 값이면 같은 대화다. 비우면 새 대화                          |
| 대화 기록   | Redis 키 `agent:conversation:<sessionId>` 의 값. `[{"role":"user","content":"..."},{"role":"assistant","content":"..."}, ...]`    |
| 턴          | 질문과 답 한 쌍. 최근 10턴(`CONVERSATION_MAX_TURNS`)만 남긴다. 길어질수록 LLM 에 보내는 양과 비용이 늘기 때문이다                  |
| TTL         | 마지막 대화 후 3600초(`CONVERSATION_TTL_SEC`)가 지나면 Redis 가 지운다. 정식 정책은 Day 24 에서 정한다. 지금은 키가 쌓이지 않게 하는 임시 값이다 |

**무엇을 저장하나.** 질문과 최종 답(텍스트)만 저장한다. 도구 호출과 검색 결과(ToolMessage)는 저장하지 않는다. 다음 질문에서 모델이 필요하면 도구를 다시 부른다. 저장이 단순하고 작아지는 대신, 앞에서 검색한 문서 원문은 다음 질문에서 쓸 수 없다.

```
요청(sessionId=user-1, "그럼 그 기간이 지나면요?")
   │
   ├─ Redis 에서 agent:conversation:user-1 읽기  → [Q1, A1]
   ├─ 모델에 [Q1, A1, 이번 질문] 을 보내 답 생성
   └─ [Q1, A1, 이번 질문, 이번 답] 을 TTL 3600초로 다시 저장
```

**이어가려면 앞 요청이 끝나야 한다.** Spring(8080)은 요청을 Kafka 에 넣고 바로 202 를 주지만, 답은 20~30초 뒤에 나온다. 앞 질문의 처리가 끝나기 전에 같은 세션으로 다음 질문을 보내면, 다음 질문은 앞의 답을 아직 볼 수 없다. ④ 에서는 `DONE` 을 확인한 뒤 다음 질문을 보낸다. 완전히 동시에 보내면 서로의 기록을 덮어쓸 수도 있고, 이것은 ⑧ 과 Day 21~22(락)의 주제다.


## 3. 만든/바꾼 파일

| 파일                                                         | 내용                                                                                                                           |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `d2-ai-python/app/services/conversation_store.py` (신규)     | `load_history`, `append_turn`(최근 N턴만 유지, TTL 갱신), `clear`, `ttl_left`. Day 19 의 `redis_client` 위에서 동작            |
| `d2-ai-python/app/services/agent_graph.py` (수정)            | `run_agent(question, provider, session_id=None)`: 있으면 이전 대화를 불러와 앞에 붙이고, 끝나면 이번 턴을 저장. Redis 오류는 경고 로그만 남기고 계속 |
| `d2-ai-python/app/services/agent_handler.py` (수정)          | Kafka 메시지의 `sessionId` 를 `run_agent` 에 넘김. 시작 로그에 `sessionId=` 추가                                               |
| `d2-ai-python/app/schemas/models.py` (수정)                  | `AgentRequest.session_id`, `AgentRequestMessage.sessionId` (둘 다 선택)                                                         |
| `d2-ai-python/app/routers/agent.py` (수정)                   | `POST /api/v1/agent/run` 이 `session_id` 를 받음                                                                               |
| `d2-ai-python/app/routers/sessions.py` (신규)                | `GET /api/v1/agent/sessions/{sessionId}`(저장된 대화, 남은 시간, 없으면 404), `DELETE` (지우기). 저장 내용을 HTTP 로 보는 확인용 |
| `d2-ai-python/app/main.py` (수정)                            | `sessions` 라우터 등록                                                                                                         |
| `d3-was-spring/.../api/AgentRequestBody.java` (수정)         | `sessionId` 필드 추가(선택, 100자 이하)                                                                                         |
| `d3-was-spring/.../api/AgentRequestController.java` (수정)   | 빈 `sessionId` 는 null 로 보고 메시지에 담음                                                                                    |
| `d3-was-spring/.../messaging/AgentRequestMessage.java` (수정) | `sessionId` 추가. JSON 맨 끝에 붙임(`"sessionId":"..."` 또는 `null`)                                                          |
| `d3-was-spring/.../AgentRequestMessageTest.java` (수정)      | 기대 JSON 에 `sessionId` 반영, `sessionId` 가 있는 경우 케이스 추가                                                            |

| 환경변수                  | 기본값 | 뜻                                      |
| ------------------------- | ------ | --------------------------------------- |
| `CONVERSATION_MAX_TURNS`  | `10`   | 남기는 최근 턴 수                       |
| `CONVERSATION_TTL_SEC`    | `3600` | 마지막 저장 후 대화가 살아 있는 시간(초) |

**Kafka 메시지 호환.** `sessionId` 는 선택이라, 옛 형식(필드 없음)과 `null` 도 그대로 처리된다. Spring 과 FastAPI 중 하나만 먼저 올려도 깨지지 않는다.


## 4. 준비

1. Redis 컨테이너가 떠 있는지 본다(`docker ps` 의 `redis1`). Day 19 가이드의 ①~② 가 통과한 상태면 좋다.
2. FastAPI 워커(8000)를 **다시 켠다**(코드가 바뀌었다). 가상환경에서 `d2-ai-python` 으로 가서 `source run.sh` 또는 `uvicorn app.main:app --port 8000`.
3. Spring(8080)을 **다시 켠다**(`sessionId` 필드가 추가됐다). 단위 테스트는 `d3-was-spring` 에서 `./gradlew test --tests "com.test.aiserving.was.messaging.AgentRequestMessageTest"` 로 돌릴 수 있고, 이미 통과했다(6번).


## 5. 실행과 확인

질문이 서로 이어지는 쌍으로 시험한다. 한 번에 한 요청씩, 앞 요청의 답이 나온 뒤에 다음을 보낸다(요청당 20~30초).

### ① 준비 확인

```bash
curl -s localhost:8000/health
```

기대(확인 전 예상): `"redis":{"connected":true,...}`.

### ② 같은 세션이 대화를 이어가는가 (FastAPI 직접)

Kafka 와 Spring 을 거치지 않고 FastAPI 로 바로 부른다(`/api/v1/agent/run` 은 답이 나올 때까지 응답이 오지 않는다). 세션 이름은 `d20-a` 로 한다.

```bash
curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "환불은 며칠 안에 신청해야 하나요?", "session_id": "d20-a"}'

curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "그럼 그 기간이 지나면 어떻게 되나요?", "session_id": "d20-a"}'
```

기대(확인 전 예상): 두 번째 답이 "그 기간"을 환불 신청 기간으로 이해하고 답한다. 서버 터미널에 `[timing] 이전 대화 0개 메시지 session=d20-a` 다음에 `이전 대화 2개 메시지 session=d20-a` 가 찍힌다(두 번째 요청이 앞 대화 2개를 받았다는 뜻).

비교로 같은 두 번째 질문을 세션 없이 보낸다.

```bash
curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "그럼 그 기간이 지나면 어떻게 되나요?"}'
```

기대: "무슨 기간을 말씀하시는지" 같은 되묻는 답이 나올 것이다(모델이 무엇을 답할지는 확인 전이다).

```bash
curl -s localhost:8000/api/v1/agent/sessions/d20-a
```

기대: `turns` 2, `ttlSec` 3600 이하, `messages` 4개(user, assistant, user, assistant).

### ③ 세션 분리

```bash
curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "방금 내가 뭐라고 물었지?", "session_id": "d20-b"}'
curl -s localhost:8000/api/v1/agent/sessions/d20-b
```

기대: `d20-b` 는 새 대화라서 `d20-a` 의 질문을 알지 못한다. 터미널에는 `이전 대화 0개 메시지 session=d20-b`. 세션 없는 요청(②의 비교)은 어디에도 저장되지 않는다(`GET .../sessions/` 에 새 항목이 생기지 않는다).

### ④ Spring → Kafka → FastAPI

세션 이름은 `d20-c`. 첫 요청을 보내고 `requestId` 를 받아 `DONE` 이 될 때까지 기다린 뒤 두 번째를 보낸다.

```bash
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question": "교환 시 배송비는 누가 부담하나요?", "sessionId": "d20-c"}'
# 응답의 requestId 로 상태 확인 (DONE 이 될 때까지, 보통 20~30초)
curl -s localhost:8000/api/v1/agent/results/<requestId>

curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question": "그럼 반품할 때는요?", "sessionId": "d20-c"}'
curl -s localhost:8000/api/v1/agent/results/<두 번째 requestId>
curl -s localhost:8000/api/v1/agent/sessions/d20-c
```

기대(확인 전 예상): 워커 로그의 `[agent] 시작 ... sessionId=d20-c`, 두 번째 요청에서 `이전 대화 2개 메시지`, 마지막 조회에서 `turns` 2. `sessionId` 를 빼고 보내면 로그가 `sessionId=None` 이다.

### ⑤ Redis 에 저장된 모양

```bash
docker exec redis1 redis-cli keys 'agent:conversation:*'
docker exec redis1 redis-cli get agent:conversation:d20-a
docker exec redis1 redis-cli ttl agent:conversation:d20-a
```

기대: 키 목록에 `agent:conversation:d20-a` 등이 보이고, `get` 은 한글이 그대로 보이는 JSON 문자열, `ttl` 은 3600 이하.

### ⑥ (선택) 만료

`CONVERSATION_TTL_SEC=10` 으로 워커(8000)를 다시 켜고(`export CONVERSATION_TTL_SEC=10`), `session_id` 를 `d20-ttl` 로 질문 한 번을 보낸 뒤 10초 후에 조회한다.

```bash
curl -s localhost:8000/api/v1/agent/sessions/d20-ttl   # 바로: 200, ttlSec 10 이하
sleep 11; curl -s -w "\n%{http_code}\n" localhost:8000/api/v1/agent/sessions/d20-ttl   # 기대: 404
```

확인한 뒤에는 환경변수를 지우고(`unset CONVERSATION_TTL_SEC`) 워커를 다시 켠다.

### ⑦ (선택) Redis 가 꺼진 동안

```bash
docker stop redis1
curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "환불은 며칠 안에 신청해야 하나요?", "session_id": "d20-down"}'
docker start redis1
```

기대(확인 전 예상): 답은 정상으로 오고, 서버 터미널에 `대화 기록을 못 불러옴`, `대화 기록을 못 저장함` 경고가 찍힌다. 대화 이어가기만 포기하고 요청은 실패하지 않는 설계다.

### ⑧ (선택) 같은 세션에 동시 요청

워커 하나만으로는 이 문제가 나타나지 않는다. `/api/v1/agent/run` 은 `async` 함수 안에서 동기 함수(`run_agent`)를 부르기 때문에 그동안 이벤트 루프가 막혀 요청이 한 건씩 차례로 처리되고, Kafka 워커도 한 번에 한 건만 처리한다. 겹치려면 워커가 둘 이상이어야 한다. 워커 2(8001)를 Day 18 가이드 4번처럼 하나 더 켠 뒤(`export KAFKA_WORKER_ID=w2`, `uvicorn app.main:app --port 8001`) 같은 세션(`d20-race`)에 두 요청을 동시에 보낸다.

```bash
curl -s -X POST localhost:8000/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "환불은 며칠 안에?", "session_id": "d20-race"}' &
curl -s -X POST localhost:8001/api/v1/agent/run -H "Content-Type: application/json" \
  -d '{"query": "교환 배송비는 누가?", "session_id": "d20-race"}' &
wait
curl -s localhost:8000/api/v1/agent/sessions/d20-race
```

기대(확인 전 예상): 두 워커가 각자 "빈 대화"를 읽고 각자 저장하므로 `turns` 가 2 가 아니라 1 일 수 있다(한쪽 턴이 덮어써져 유실). **실행 결과: `turns` 2 로 유실이 없었다**(DAY20_RESULT 3-6). 이 예상은 과했다. `append_turn` 은 저장 직전에 대화를 다시 읽기 때문에, 유실은 두 워커의 읽기와 쓰기가 몇 밀리초 안에 겹칠 때만 생긴다. 락이 없는 현재 구조의 문제를 눈으로 보는 확인이며, 락은 Day 21, 비교는 Day 22 에서 한다. 요청이 겹쳐야 일어나므로 한 번 안 보인다고 문제가 없는 것은 아니다. 워커 2(8001)는 확인 후 `Ctrl+C` 로 끈다.


## 6. 확인한 것과 하지 않은 것

| 구분         | 내용                                                                                                                                                                                                                                      |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 확인함       | 샌드박스에서 가짜 에이전트 그래프로 확인: 같은 세션의 두 번째 호출이 앞의 질문과 답을 받음, 세션 없으면 새 대화, 다른 세션 분리, `CONVERSATION_MAX_TURNS=3` 에서 최근 3턴만 남음, TTL 3600 설정, `clear`, Redis 가 꺼져도 답을 돌려주고 경고만 남김, `sessions` API 의 200/404/삭제, Kafka 메시지의 옛 형식, `sessionId` 있음, `null` 파싱. **실제 Gemini 가 아니라 가짜 그래프이고 샌드박스의 `redis-server` 7.0.15 환경이다** |
| 확인함 (내 환경) | Spring `AgentRequestMessageTest` 만 실행해 `BUILD SUCCESSFUL`. 출력된 JSON 으로 `sessionId` 가 없을 때 `"sessionId":null`, 있을 때 `"sessionId":"user-1"` 이 맨 끝에 붙는 것을 확인했다. 이 환경에서 `compileJava` 는 `UP-TO-DATE`(이미 컴파일된 상태)였다 |
| 확인하지 않음 | 실제 Gemini 가 이전 대화를 보고 의미 있게 이어서 답하는지(②의 기대), 컨트롤러가 `sessionId` 를 받아 메시지에 담는 부분(이 단위 테스트는 `AgentRequestMessage` 의 JSON 변환만 본다), 사용자 환경의 Redis, `./gradlew test` 전체 실행, ⑥~⑧ 의 기대 전부                                                             |


## 7. 한계

| 항목                      | 내용                                                                                                                                                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 같은 세션의 동시 요청     | 워커(프로세스)가 둘 이상이면 읽고 쓰는 사이에 서로의 턴을 덮어쓸 수 있다(락 없음). Day 21 락, Day 22 비교                                                                                   |
| Kafka 키가 `requestId`    | 같은 세션의 요청이 서로 다른 파티션, 다른 워커로 가서 동시에 처리될 수 있다. 세션 순서를 지키려면 키를 `sessionId` 로 바꾸는 방법이 있다(이번에는 바꾸지 않음)     |
| 도구 결과 미저장          | 앞에서 검색한 문서 원문은 다음 질문에서 쓸 수 없다(필요하면 모델이 다시 검색한다)                                                                              |
| 길이 제한이 턴 수 기준    | 최근 10턴으로 자르지만 글자 수, 토큰 수로는 재지 않아서 긴 답이 쌓이면 모델 입력이 커질 수 있다                                                                |
| 만료 정책이 임시          | 3600초는 임시 값이다. 세션별 정책은 Day 24                                                                                                                    |
| 요청 결과는 아직 메모리   | `GET .../results/{requestId}` 의 결과는 여전히 워커 메모리다(Redis 로 옮기는 것은 이후 날짜). 대화 기록만 Redis 다                                                |
| `sessionId` 검증이 약함   | 길이(100자)만 본다. 누가 어떤 세션을 읽을 수 있는지(권한)는 다루지 않는다. 다른 사람의 `sessionId` 를 알면 그 대화에 이어서 질문하고 `sessions` API 로 읽을 수 있다 |


## 8. 체크리스트

- [x] 코드 작성 (가짜 그래프와 `redis-server` 로 동작 확인)
- [x] Spring `AgentRequestMessageTest` (통과, 전체 `./gradlew test` 는 아직)
- [ ] ① 준비 (`/health` 의 `redis`)
- [x] ② 같은 세션 이어가기, 세션 없는 비교 (결과는 DAY20_RESULT.md)
- [ ] ② `d20-a` 의 `sessions` 조회 (`turns`, `ttlSec`, 저장된 메시지. `d20-b` 는 ③ 에서 조회함)
- [x] ③ 세션 분리
- [x] ④ Spring → Kafka → FastAPI 로 `sessionId` 전달 (`d20-c`, 결과는 DAY20_RESULT.md 3-4)
- [ ] ⑤ `redis-cli` 로 저장된 모양 (실행했다고 함, 출력 없음)
- [ ] (선택) ⑥ 만료 (실행했다고 함, 출력 없음)
- [ ] (선택) ⑦ Redis 중지 중 요청 (실행했다고 함, 출력 없음)
- [x] (선택) ⑧ 같은 세션 동시 요청 (유실은 보지 못함, 결과는 DAY20_RESULT.md 3-6)
- [x] 결과를 `DAY20_RESULT.md` 에 기록 (② 의 질문 쌍까지, 나머지는 실행하면 추가)
