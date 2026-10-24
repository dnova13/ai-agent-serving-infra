# Day 35 코드 가이드 — 전체 시스템 통합 재검증 스크립트

> 실행 방법과 확인 항목은 [DAY35_GUIDE.md](DAY35_GUIDE.md). 이 문서는 **새로 만든 `system_verify.py` 하나가 무엇을 어떤 순서로 하는지** 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, 9092 = Kafka(호스트), 6379 = Redis, 9090 = Prometheus, 3000 = Grafana.

**목적:** Day 1~34 에 걸쳐 만든 점검을 한 번에 다시 돌리고, 평소 설정으로 켜 둔 시스템을 가짜 모델 요청으로 한 줄로 이어 확인한 뒤, 같은 경로가 실제 모델(Claude)로도 되는지 소량 확인해 PASS / FAIL / SKIP 으로 요약한다. 새 기능이 아니라 **회귀 검증**이다(이전 Day 의 수정이 다른 Day 의 기능을 깨뜨렸는지 본다).

**Day 35 의 코드 변경:** 새 파일 `d2-ai-python/app/checks/system_verify.py` 하나(함수 7개). 기존 코드는 바꾸지 않았다.


## 1. 전체 흐름

```
cd d2-ai-python && python -m app.checks.system_verify
  1단계 run_offline()   기존 점검 스크립트를 하나씩 실행 → 종료 코드로 PASS / SKIP / FAIL
  2단계 run_live()      켜 둔 시스템에 요청을 보내 응답을 직접 비교
  3단계 run_real()      실제 Claude 로 비동기 질문 n건, 동기 1건을 보내 답이 오는지 확인
  main()                PASS/FAIL/SKIP 개수 요약, FAIL 이 하나라도 있으면 종료 코드 1
```

`--only offline` 은 1단계만, `--only live` 는 2단계만, `--only real` 은 3단계만 실행한다. 옵션 없이 실행하면 세 단계 모두 실행하고, 3단계는 Claude 와 검색 도구의 Gemini 임베딩을 실제로 부른다.


## 2. 공통 부품

| 이름 | 하는 일 |
| ---- | ------- |
| `CHECKS` | 1단계에서 실행할 점검 목록 `(이름, python 인자, 실행 위치)`. 모두 `d2-ai-python` 에서 `-m app.checks.<이름>` 으로 실행한다(`lock_stress` 는 `-m app.experiments.lock_stress`). 목록: `redis_check`, `result_check`, `ttl_check`, `lock_check`, `selfcorrect_check`, `dlq_check`, 그리고 `lock_stress` 의 `baseline`, `timeout` 시나리오(N=50) |
| `rows` | 항목별 결과 `(이름, PASS/FAIL/SKIP, 설명)` 를 모아 두는 목록. 마지막 요약에 쓴다 |
| `rec(name, status, detail)` | 결과를 `rows` 에 넣고 `[PASS] 이름 (설명)` 한 줄을 출력한다. PASS 면 `True` 를 돌려줘 호출한 쪽이 `all_ok &= rec(...)` 처럼 쓴다 |
| `http(method, url, body, timeout)` | 표준 라이브러리(`urllib`)로 요청을 보내 `(상태 코드, 본문)` 을 돌려준다. 본문이 JSON 이면 파싱해서 준다. 4xx, 5xx 도 `(코드, 본문)` 으로 돌려주고, 연결 자체가 실패하면 `(0, None)` |
| `REAL_QUESTIONS` | 3단계에 보낼 질문 5개(k6 `ramping.js` 와 같은 환불, 배송 FAQ) |
| `poll_result(worker, rid, timeout)` | 워커의 `/api/v1/agent/results/<requestId>` 를 0.5초마다 읽어 `DONE` 또는 `FAILED` 가 되면 `(결과, 걸린 초)` 를 돌려준다. 최대 60초 |

`lock_stress` 를 N=50 으로 돌리는 이유: N=100 은 Redis 연결 풀 한도(100)와 같아 Day 33 에서 본 경계에 걸릴 수 있어서, 이 검증에서는 경계 밖의 값을 쓴다.


## 3. 1단계 `run_offline()`

`CHECKS` 를 순서대로 `python -m app.services.<이름>` 으로 실행한다(`sys.executable` 이라 지금 쓰는 가상환경의 파이썬이 쓰인다. 그래서 `d2-ai-python` 에서 실행해야 `app.services...` 를 찾는다. `dlq_check` 만 실행 위치를 `app` 폴더로 바꿔 실행한다).

| 종료 코드 | 판정 |
| --------- | ---- |
| 0 | PASS (걸린 초 표시) |
| 2 | SKIP (마지막 출력 줄 표시). 점검 스크립트 중 코드에서 2 를 내는 곳은 `lock_stress` 의 "Redis 에 접속하지 못함" 과 "알 수 없는 시나리오" 뿐이다 |
| 그 밖 | FAIL (표준 에러의 마지막 줄 표시) |
| 600초 안에 안 끝남 | FAIL |

판정은 각 스크립트의 종료 코드만 본다. 스크립트가 안에서 항목을 건너뛰었는지는 요약에 나오지 않으므로 필요하면 그 스크립트를 따로 실행한다.


## 4. 2단계 `run_live()`

평소 설정(Spring 인자 없이, 워커는 토픽 `agent.requests`, 그룹 `agent-workers`, 가짜 모델 켬)으로 켜져 있어야 한다. 항목은 위에서 아래로 진행한다.

| 순서 | 항목 | 판정 |
| ---- | ---- | ---- |
| 2-1 | 워커 `/health` (`--workers` 에 준 주소마다) | 200, `status == ok`, Kafka 컨슈머 `state == running`, 토픽 `agent.requests`, 그룹 `agent-workers`, 맡은 파티션 있음, Redis 연결됨. 하나라도 어긋나면 FAIL 이고 어떤 점이 어긋났는지 적는다(예: 토픽이 `agent.requests` 가 아니면 Day 34.1 실험 설정이 남았을 수 있다) |
| 2-2 | Spring `/actuator/health`, `/actuator/prometheus`, 워커 `/metrics` | `UP`, 지표 이름(`http_server_requests`, `agent_worker`)이 본문에 있는지 |
| 2-3 | end-to-end 가짜 모델 요청 | Spring 에 `POST /api/agent/requests`(provider `fake`, 새 sessionId) → 202 와 requestId → `poll_result` 로 `DONE`, 답에 `[fake]`, 결과에 `worker`, `partition`, `offset` 이 있음 |
| 2-4 | 같은 세션 대화 이어가기, 세션 삭제 | 같은 sessionId 로 두 번째 요청 → `DONE`, 저장된 턴 2. 삭제(DELETE) 200 뒤에 조회가 404 |
| 2-5 | 입력 검증 | 공백뿐인 question → 400 |
| 2-6 | Prometheus 대상, Grafana | 대상 중 `spring`, `kafka`, `redis` job 이 모두 up 이면 PASS(나머지 대상이 up 이 아니면 설명에만 적는다). Grafana `/api/health` 200. 응답이 없으면 SKIP |
| 2-7 | 컨슈머 그룹 lag | `docker exec kafka kafka-consumer-groups --describe` 출력에서 `agent-workers` 줄의 LAG 열(6번째)을 모두 더해 0 이면 PASS. docker 를 못 쓰면 SKIP |

**중간에 멈추는 규칙:** 2-1 에서 워커 상태가 하나라도 정상이 아니면 뒤의 항목(Spring 상태, end-to-end, 같은 세션, 입력 검증)은 `SKIP`("워커 상태 이상")으로 기록하고 `run_live` 를 끝낸다. 워커가 없으면 이어 보는 것이 의미 없어서다. 2-3 의 첫 요청이 실패하면 같은 세션 항목도 `SKIP` 한다.

**폴링 대상:** `poll_result` 는 Spring 이 아니라 **워커(`--workers` 의 첫 주소)** 에서 결과를 읽는다. 결과는 Redis 에 저장되고, 워커가 `GET /api/v1/agent/results/<requestId>`(`routers/results.py`)로 조회를 제공한다.


## 5. 3단계 `run_real()`

2단계와 같은 설정이면 된다. 워커에 `FAKE_LLM_ENABLED=true` 가 켜져 있어도 `provider=claude` 는 실제 모델로 간다(`agent_graph.get_model` 에서 `fake`, `fake-agent` 만 이 환경변수를 본다). `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY`(검색 도구의 임베딩)가 워커 환경에 있어야 한다.

| 순서 | 항목 | 하는 일과 판정 |
| ---- | ---- | -------------- |
| 3-0 | 워커 `/health` | 200 이 아니면 두 항목 모두 `SKIP`("워커 /health http=...") 하고 끝낸다 |
| 3-1 | 실제 모델 end-to-end (비동기) | `--real-n` 건(기본 3)을 Spring `POST /api/agent/requests`(provider `claude`, 건마다 새 sessionId)로 접수한 뒤, 접수한 순서대로 `poll_result`(최대 120초)로 결과를 기다린다. 건마다 `status == DONE`, 답이 비어 있지 않음, 답에 `[fake]` 가 없음, `worker`, `partition`, `offset` 이 있음을 본다. 전부 만족하면 PASS. 하나라도 아니면 FAIL 이고 어느 건이 왜 아닌지(접수 실패 http, `FAILED` 의 `error`, 빈 답, 가짜 답, 빠진 필드)를 설명에 적는다. 설명에는 `n/n건 DONE`, 접수부터 완료까지 평균과 최대, 결과의 `elapsedMs`(워커 처리 시간) 평균이 들어간다. 끝나면 만든 세션을 지운다 |
| 3-2 | 실제 모델 동기 경로 `/api/agent/sync` | Spring 에 질문 1건(provider `claude`, 응답 대기 최대 60초)을 보내 200 이고 `answer` 가 비어 있지 않고 `[fake]` 가 아니면 PASS. 아니면 FAIL 과 상태 코드, 본문 앞부분 |

접수한 요청은 워커 1대가 차례로 처리하므로 n 건의 접수부터 완료까지 시간은 건마다 늘어난다(마지막 건이 가장 길다). 3-1 의 평균과 최대, `elapsedMs` 평균을 같이 보면 대기 시간과 처리 시간을 나눠 볼 수 있다.


## 6. `main()` 과 종료 코드

1. 인자: `--only`(`offline`, `live`, `real`), `--real-n`(3단계 비동기 질문 수, 기본 3), `--workers`(기본 `localhost:8000`, 쉼표로 여러 개), `--spring`, `--prometheus`, `--grafana`.
2. `--only` 에 따라 1, 2, 3단계를 실행한다(없으면 모두).
3. `rows` 를 세어 `== 요약: PASS n, FAIL n, SKIP n` 을 출력하고, PASS 가 아닌 항목을 다시 한 줄씩 보여 준다.
4. 마지막 줄은 `== 결과: PASS`(FAIL 이 0개) 또는 `== 결과: FAIL`. 종료 코드도 같다(0 또는 1). SKIP 은 결과에 영향을 주지 않는다.


## 7. 이 스크립트가 보지 않는 것

- **실제 모델은 소량만 쓴다.** 3단계는 질문 3건과 동기 1건이라 처리량, 답변 품질, 호출 한도(429)는 보지 않는다(처리량은 Day 34.2 에서 쟀다). 1, 2단계는 가짜 모델만 쓴다.
- 워커 1대 기준이다(`--workers` 로 여러 대의 상태는 볼 수 있지만 end-to-end 는 첫 워커로 읽는다).
- 성능은 재지 않는다. 회귀 검증(되는가, 안 되는가)이다.
