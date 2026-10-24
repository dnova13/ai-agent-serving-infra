# Day 31 가이드 — Self-Correction (실패 종류에 따른 재시도)

> 상태: **①②③ 실행 완료([DAY31_RESULT.md](DAY31_RESULT.md))**.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, 9092 = Kafka, 6379 = Redis(`redis1`), 6333 = Qdrant. ① 은 이 중 아무것도 필요 없고, ② 는 전부, ③ 은 Qdrant 와 모델 API 키만 필요하다.
> 앞선 결과: [DAY30_RESULT.md](DAY30_RESULT.md)(Prometheus + Grafana 모니터링).

**전체 목적:** 지금의 재시도(Day 6)는 도구가 실패하면 같은 입력으로 같은 도구를 다시 실행한다. Qdrant 연결이 잠깐 끊긴 것처럼 시간이 지나면 풀리는 실패에는 맞지만, 검색어 자체가 문제인 실패는 몇 번을 다시 해도 같은 결과가 나온다. 오늘은 실패를 두 종류로 나눠, 일시 장애는 지금처럼 재실행하고 입력 오류는 실패 원인을 모델에게 알려 입력을 고쳐 다시 부르게 한다. 두 경로가 의도대로 갈리는지, 한도에서 멈추는지, 지표에 잡히는지 확인한다.


## 1. 오늘 목표

| 확인할 것                                                                                                         | 실험 |
| ----------------------------------------------------------------------------------------------------------------- | ---- |
| 가짜 모델과 가짜 검색으로 7개 시나리오(정상, 일시 장애, 입력 오류 정정, 정정 불가, Day 6 회귀 등)가 기대대로 흐르는지 | ①    |
| 워커를 거치는 전체 경로(Spring, Kafka, 워커, 실제 Qdrant)에서도 같은 흐름이 되고 지표가 `/metrics` 에 나오는지        | ②    |
| 실제 모델(Gemini, Claude)이 정정 안내를 읽고 검색어를 고치는지                                                    | ③    |


## 2. 개념

| 항목                | 내용                                                                                                                                                                  |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 일시 장애(transient) | 시간이 지나면 풀릴 수 있는 실패. 연결 끊김, 타임아웃, 429 등. 같은 입력으로 다시 실행하면 성공할 수 있다. 입력 오류가 아닌 모든 예외를 여기에 넣는다(`강제실패` 포함)       |
| 입력 오류(input)    | 입력 자체가 문제인 실패. 새 예외 `ToolInputError`, 없는 도구 이름, 도구 인자 검증 실패. 같은 입력으로 다시 실행해도 소용없다                                            |
| 재시도(retry)       | 일시 장애일 때 모델을 거치지 않고 그래프가 같은 호출을 그대로 다시 실행하는 것. 한도 `MAX_RETRIES`=2 (Day 6 그대로)                                                    |
| 정정(correction)    | 입력 오류일 때 실패 원인을 모델에게 돌려보내 입력을 고쳐 다시 부르게 하는 것. 한도 `MAX_CORRECTIONS`=2. 도구 호출은 처음 1번 + 정정 2번 = 최대 3번이다                    |
| 정정 안내           | 입력 오류 결과 뒤에 붙이는 문구. "같은 입력으로 다시 호출하지 말고 원인을 읽고 고쳐 호출하라, 남은 정정 기회 N번"                                                      |
| 복구(recovery)      | 실패 뒤 도구가 성공한 것. 직전 실패가 일시 장애면 `retry` 로, 입력 오류면 `correction` 으로 센다                                                                       |
| `fake-agent`        | 도구를 부르고, 정정 안내를 받으면 검색어에서 `교정테스트` 를 빼 다시 부르는 가짜 모델. `FAKE_LLM_ENABLED=true` 일 때만 켜진다. 기존 `fake` 는 도구를 안 불러 이 경로를 못 탄다 |

도구 실행 뒤 분기는 아래와 같다.

| 상황                         | 다음                       | 한도                                    | 한도를 넘으면                                |
| ---------------------------- | -------------------------- | --------------------------------------- | -------------------------------------------- |
| 성공                         | `agent`(모델이 답 작성)     | -                                       | -                                            |
| 일시 장애                    | `tools`(같은 호출 재실행)   | `retry_count` 가 `MAX_RETRIES`(2) 이상이면 중단 | `give_up`: "2번 시도했지만 처리하지 못했습니다"   |
| 입력 오류                    | `agent`(정정 안내와 함께)   | `correction_count` 가 `MAX_CORRECTIONS`(2) 를 넘으면 중단 | `give_up`: "검색 조건을 바꿔 2번 다시 시도했지만 ..." |

한 번에 여러 도구를 불러 실패 종류가 섞이면 일시 장애로 본다(재실행은 이번에 부른 도구를 전부 다시 실행하기 때문).


## 3. 바뀐 것

| 파일                                              | 내용                                                                                                                                                                  |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `d2-ai-python/app/services/agent_graph.py`        | `ToolInputError`, `MAX_CORRECTIONS`, 상태 필드 `correction_count`·`last_failure_kind`, `tool_node` 의 실패 분류와 정정 안내, `after_tools_router`·`give_up_node` 분기, 도구의 입력 검사(빈 검색어, 테스트 단어 `교정테스트`·`교정불가`), provider `fake-agent` |
| `d2-ai-python/app/services/fake_llm.py`           | `FakeAgentChatModel` 추가(기존 `FakeChatModel` 은 그대로)                                                                                                                |
| `d2-ai-python/app/services/metrics.py`            | `agent_tool_failures_total{tool,kind}`, `agent_tool_recoveries_total{via}`, `agent_give_ups_total{reason}` 추가                                                         |
| `d2-ai-python/app/checks/selfcorrect_check.py`  | 새 파일. ① 의 시나리오 7개를 돌려 PASS/FAIL 출력                                                                                                                       |
| `docs/DAY31_GUIDE.md`                             | 이 문서                                                                                                                                                                |

기존 동작이 바뀌는 곳이 둘 있다. `retry_count` 는 이제 일시 장애일 때만 늘어난다. 없는 도구 이름은 예전에는 일시 장애처럼 재실행됐지만 이제 입력 오류로 모델에게 돌아간다. `강제실패`(Day 6 시험 분기)는 예전과 똑같이 재실행 후 포기한다. 새 패키지는 없어서 `pip install` 은 필요 없다.


## 4. 준비

①③ 은 파이썬 가상환경(venv)을 켠 터미널이면 된다. ② 는 Day 30 까지 쓴 환경을 모두 띄운다(Kafka, Redis, Qdrant, Spring). 먼저 `agent-workers` 그룹에 쌓인 요청이 없는지 본다(워커는 꺼 둔 상태).

```bash
# 그룹 LAG 가 0 인지 확인한다 (Day 30 끝에는 0 이었다)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group agent-workers
```

LAG 가 0 이 아니면 워커가 꺼진 채로 읽을 위치를 토픽 끝으로 옮긴다.

```bash
# 워커가 모두 꺼져 있어야 실행된다
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --topic agent.requests --reset-offsets --to-latest --execute
```


## 5. 실행과 확인

### ① 가짜 모델로 시나리오 7개 확인

**목적:** Kafka, Redis, Qdrant, 모델 API 없이 그래프만 돌려, 실패 종류에 따라 경로가 갈리고 한도에서 멈추며 지표가 세어지는지 확인한다. 대기 시간이 없어 1초 안에 끝난다.

```bash
# 프로젝트의 d2-ai-python 폴더에서 실행한다
cd d2-ai-python
```

```bash
# 시나리오 A~G 를 돌려 PASS/FAIL 을 출력한다
python -m app.checks.selfcorrect_check
```

시나리오와 기대값:

| 시나리오                     | 질문과 가짜 검색 상태                       | 도구 실행 | 모델 호출 | retry | correction | 최종 답                                      |
| ---------------------------- | ------------------------------------------- | --------- | --------- | ----- | ---------- | -------------------------------------------- |
| A 정상                       | 실패 없음                                   | 1         | 2         | 0     | 0          | "검색 결과 ...자를 받아 답했습니다"            |
| B 일시 장애 1번 후 성공      | 검색이 처음 1번 연결 실패                   | 2         | 2         | 1     | 0          | 같은 답. 모델은 재실행을 모른다                |
| C 일시 장애가 계속           | 검색이 항상 연결 실패                       | 2         | 1         | 2     | 0          | "2번 시도했지만 요청을 처리하지 못했습니다"    |
| D 입력 오류 후 정정 성공     | 질문에 `교정테스트`                         | 2         | 3         | 0     | 1          | "검색 결과 ...자를 받아 답했습니다"            |
| E 입력 오류를 못 고침        | 질문에 `교정불가`                           | 3         | 3         | 0     | 3          | "검색 조건을 바꿔 2번 다시 시도했지만 ..."     |
| F `강제실패` (Day 6 회귀)    | 질문에 `강제실패`                           | 2         | 1         | 2     | 0          | "2번 시도했지만 요청을 처리하지 못했습니다"    |
| G 정정 뒤 일시 장애          | D 에 검색 연결 실패 1번을 겹침              | 3         | 3         | 1     | 1          | "검색 결과 ...자를 받아 답했습니다"            |

그 밖의 기대:

| 항목                           | 기대                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| D, G 의 첫 실패 결과           | 실패 원인, `[정정 안내]`, "남은 정정 기회: 2번" 이 들어 있다                                                  |
| E 의 실패 결과 3개             | 남은 정정 기회가 2번 → 1번으로 줄고, 마지막(포기) 결과에는 정정 안내가 없다                                    |
| 지표 합계(7개 시나리오 전체)   | 실패 transient 6, input 5 / 복구 retry 2, correction 1 / 포기 retry_exhausted 2, correction_exhausted 1       |
| 마지막 줄                      | "모든 항목 PASS"                                                                                              |


### ② 워커를 거치는 전체 경로

**목적:** Spring, Kafka, 워커, 실제 Qdrant 를 거치는 경로에서도 ① 과 같은 흐름이 되는지, 워커의 `/metrics` 에 새 지표가 나오는지 확인한다. 모델은 `fake-agent` 라서 흐름이 정해져 있고, 검색은 실제 Qdrant 와 임베딩 API 를 쓴다. 워커는 1개(8000)만 띄운다.

```bash
# 워커 1개를 띄운다 (가짜 모델을 켜야 provider 를 fake-agent 로 보낼 수 있다)
cd d2-ai-python && FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1 uvicorn app.main:app --port 8000
```

다른 터미널에서 요청 세 건을 보낸다. 각 응답의 `requestId` 를 적어 둔다.

```bash
# 요청 1: 정상 질문
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"fake-agent"}'
```

```bash
# 요청 2: 입력 오류 후 정정이 되는 질문
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" -d '{"question":"교정테스트 환불은 며칠 안에 신청해야 하나요?","provider":"fake-agent"}'
```

```bash
# 요청 3: 입력 오류를 못 고치는 질문
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" -d '{"question":"교정불가 환불은 며칠 안에 신청해야 하나요?","provider":"fake-agent"}'
```

```bash
# 각 요청의 결과 조회 (<requestId> 에 위에서 적은 값)
curl -s localhost:8000/api/v1/agent/results/<requestId>
```

```bash
# 워커의 새 지표
curl -s localhost:8000/metrics | grep -E '^(agent_tool|agent_give_ups|agent_worker_messages)'
```

확인 전 예상:

| 항목                                      | 예상                                                                                                                         |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 요청 1 결과                               | `DONE`. 답이 "[fake-agent] 검색 결과 ...자를 받아 답했습니다"                                                                 |
| 요청 2 결과                               | `DONE`. 답은 요청 1 과 같은 모양(정정 1번 뒤 성공)                                                                            |
| 요청 3 결과                               | `DONE`(워커 입장에서는 정상 완료). 답이 "죄송합니다, 검색 조건을 바꿔 2번 다시 시도했지만 ..."                                |
| `agent_tool_failures_total{kind="input"}` | 4 (요청 2 에서 1번, 요청 3 에서 3번)                                                                                           |
| `agent_tool_failures_total{kind="transient"}` | 줄 자체가 나오지 않는다(한 번도 세지 않으면 카운터 줄이 생기지 않는다)                                                    |
| `agent_tool_recoveries_total{via="correction"}` | 1                                                                                                                       |
| `agent_give_ups_total{reason="correction_exhausted"}` | 1                                                                                                                 |
| `agent_worker_messages_total{result="ok"}` | 3 (포기 답도 메시지 처리는 성공이다)                                                                                         |
| 임베딩 API 호출                           | 요청 1 에서 1번, 요청 2 의 정정 호출에서 1번. 입력 오류는 임베딩 전에 걸러져 호출하지 않는다                                  |

확인이 끝나면 워커를 끈다(Ctrl+C).


### ③ 실제 모델이 정정하는지

**목적:** 실제 Gemini 와 Claude 가 `[정정 안내]` 를 읽고 검색어를 고치는지 본다. 이 경로는 모델 판단에 달려 있어서 ①② 와 달리 결과를 미리 정할 수 없다. 그래프의 `[DEBUG] after_tools_router` 줄에서 실패 종류와 정정 횟수를 읽는다. Qdrant 가 떠 있고 `.env` 에 키가 있어야 한다.

```bash
# Gemini 로 실행 (d2-ai-python 폴더에서)
cd d2-ai-python && python -c "from app.services.agent_graph import run_agent; print(run_agent('교정테스트 환불 정책 알려줘', provider='gemini'))"
```

```bash
# Claude 로 실행 (d2-ai-python 폴더에서)
cd d2-ai-python && python -c "from app.services.agent_graph import run_agent; print(run_agent('교정테스트 환불 정책 알려줘', provider='claude'))"
```

확인 전 예상(어느 쪽이 나올지는 모른다):

| 가능한 결과                                                                                           | 출력에서 보이는 것                                                                                              |
| ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 가. 모델이 `교정테스트` 를 검색어에 넣었다가 정정 안내를 읽고 단어를 빼 다시 부름                      | `failed=True, ..., correction_count=1, kind=input` 줄 뒤 `failed=False` 줄, 환불 정책 답                          |
| 나. 모델이 처음부터 단어를 검색어에서 뺌(Day 11 에서 괄호 키워드를 뺀 것과 같은 현상)                   | `failed=True` 줄이 없고 바로 `failed=False`. 정정 경로를 타지 않았으므로 이 질문으로는 판단할 수 없다               |
| 다. 정정 안내를 읽고도 다시 부르지 않고 사과로 끝냄(Day 6 에서 본 현상)                                 | `failed=True, correction_count=1, kind=input` 줄 하나 뒤 사과 답. `give_up` 로그는 없다                           |
| 라. 단어를 못 빼고 같은 검색어로 계속 부름                                                            | `correction_count=1, 2, 3` 줄 뒤 `give_up` 진입 로그와 "검색 조건을 바꿔 2번 ..." 답                              |


## 6. 알려진 한계

- ①② 의 모델은 정해진 대로 움직이는 가짜라서, 정정 경로의 배선(분기, 한도, 지표)은 검증하지만 "실제 모델이 잘 고치는가"는 ③ 에서만 본다.
- 입력 오류의 판정은 예외 종류로만 한다. 도구가 `ToolInputError` 를 던지는 곳은 지금 검색 도구의 빈 검색어와 테스트 단어뿐이다. 검색 결과가 0건이거나 점수가 낮은 경우는 실패로 보지 않는다(오늘 범위 밖).
- 테스트 단어 `교정테스트`, `교정불가` 는 `강제실패` 처럼 검색 도구 안에 시험용으로 남겨 둔 분기다.
- 일시 장애 재시도에는 대기 시간이 없다(Day 6 그대로). 곧바로 다시 실행한다.
- 정정 안내를 모델이 어떻게 따르는지는 모델과 프롬프트에 따라 다르다. 안내는 도구 결과 문자열에 붙이는 방식이라 모델이 무시할 수 있다.
- 지표는 워커 프로세스별로 따로 센다. Grafana 대시보드에는 아직 이 지표 패널을 넣지 않았다(Prometheus 에서는 질의할 수 있다).


## 7. 체크리스트

- [x] ① `python -m app.checks.selfcorrect_check` 가 모든 항목 PASS
- [x] ② 워커를 거쳐 요청 3건이 기대대로 처리되고 지표가 나옴
- [x] ③ 실제 모델(Gemini, Claude)로 정정 여부 확인
- [x] DAY31_RESULT.md 작성
