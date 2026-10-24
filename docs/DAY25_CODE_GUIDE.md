# Day 25 코드 가이드 — 가짜 LLM 모드와 k6 ramping 스크립트

> 실행하는 방법과 기대값은 [DAY25_GUIDE.md](DAY25_GUIDE.md). 이 문서는 바뀐 코드가 어떻게 동작하는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이며 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, 6379 = Redis, 9092 = Kafka.
> 상태: 가짜 LLM 은 샌드박스에서 실행해 확인했다(켜면 지연 후 응답, 꺼져 있으면 `ValueError`). k6 스크립트는 샌드박스의 k6 v0.54.0 과 가짜 HTTP 서버로 확인했다. 실제 Spring, Kafka, 워커로는 아직 돌리지 않았다.

**전체 목적:** 실제 LLM 없이 수백 건의 요청을 서버 구조 그대로 흘려보내려고, `provider: fake` 가 모델 호출을 "정해진 시간 기다리는 가짜"로 바꾸는 지점과, 그 구조에 가상 사용자를 올렸다 내리는 k6 스크립트가 무엇을 재는지 따라간다.


## 1. 전체 흐름

```
k6 (VU 0→N→0)
  └─ MODE=async: POST 8080 /api/agent/requests {"question","provider":"fake"} → 202 {requestId}
        └─ Kafka → 워커(8000, 8001) → agent_handler → run_agent → AGENT_GRAPH
              agent_node → get_model("fake") → FakeChatModel.bind_tools → invoke
                  time.sleep(5초 ±20%) → AIMessage("[fake] 가짜 응답입니다 ...")   (tool_calls 없음 → 그래프 종료)
              → Redis 결과 DONE
     POLL=1 이면: GET 8000 /api/v1/agent/results/{requestId} 를 1초마다, DONE 이 될 때까지 → e2e_ms
  └─ MODE=sync : POST 8000 /api/v1/agent/run {"query","provider":"fake"} → (답이 나올 때까지 기다림) 200
```

| 단계 | 하는 일                    | 파일                                              | 함수 / 항목                                        |
| ---- | -------------------------- | ------------------------------------------------- | -------------------------------------------------- |
| A    | 가짜 모델                  | `d2-ai-python/app/services/fake_llm.py`           | `enabled`, `FakeChatModel.bind_tools`, `FakeChatModel.invoke`, `_float_env` |
| B    | 모델 선택 지점             | `d2-ai-python/app/services/agent_graph.py`        | `get_model` 의 `provider == "fake"` 분기, 파일 위쪽 `fake_llm` import |
| C    | 스모크                     | `scripts/k6/smoke.js`                             | `default`                                          |
| D    | ramping 시나리오           | `scripts/k6/ramping.js`                           | `options.scenarios.ramp`, `runAsync`, `runSync`, `default` |


## 2. 주요 코드

### 2-1. 가짜 모델 (`fake_llm.py`)

```python
class FakeChatModel:
    def bind_tools(self, tools):
        return self
    def invoke(self, messages):
        delay = max(0.0, base * (1 + random.uniform(-jitter, jitter)))
        time.sleep(delay)
        return AIMessage(content=f"[fake] 가짜 응답입니다 (지연 {delay:.1f}초)")
```

| 부분                              | 설명                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 메서드 두 개만                    | `agent_node` 는 `get_model(...)` 로 받은 모델에 `bind_tools(AVAILABLE_TOOLS)` 를 부르고 `.invoke(메시지 목록)` 의 결과를 쓴다. 그 두 메서드만 같은 모양으로 만들었다(LangChain 모델을 상속하지 않는 단순 흉내) |
| `bind_tools` 가 `self`             | 도구를 쓰지 않으므로 그대로 돌려준다                                                                                                    |
| 도구 요청 없음                    | `AIMessage` 의 `tool_calls` 는 비어 있어, 그래프는 첫 응답에서 종료한다. 그래서 임베딩 API 와 Qdrant 는 호출되지 않고 LLM 호출은 1번이다   |
| `time.sleep`                      | 실제 모델 SDK 호출처럼 그 스레드를 막는다. 워커에서는 `asyncio.to_thread` 의 스레드가, `/agent/run` 에서는 이벤트 루프 자체가 막힌다(Day 26 에서 볼 차이) |
| 지연                              | `FAKE_LLM_DELAY_SEC`(기본 5), `FAKE_LLM_JITTER`(기본 0.2). 0 미만이면 `ValueError`. 호출마다 값을 읽어서 서버를 다시 띄워야 바뀐다(프로세스 환경변수이므로) |
| 켜는 조건                         | `enabled()` 가 `FAKE_LLM_ENABLED=true` 일 때만 참. 꺼져 있으면 `provider: fake` 는 기존 오류(`지원하지 않는 provider입니다`)로 거절되어 실수로 쓰이지 않는다 |

### 2-2. 모델 선택 (`agent_graph.get_model`)

```python
# [NEW Day25] 부하 테스트용 가짜 모델. FAKE_LLM_ENABLED=true 일 때만 켜진다
elif provider == "fake" and fake_llm.enabled():
    return fake_llm.FakeChatModel()
else:
    raise ValueError(f"지원하지 않는 provider입니다: {provider}")
```

기존 `gemini`, `claude` 분기는 그대로이고 그 뒤에 분기 하나를 끼웠다. `fake_llm` import 는 파일의 다른 서비스 import 와 같은 `try/except ModuleNotFoundError` 패턴(서버 실행과 `-m` 실행 둘 다 지원) 안에 넣었다. `agent_core.py` 의 옛 `get_model` 은 건드리지 않았다(지금 서버가 쓰지 않는 경로).

### 2-3. ramping 시나리오 (`ramping.js`)

| 부분                                  | 설명                                                                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `executor: 'ramping-vus'`             | `stages` 가 `RAMP_UP → HOLD → RAMP_DOWN` 순서로 `0 → MAX_VUS → MAX_VUS → 0`. 기본 `30s, 30s, 10s`                                       |
| `default` 함수                        | `MODE` 에 따라 `runAsync` 또는 `runSync` 를 한 번 하고 `THINK_SEC`(기본 1초) 쉰다. VU 마다 이것이 반복된다                              |
| `runAsync`                            | Spring 에 POST, `202` 인지 `check`. `POLL=1` 이면 `requestId` 로 1초마다 결과를 조회해 `DONE` 이면 `e2e_ms`(접수 전부터 DONE 까지)와 `results_done`, `FAILED` 면 `results_failed`, `POLL_MAX_SEC` 안에 못 끝나면 `results_poll_timeout` 에 기록 |
| `runSync`                             | `/api/v1/agent/run` 에 POST(`timeout` = `POLL_MAX_SEC` 초), `200` 인지 `check`. Day 26 의 504 재현에 쓴다                                 |
| `accepted` (Rate)                     | 접수/실행 성공 비율. async 는 202, sync 는 200 일 때 성공                                                                                |
| 결과 조회의 정상 응답 범위            | 결과 조회(`POLL=1`)는 `responseCallback: http.expectedStatuses(200, 404)` 로 200 과 404 를 정상으로 둔다. 요청이 아직 워커에 넘어가지 않았으면 결과 키가 없어 404 가 오는 것이 정상인데, k6 기본은 404 를 실패로 세어 `http_req_failed` 가 부풀려진다(Day 25 ⑤ 첫 실행에서 67.91%, 수정 전 스크립트). 접수 요청은 이 설정이 없어 202 외 응답은 계속 실패로 센다. 샌드박스에서 가짜 서버(처음 1초 404)로 확인했고 이 변경 뒤 실제 환경에서는 다시 돌리지 않았다 |
| 요청 이름표(`tags.name`)              | 접수 요청과 결과 조회 요청을 `http_req_duration` 에서 따로 보려고 붙였다. 임계값도 접수 요청에만 건다(`http_req_duration{name:...}`). `POLL=1` 의 조회 요청이 접수 응답 시간 통계에 섞이지 않게 하기 위해서다 |
| 임계값                                | `accepted` 99% 이상, 접수 `p(95)` 2초 미만. 오늘 정한 임시값이며 시험 중단 조건은 아니다(`abortOnFail` 없음). 요약의 ✓/✗ 표시용이다       |
| 질문                                  | 5개를 무작위로 고른다. 가짜 모델은 질문 내용을 쓰지 않으므로 응답에는 영향이 없다                                                       |
| 환경변수                              | `MODE`, `MAX_VUS`, `RAMP_UP`, `HOLD`, `RAMP_DOWN`, `THINK_SEC`, `PROVIDER`(기본 `fake`), `POLL`, `POLL_MAX_SEC`, `SPRING`, `FASTAPI`     |

### 2-4. 스모크 (`smoke.js`)

VU 1명이 1번 반복(`iterations: 1`). Spring 에 1건 보내 `202` 를 확인하고, 1초마다 최대 60번 결과를 조회해 `DONE` 이면 완료 시간과 `worker`, `partition`, `answer` 를 `console.log` 로 출력한다. `FAILED` 이거나 60초 안에 `DONE` 이 아니면 `fail()` 로 시험을 실패 처리한다.


## 3. 결과를 읽는 순서

1. 스모크에서 접수 시간(ms)과 완료 시간(약 5000ms)이 분리되어 보이는지 본다.
2. ramping 요약에서 `accepted`, 접수 요청의 `p(95)`, `http_req_failed` 를 본다(접수 단계의 건강).
3. `POLL=1` 이면 `e2e_ms` 의 평균과 `p(95)`, `results_poll_timeout` 을 본다(완료 단계의 건강). 접수 p(95) 와 `e2e_ms` 의 차이가 이 구조의 특징이다.
4. 시험이 끝나면 `kafka-consumer-groups` 로 그룹의 lag 이 얼마나 남았는지, 필요하면 DAY25_GUIDE 6번의 비우기 명령을 쓴다.


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                                  |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| 실제 Spring, Kafka, 워커 실행 | 하지 않았다. 샌드박스에서는 `run_agent(..., provider="fake")` 를 LangGraph 로 실행했고(Qdrant, 대화 저장, 락은 빈 모듈로 대체), k6 는 가짜 HTTP 서버(접수 202, 2초 뒤 DONE, `/agent/run` 2초 대기)에 돌렸다 |
| `FakeChatModel` 과 실제 호출 경로 | 워커를 거치는 `agent_handler` 의 `to_thread` 와 `wait_for(120초)` 안에서 도는 것은 확인하지 않았다                                                       |
| 임계값 표시                   | 샌드박스에서 async 는 ✓, sync(2초 대기)는 `http_req_duration{name:POST /api/v1/agent/run}` 임계값이 ✗ 로 나오는 것을 확인했다. 실제 값은 실행해야 안다             |
| k6 버전 차이                  | 샌드박스 검증은 k6 v0.54.0 이고, 내 환경은 k6 v1.4.2(darwin/arm64)다. 같은 스크립트가 v1.4.2 에서 스모크와 ramping(④~⑥)을 정상 실행했다(DAY25_RESULT). 두 버전의 요약 출력 모양 차이는 비교하지 않았다 |
