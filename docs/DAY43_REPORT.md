# Day 43 — 코드 리팩토링, 주석 정리

**목적:** 저장소를 처음 여는 사람이 코드를 README 와 같은 구조로 읽을 수 있게 한다. 동작은 바꾸지 않는다(로그 출력 방식만 바꿨다).

## 1. 바꾼 것

| 순서 | 작업 | 내용 |
| --- | --- | --- |
| ① | `[OLD]`, `[NEW]` 표시 | 옛 코드를 남긴 `[OLD]` 주석 줄을 지웠다(옛 코드는 git 이력에 있다). `[NEW] Day N:` 는 `Day N:` 으로 바꿔 어느 Day 에 생긴 코드인지는 남겼다. Python, Java, k6, 셸 스크립트 24개 파일 |
| ① | 끝난 TODO, 맞지 않는 설명 | `agent_graph.py` 맨 위 설명을 지금 흐름(Day 3~36)으로 다시 썼다. 끝난 TODO(6주 차 Self-Correction 등), 쓰지 않는 옛 도구 실행 코드 주석, 옛 모델 이름 주석을 지웠다. `main.py`, `routers/agent.py`, `routers/health.py` 의 설명을 지금 역할에 맞췄다 |
| ② | `print` → `logging` | `agent_graph.py` 의 `[timing]` 줄은 INFO, `>>> [DEBUG]` 줄은 DEBUG 로 바꿨다. 형식은 메시지만이라 예전 출력과 모양이 같다. 로거는 지금의 표준 출력에 써서 확인 스크립트의 출력 숨기기(`redirect_stdout`)도 그대로 동작한다. 실험, 확인 스크립트의 결과 출력은 `print` 그대로다 |
| ③ | `agent_core.py` | Day 2~5 의 첫 에이전트. 어디에서도 가져오지 않아(`routers/agent.py` 의 import 는 주석이었다) `archive/` 로 옮겼다 |
| ④ | 폴더 나누기 | `app/services/` 를 셋으로 나눴다. 아래 2절 |
| ④ | FastAPI 표준 구조 | 모든 import 를 `from app.services import ...` 처럼 `app.` 으로 시작하게 바꿨다. 서버도 `app` 폴더로 들어가지 않고 `d2-ai-python` 폴더에서 `uvicorn app.main:app` 으로 켠다. `app` 안에서만 되던 `try/except ModuleNotFoundError` 이중 import 12곳은 하나로 합쳤다. 아래 2절 |
| ⑤ | 초기 흔적 | `claude/`, `d1-ai-agent/`, `d2-ai-python` 의 `ai_test.py`, `test.py`, `weather-ai.py`, `READEME.md`, `CODE_ANALYSIS.md` 를 `archive/` 로 옮겼다([archive/README.md](../archive/README.md)) |
| - | README | 7-3 "코드 위치" 표를 더했다 |

## 2. 폴더 나누기

| 폴더 | 파일 | 실행 |
| --- | --- | --- |
| `app/services/` (서버 코드 16개) | agent_graph, agent_handler, conversation_store, embedding, fake_llm, kafka_worker, metrics, model_fallback, qdrant_ingest, qdrant_search, qdrant_setup, rag_compare, redis_client, redis_lock, result_store, ttl_policy | 서버가 가져다 쓴다 |
| `app/checks/` (확인 13개) | agent_e2e, dlq_check, dlq_tool, kafka_consumer_test, kafka_producer_test, llm_check, lock_check, model_fallback_check, redis_check, result_check, selfcorrect_check, system_verify, ttl_check | `python -m app.checks.<이름>` |
| `app/experiments/` (실험 8개) | chunk_experiment, hnsw_experiment, lock_stress, partition_bench, race_test, rag_eval, score_noise, search_eval | `python -m app.experiments.<이름>` |

- 실행 명령이 바뀌었다. 예: `python -m app.services.system_verify` → `python -m app.checks.system_verify`, `python -m app.services.rag_eval` → `python -m app.experiments.rag_eval`. `dlq_check` 도 같은 방식이다(`python -m app.checks.dlq_check`). 서버는 `d2-ai-python` 폴더에서 `uvicorn app.main:app --port 8000` 으로 켠다.
- 코드 속 실행 예시, `system_verify` 의 실행 목록, `docs/` 의 가이드와 결과 문서 74개 파일의 경로(249곳)를 같이 바꿨다.
- 서로 가져오는 곳 두 군데를 고쳤다: `dlq_check` → `app.checks.dlq_tool`, `rag_eval` → `app.experiments.chunk_experiment`.
- `docs/` 의 Day 별 GUIDE, CODE_GUIDE 중 서버 실행 명령이 있는 20개 문서와 그 밖의 `app` 폴더 기준 명령을 새 방식(`cd d2-ai-python`, `uvicorn app.main:app`, `python -m app.checks.<이름>`)으로 고쳤다. 그날 실제로 한 일을 적은 RESULT 문서의 명령은 그날 기록이라 그대로 두었다. Day 3 가이드의 `python services/agent_graph.py` 는 Day 3 당시 코드라 그대로다.
- 모든 import 를 `app.` 로 시작하게 바꿨다(예: `from services import redis_client` → `from app.services import redis_client`). `app` 안에서만 되던 예전 `try/except ModuleNotFoundError` 이중 import 12곳은 하나로 합쳤다. 그래서 서버, 확인, 실험이 모두 `d2-ai-python` 폴더에서 같은 방식으로 실행된다.

## 3. 확인한 것

| 확인 | 결과 |
| --- | --- |
| 문법 | `python -m compileall app` 통과 |
| import 경로 | 모든 `from app.... import` 74곳의 모듈과 이름이 실제로 있는지 정적으로 확인하고, 옛 방식(`from services ...`)이 남지 않았는지도 확인. 문제 0 |
| 동작이 바뀌지 않았는지 | 바꾼 Python 파일의 구문 트리(AST, 설명문 제외)를 바꾸기 전과 비교. 다른 곳은 import 경로(`app.` 접두사와 이중 import 정리), `system_verify` 실행 목록, `agent_graph.py` 의 로그 출력뿐이다. Java, k6, 셸 스크립트는 주석만 바뀌었다 |
| 로그 출력 | 로거 설정만 따로 실행해 `[timing]` 줄 모양, DEBUG 숨김, `redirect_stdout` 로 숨기기가 되는지 확인 |

**사용자가 실행해 확인한 것(2026-10-23, `d2-ai-python` 폴더, `app.` import 로 바꾼 뒤):**

| 실행 | 결과 |
| --- | --- |
| `python -m app.checks.system_verify --only offline` | PASS 7, FAIL 0, SKIP 0 (redis_check, result_check, ttl_check, lock_check, selfcorrect_check, dlq_check, lock_stress) |
| `uvicorn app.main:app --port 8000` 로 워커 w1 을 켜고 `--only live` | PASS 11, FAIL 0, SKIP 0. 워커 `/health`(파티션 [0, 1, 2]), Spring, `/metrics`, 가짜 모델 end-to-end 1.5초, 세션 대화 이어가기(저장된 턴 2), 입력 검증 400, Grafana, 컨슈머 lag 0 |
| `d3-was-spring` 에서 `./gradlew test` | BUILD SUCCESSFUL (3초) |

`--only live` 의 Prometheus 항목에는 `up 이 아닌 대상: fastapi@host.docker.internal:8001` 이 나왔다. 이번에 워커를 w1(8000) 하나만 켰기 때문이며, 점검은 PASS 다.

## 4. 달라진 점

- 서버를 켜는 명령이 바뀌었다. 예전: `cd d2-ai-python/app` 후 `uvicorn main:app --port 8000`. 지금: `cd d2-ai-python` 후 `uvicorn app.main:app --port 8000`. `run.sh`, `scripts/bench-workers.sh`, Day 별 GUIDE 도 이 방식이다. 새 `d2-ai-python/README.md` 에 실행 방법을 적었다.
- `>>> [DEBUG] after_tools_router 호출됨`, `give_up_node 진입함` 줄은 이제 기본으로 나오지 않는다. 보려면 워커를 `AGENT_LOG_LEVEL=DEBUG` 로 띄운다. Day 6, 31, 36 결과 문서의 화면에는 이 줄이 찍혀 있다(그때의 출력이다).
- `image.png`, `10월_일정표.xlsx` 는 git 에 들어 있지 않은 파일이라 옮기지 않았다.
