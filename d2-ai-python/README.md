# d2-ai-python — FastAPI 워커

Spring(WAS)이 Kafka 에 넣은 에이전트 요청을 받아 LangGraph 에이전트로 처리하고, 결과를 Redis 에 저장하는 Python 서버다. 프로젝트 전체 설명은 루트 [README.md](../README.md) 를 본다.

## 사용하는 것

| 프레임워크·도구 | 하는 일 |
| --- | --- |
| FastAPI + uvicorn | HTTP 서버. 에이전트 직접 실행, RAG 비교, 결과 조회, 대화 기록, `/health`, `/metrics` |
| LangGraph / LangChain | 검색 도구를 쓰는 에이전트(Gemini, Claude, OpenAI 모델 연결) |
| aiokafka Consumer | `agent.requests` 토픽을 읽는 워커. 서버가 뜰 때 같이 시작한다(`lifespan`) |
| Redis | 요청 결과, 대화 기록, 같은 세션 동시 처리를 막는 락 |
| Qdrant | 문서 벡터 검색(RAG) |
| prometheus-fastapi-instrumentator | `/metrics` 로 HTTP 요청 수와 응답 시간을 연다 |

## 폴더

```
d2-ai-python/            ← 여기서 가상환경을 켜고 모든 명령을 실행한다
├── venv/  .env  requirements.txt  run.sh
└── app/
    ├── main.py          FastAPI 앱 진입점(라우터 등록, Kafka 워커 시작/종료)
    ├── routers/         엔드포인트: agent, compare, results, sessions, health
    ├── schemas/         요청·응답 모델(pydantic)
    ├── services/        서버가 쓰는 코드: 에이전트, Kafka 워커, Redis, Qdrant, 모델 폴백, 지표
    ├── data/            검색 실험용 문서와 질문
    ├── checks/          확인 스크립트(PASS/FAIL)
    └── experiments/     실험 스크립트(청크, HNSW, 락, 파티션 처리량, RAG 비교)
```

모든 import 는 `from app.services import ...` 처럼 `app.` 으로 시작한다. 그래서 서버와 스크립트를 전부 `d2-ai-python` 폴더에서 실행한다.

## 준비

```bash
cd d2-ai-python
python -m venv venv                 # 처음 한 번
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                # GOOGLE_API_KEY 등을 채운다
```

Redis, Qdrant, Kafka 는 루트의 `docker-compose.yml` 로 띄운다.

## 실행

```bash
cd d2-ai-python
source venv/bin/activate
uvicorn app.main:app --port 8000          # 개발 중에는 --reload 를 붙인다. source run.sh 도 같다
```

워커를 여러 대 띄울 때는 포트와 `KAFKA_WORKER_ID` 를 다르게 준다. 모델 호출 없이 쓰려면 가짜 모델을 켠다.

```bash
export KAFKA_WORKER_ID=w1 FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1
uvicorn app.main:app --port 8000
```

- 자동 문서: <http://localhost:8000/docs>
- 확인: `python -m app.checks.<이름>`, 통합 확인은 `python -m app.checks.system_verify`
- 실험: `python -m app.experiments.<이름>`

## 엔드포인트

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| GET | `/health` | 상태(Kafka 워커, Redis 등) |
| GET | `/metrics` | Prometheus 지표 |
| POST | `/api/v1/agent/run` | 에이전트를 바로 실행하고 응답을 기다린다(동기) |
| GET | `/api/v1/agent/results/{requestId}` | Kafka 로 처리한 요청의 결과 조회 |
| GET / DELETE | `/api/v1/agent/sessions/{sessionId}` | 대화 기록 조회, 삭제 |
| POST | `/api/v1/rag/compare` · `/ask` · `/ask-plain` | RAG 유무 비교, 문서 검색 답변, 검색 없는 답변 |

요청·응답 필드는 [docs/API.md](../docs/API.md) 에 있다.

## 자주 쓰는 환경변수

| 변수 | 기본값 | 뜻 |
| --- | --- | --- |
| `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` | 없음 | 모델 키 |
| `KAFKA_BOOTSTRAP` / `KAFKA_TOPIC` / `KAFKA_GROUP` | `localhost:9092` / `agent.requests` / `agent-workers` | Kafka 접속 |
| `KAFKA_WORKER_ID` | `worker-<pid>` | 워커 이름(로그와 결과에 남는다) |
| `KAFKA_CONSUMER_ENABLED` | `true` | `false` 면 HTTP 만 받고 Kafka 는 읽지 않는다 |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis |
| `QDRANT_HOST` / `QDRANT_PORT` | `localhost` / `6333` | Qdrant |
| `FAKE_LLM_ENABLED` / `FAKE_LLM_DELAY_SEC` | `false` / — | 부하 테스트용 가짜 모델 |
| `AGENT_TIMEOUT_SEC` | `120` | 에이전트 한 건 최대 시간 |
| `AGENT_LOG_LEVEL` | `INFO` | `DEBUG` 면 에이전트 내부 로그까지 나온다 |

전체 목록과 설명은 `.env.example` 에 있다.
