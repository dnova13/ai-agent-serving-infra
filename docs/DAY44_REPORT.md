# Day 44 — `.env.example`, 실행 가이드, GitHub 클린업

**목적:** 저장소를 처음 받은 사람이 키를 어디에 넣고 어떤 순서로 켜는지 알 수 있게 하고, 공개해도 되는 상태로 정리한다.

## 1. 바꾼 것

| 작업 | 내용 |
| --- | --- |
| `d2-ai-python/.env.example` 다시 쓰기 | 코드가 읽지 않는 `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_AGENT_TASKS` 를 지우고 코드가 읽는 `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC` 으로 바꿨다. 코드에는 있고 예시에는 없던 항목(Kafka 워커, 모델, 가짜 모델, 폴백, Redis 락, 결과 저장 TTL)을 더했다 |
| 구성 | "필수"(`GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`), "접속 주소", "실험용" 세 칸. 기본값이 있는 항목은 `#` 로 막아 두었고 바꿀 때만 푼다. 실제 키는 넣지 않았다 |
| `RUN_GUIDE.md` (새 파일, 한눈에 보이도록 저장소 루트에 둔다) | 준비물, 컨테이너, 파이썬 환경과 키, 워커, Spring, 요청 한 번, 전체 확인, 끄기, 막힐 때 |
| README | 7번 "문서 안내" 맨 위에 RUN_GUIDE 링크 |
| `d2-ai-python/README.md` | 환경변수 전체 목록은 `.env.example` 을 보게 했다 |
| `requirements.txt` | `d2-ai-python/requirements.txt` 와 `d2-ai-python/app/requirements.txt` 가 따로 있었고 앞의 것에는 `langgraph`, `prometheus-fastapi-instrumentator` 등 12개가 빠져 있었다. `app/` 쪽 내용으로 `d2-ai-python/requirements.txt` 하나로 합치고 `app/requirements.txt` 를 지웠다. 설치는 `d2-ai-python` 에서 `pip install -r requirements.txt`. RUN_GUIDE, `d2-ai-python/README.md`, DAY30 가이드의 경로를 고쳤다(DAY13, DAY19 가이드는 그날 기록이라 그대로) |
| 개인 절대 경로 | 가이드 6개의 `cd "/Users/<이름>/My home/.../d2-ai-python"` 를 `cd d2-ai-python` 으로(Day 7 의 `d1-ai-agent` 는 `archive/d1-ai-agent`) 바꿨다 |
| `.git` 정리 | 앞선 커밋 중 남은 `stale-*` 임시 파일 160개를 지웠다. 사용자가 만든 `.git/day23-snapshot`, `day24-wip`, `gk` 는 건드리지 않았다 |

## 2. 확인한 것

| 확인 | 결과 |
| --- | --- |
| `.env.example` 과 코드 | 코드의 `os.getenv` 이름 46개가 모두 예시에 있고, 예시에만 있는 이름은 없다 |
| 무시 목록 | `docker-data/`, `venv/`, `build/`, `.DS_Store`, `__pycache__/`, `.env`, `image.png`, `.xlsx` 모두 `.gitignore` 에서 걸러진다 |
| RUN_GUIDE 의 값 | 포트, 컨테이너 이름, 엔드포인트, 응답 필드(`requestId`, `status`, `answer`), 실행 명령을 `docker-compose.yml`, 코드, `system_verify` 에서 맞춰 보았다 |

**docker-data 와 그라파나, 프로메테우스:** 대시보드 그림의 틀(패널, 데이터 소스)은 git 에 있는 `monitoring/` 에서 읽는다(`grafana/dashboards/ai-serving.json`, `provisioning/`). 그래서 `docker-data/` 없이도 컨테이너를 켜면 같은 대시보드가 나온다. 다만 그래프에 그려질 값은 `docker-data/prometheus`(최대 7일 보관)에 쌓인 것이라 새로 받은 환경에서는 비어 있고, 서버와 k6 를 돌려야 채워진다. 검색용 문서(Qdrant)는 `python -m app.services.qdrant_ingest` 로 다시 넣는다.

**사용자가 확인한 것(2026-10-23):** `RUN_GUIDE.md` 를 처음부터 따라 해 보았고 된다고 확인했다.

**참고:** 사용자의 `.env` 에는 옛 키 `KAFKA_TOPIC_AGENT_TASKS` 가 남아 있지만 코드가 읽지 않아 동작에는 영향이 없다. 지워도 된다.
