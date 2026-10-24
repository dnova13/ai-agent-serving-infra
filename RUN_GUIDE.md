# 처음부터 실행하는 법

저장소를 처음 받은 사람이 서버를 켜고 요청 한 번을 끝까지 보내 보는 순서다. 프로젝트 설명은 [README.md](README.md), 서버별 설명은 [d2-ai-python/README.md](d2-ai-python/README.md), [d3-was-spring/README.md](d3-was-spring/README.md) 에 있다.

**흐름:** Spring(8080)이 요청을 Kafka 에 넣고 바로 202 로 응답한다 → FastAPI 워커(8000)가 Kafka 에서 꺼내 에이전트를 실행하고 결과를 Redis 에 저장한다 → 클라이언트가 `requestId` 로 결과를 조회한다.

## 1. 준비물

| 도구 | 용도 | 확인 |
| --- | --- | --- |
| Docker (Compose 포함) | Kafka, Redis, Qdrant, Prometheus, Grafana | `docker compose version` |
| Python 3.10 이상 | FastAPI 워커 | `python --version` |
| Java 21 | Spring (Gradle 래퍼가 들어 있어 Gradle 은 따로 설치하지 않는다) | `java -version` |
| k6 (선택) | 부하 시험(`scripts/k6/`)을 돌릴 때만 | `k6 version` |
| Google AI Studio 키 | Gemini 답변과 문서 임베딩. [발급](https://aistudio.google.com/apikey) | |
| Anthropic 키 (선택) | Claude 로 답하거나 폴백으로 쓸 때만 | |

가짜 모델(`FAKE_LLM_ENABLED=true`)만 쓰면 모델 호출 없이 흐름을 확인할 수 있다. 다만 에이전트가 문서를 검색할 때 임베딩이 필요해서 `GOOGLE_API_KEY` 는 있어야 한다.

## 2. 컨테이너 켜기

저장소 루트에서 실행한다.

```bash
docker compose up -d
docker compose ps          # 모두 Up 인지 본다
```

| 서비스 | 주소 |
| --- | --- |
| Kafka | `localhost:9092` (화면: Kafka UI <http://localhost:8090>) |
| Redis | `localhost:6379` |
| Qdrant | `localhost:6333` (<http://localhost:6333/dashboard>) |
| Prometheus | <http://localhost:9090> |
| Grafana | <http://localhost:3000> |

데이터는 `docker-data/` 에 쌓인다(git 에 올라가지 않는다).

## 3. 파이썬 환경과 키

```bash
cd d2-ai-python
python -m venv venv                  # 처음 한 번
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

`.env` 를 열어 `GOOGLE_API_KEY=` 뒤에 키를 넣는다. Claude 도 쓰면 `ANTHROPIC_API_KEY=` 도 넣는다. 나머지 항목은 기본값으로 충분하다(설명은 `.env.example` 안에 있다).

문서를 Qdrant 에 넣는다(처음 한 번, 임베딩에 키를 쓴다).

```bash
python -m app.services.qdrant_ingest
```

## 4. 워커 켜기 (터미널 A)

`d2-ai-python` 폴더에서 가상환경을 켠 상태로 실행한다. 폴더를 `app` 안으로 옮기지 않는다.

```bash
cd d2-ai-python
source venv/bin/activate
export KAFKA_WORKER_ID=w1
uvicorn app.main:app --port 8000
```

가짜 모델로 켜려면 `uvicorn` 앞에 아래 두 줄을 더한다.

```bash
export FAKE_LLM_ENABLED=true
export FAKE_LLM_DELAY_SEC=1
```

로그에 `파티션 배정`, `Kafka 접속 완료, 수신 대기` 가 보이면 된다. <http://localhost:8000/health> 도 열어 본다.

## 5. Spring 켜기 (터미널 B)

```bash
cd d3-was-spring
./gradlew bootRun
```

로그에 `Started WasApplication` 이 보이면 된다. 토픽 `agent.requests`(파티션 3개)는 Spring 이 처음 뜰 때 만든다.

## 6. 요청 한 번 보내기 (터미널 C)

```bash
curl -s -X POST http://localhost:8080/api/agent/requests \
  -H "Content-Type: application/json" \
  -d '{"question": "환불은 며칠 안에 신청해야 하나요?", "provider": "gemini"}'
```

`{"requestId":"…","status":"ACCEPTED"}` 가 오면 접수된 것이다. 가짜 모델이면 `"provider": "fake"` 로 보낸다. 결과는 워커에서 `requestId` 로 조회한다.

```bash
curl -s http://localhost:8000/api/v1/agent/results/<requestId>
```

`status` 가 `DONE` 이면 `answer` 가 들어 있다. `PROCESSING` 이면 몇 초 뒤 다시 조회한다. 같은 `sessionId` 를 붙여 다시 보내면 이전 대화를 이어간다.

## 7. 모니터링 보기 (Prometheus, Grafana)

2번에서 `docker compose up -d` 를 하면 Prometheus 와 Grafana 도 같이 켜진다. 따로 설치하거나 설정할 것은 없다. 설정은 저장소의 `monitoring/` 에 있고 컨테이너가 시작할 때 읽는다.

| 무엇 | 주소 | 볼 것 |
| --- | --- | --- |
| Prometheus | <http://localhost:9090/targets> | 대상(spring, fastapi, kafka, redis, qdrant)의 State 가 UP 인지 |
| Grafana | <http://localhost:3000/d/ai-serving> | 대시보드 "AI 에이전트 서빙 개요". 로그인 없이 보기만 된다. 편집은 `admin` / `admin` |

1. 컨테이너가 떠 있는지 본다: `docker compose ps` 에서 `prometheus1`, `grafana1`, `kafka-exporter`, `redis-exporter` 가 Up.
2. 4번의 워커와 5번의 Spring 을 켠다. Prometheus 는 5초마다 `localhost:8080`(Spring), `localhost:8000`, `8001`(워커)의 지표를 가져간다. 켜지 않은 서버는 `/targets` 에서 DOWN 으로 보이는 것이 정상이다(워커를 8000 하나만 켜면 8001 이 DOWN).
3. 요청을 여러 번 보낸다(6번의 요청을 반복하거나 `scripts/k6/` 의 시나리오). 그래프에 값이 쌓이기 시작한다.
4. Grafana 대시보드를 연다. 접수 요청 처리량, 응답 시간 p95/p99, 컨슈머 그룹 lag, 워커 처리 건수, Redis 명령 수 같은 패널이 있다. 오른쪽 위에서 시간 범위를 "Last 15 minutes" 정도로 두면 방금 보낸 요청이 보인다.

새로 받은 환경에서는 그래프가 비어 있다. 값은 컨테이너의 데이터 폴더(`docker-data/prometheus`, 최대 7일 보관)에 쌓이고, 이 폴더는 git 에 올라가지 않기 때문이다. 대시보드의 틀(패널, 데이터 소스)은 `monitoring/` 에서 읽으므로 데이터 없이도 같은 화면이 나온다. 요청을 보내야 값이 채워진다.

## 8. 전체 확인

```bash
cd d2-ai-python
source venv/bin/activate
python -m app.checks.system_verify --only offline    # Redis 만 필요. 7개 점검
python -m app.checks.system_verify --only live       # 위 1~5 가 켜져 있고 워커가 가짜 모델일 때. 11개 점검
```

`--only live` 는 워커를 `FAKE_LLM_ENABLED=true`, `FAKE_LLM_DELAY_SEC=1` 로 켰을 때 기준이다. 실제 모델까지 보는 3단계는 비용이 들어 따로 실행한다([DAY35_GUIDE.md](docs/DAY35_GUIDE.md)). Spring 테스트는 `cd d3-was-spring && ./gradlew test`.

## 9. 끄기

```bash
# 터미널 A, B 에서 Ctrl+C
docker compose stop        # 데이터는 남는다. 지우려면 docker compose down
```

## 막힐 때

| 증상 | 확인 |
| --- | --- |
| `No module named 'app'` | `d2-ai-python` 폴더에서 실행했는지 본다(`app` 안이 아니다) |
| 워커 `/health` 가 안 열림 | 터미널 A 로그, 포트 8000 을 다른 프로세스가 쓰는지(`lsof -i :8000`) |
| 요청은 202 인데 결과가 계속 `PROCESSING` | 워커 로그에 요청이 들어왔는지, `KAFKA_TOPIC` 이 Spring 의 토픽(`agent.requests`)과 같은지 |
| Spring 요청이 503 `REJECTED` | Kafka 컨테이너가 떠 있는지(`docker compose ps`) |
| 결과가 `FAILED` | 워커 로그의 오류. 키가 비어 있거나 잘못됐는지 본다 |
