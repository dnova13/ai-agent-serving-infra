# Day 30 코드 가이드 — 지표가 만들어져 Grafana 화면에 뜨기까지

> 실행 방법과 확인 항목은 [DAY30_GUIDE.md](DAY30_GUIDE.md), 결과는 [DAY30_RESULT.md](DAY30_RESULT.md). 이 문서는 **지표가 어디서 만들어지고 어떻게 Prometheus 를 거쳐 Grafana 패널이 되는지**를 파일 단위로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수와 설정 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000/8001 = FastAPI 워커**, 9092 = Kafka, 9308 = kafka-exporter, 6379 = Redis, 9121 = redis-exporter, 6333 = Qdrant, **9090 = Prometheus**, **3000 = Grafana**.

**목적:** 접수(Spring), 큐(Kafka), 처리(FastAPI 워커), 저장소(Redis, Qdrant)의 상태를 한 화면에서 보는 대시보드를 만든다. 서버 로직은 바꾸지 않고 지표를 내보내는 코드와 수집·표시 설정만 더했다.

**Day 30 의 코드 변경:** 커밋 4개로 나뉜다.

| 커밋      | 내용                                                         |
| --------- | ------------------------------------------------------------ |
| `bf70bd7` | FastAPI 워커 지표 (`metrics.py`, `main.py`, `kafka_worker.py`, `requirements.txt`) |
| `159f1fd` | Spring 지표 (`build.gradle`, `application.properties`)        |
| `b1b8d3c` | Prometheus, Grafana, exporter 컨테이너와 대시보드 (`docker-compose.yml`, `monitoring/**`) |
| `f8e4d67`, `db99555` | 가이드, 결과 문서, 캡처 이미지                       |


## 1. 전체 흐름

```
Spring(8080)  /actuator/prometheus ──┐
FastAPI(8000) /metrics ──────────────┤
FastAPI(8001) /metrics ──────────────┤                       5초마다 가져감 (scrape)
kafka-exporter(9308) ← Kafka 브로커 ─┼──▶ Prometheus(9090) ──────────────▶ Grafana(3000)
redis-exporter(9121) ← Redis ────────┤      (시계열 저장)        PromQL 로 질의, 패널로 그림
Qdrant(6333) /metrics ───────────────┘
```

| 단계 | 하는 일                                                 | 파일                                                                        |
| ---- | ------------------------------------------------------- | --------------------------------------------------------------------------- |
| A    | 워커 지표 정의                                          | `d2-ai-python/app/services/metrics.py`                                      |
| B    | 워커 코드에서 지표 값 올리기                            | `d2-ai-python/app/services/kafka_worker.py`                                 |
| C    | HTTP 지표 + `/metrics` 열기                             | `d2-ai-python/app/main.py`, `d2-ai-python/requirements.txt`              |
| D    | Spring 지표 열기                                        | `d3-was-spring/build.gradle`, `d3-was-spring/src/main/resources/application.properties` |
| E    | 컨테이너 4개 추가                                       | `docker-compose.yml`                                                        |
| F    | 어디서 가져올지                                         | `monitoring/prometheus.yml`                                                 |
| G    | Grafana 가 시작할 때 데이터소스와 대시보드를 자동 등록  | `monitoring/grafana/provisioning/**`, `monitoring/grafana/dashboards/ai-serving.json` |


## 2. 주요 코드

### 2-1. 워커 지표 정의와 기록 (A, B)

**목적:** Kafka 워커가 메시지를 처리한 결과와 시간을 센다. 기록만 하고 서버는 열지 않는다(노출은 2-2).

- 파일: `d2-ai-python/app/services/metrics.py`

| 지표                              | 종류      | 라벨     | 의미                                                                 |
| --------------------------------- | --------- | -------- | -------------------------------------------------------------------- |
| `agent_worker_messages_total`     | Counter   | `result` | 워커가 끝낸 메시지 수. `ok`(성공), `failed`(처리 실패로 건너뜀), `skipped`(해석 불가로 건너뜀) |
| `agent_worker_handle_seconds`     | Histogram | 없음     | 메시지 한 건 처리 시간. 구간(bucket)은 0.05초~60초로 넓게 잡아 가짜 모델 5초부터 실제 LLM 수십 초까지 보이게 했다 |
| `agent_worker_commits_total`      | Counter   | 없음     | commit 성공 횟수                                                      |
| `agent_worker_up`                 | Gauge     | 없음     | Kafka 에 접속해 수신 대기 중이면 1, 아니면 0                           |

(Day 31 의 `agent_tool_*`, `agent_give_ups_total` 과 Day 32 의 `agent_worker_dlq_total` 은 같은 파일에 나중에 추가됐다.)

기록하는 곳은 `kafka_worker.py` 의 네 군데다.

| 위치                                   | 기록                                                                         |
| -------------------------------------- | ---------------------------------------------------------------------------- |
| `_run` 에서 접속 완료 직후             | `WORKER_UP.set(1)`                                                           |
| `_run` 의 `finally`                    | `WORKER_UP.set(0)` (접속이 끊기거나 종료될 때)                                |
| `_run` 의 commit 성공 뒤               | `COMMITS.inc()`                                                              |
| `_handle` 의 성공/실패/해석 불가 분기  | `MESSAGES.labels(result=...).inc()`, 성공·실패 때 `HANDLE_SECONDS.observe(걸린 시간)` |

**Counter 와 Histogram:** Counter 는 계속 늘기만 하는 누적값이라 화면에서는 `rate()` 로 "초당 몇 건"으로 바꿔 쓴다. Histogram 은 시간을 구간별 개수로 쌓아 두므로 `histogram_quantile()` 로 p50, p95 를 계산한다.

### 2-2. /metrics 열기 (C)

- 파일: `d2-ai-python/app/main.py`

```python
from prometheus_fastapi_instrumentator import Instrumentator
...
Instrumentator(excluded_handlers=["/metrics"]).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
```

| 부분                              | 내용                                                                                                  |
| --------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `instrument(app)`                 | 모든 HTTP 요청의 수와 응답 시간(`http_requests_total`, `http_request_duration_seconds`)을 자동으로 센다  |
| `expose(app, "/metrics")`         | `GET /metrics` 로 위 지표와 `metrics.py` 의 워커 지표를 함께 내보낸다                                   |
| `excluded_handlers=["/metrics"]`  | Prometheus 가 5초마다 부르는 `/metrics` 자신은 지표에서 뺀다                                            |
| `include_in_schema=False`         | Swagger(`/docs`) 목록에 나오지 않게 한다                                                               |

워커 2개(8000, 8001)는 프로세스가 달라 지표도 따로이고, Prometheus 가 `instance` 라벨(포트)로 구분한다.

### 2-3. Spring 지표 (D)

- 파일: `d3-was-spring/build.gradle`, `application.properties`

| 설정                                                                                      | 내용                                                                                       |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `spring-boot-starter-actuator`, `micrometer-registry-prometheus`                          | 지표 수집 틀과 Prometheus 형식 출력                                                          |
| `management.endpoints.web.exposure.include=health,prometheus`                             | 두 엔드포인트만 연다 (`GET /actuator/prometheus`)                                           |
| `management.metrics.distribution.percentiles-histogram.http.server.requests=true`         | 응답 시간을 구간별로 내보내 Grafana 에서 p95, p99 를 계산하게 한다                          |
| `server.tomcat.mbeanregistry.enabled=true`                                                | Tomcat 스레드 지표(`tomcat_threads_busy_threads` 등)를 켠다                                  |
| `management.metrics.tags.application=was`                                                 | 모든 지표에 `application="was"` 라벨을 붙인다                                                |

### 2-4. 수집 대상 (E, F)

- 파일: `docker-compose.yml`, `monitoring/prometheus.yml`

`docker-compose.yml` 에 컨테이너 4개가 더해졌다: `prometheus1`(9090), `grafana1`(3000), `kafka-exporter`(9308), `redis-exporter`(9121). Kafka 와 Redis 는 자체 `/metrics` 가 없어 exporter 가 대신 읽어서 내보낸다.

`prometheus.yml` 은 5초(`scrape_interval`)마다 아래 대상의 `/metrics` 를 가져간다.

| job        | 대상                                              | 경로                  |
| ---------- | ------------------------------------------------- | --------------------- |
| prometheus | `localhost:9090`                                  | `/metrics`            |
| spring     | `host.docker.internal:8080`                       | `/actuator/prometheus` |
| fastapi    | `host.docker.internal:8000`, `:8001`              | `/metrics`            |
| kafka      | `kafka-exporter:9308`                             | `/metrics`            |
| redis      | `redis-exporter:9121`                             | `/metrics`            |
| qdrant     | `qdrant:6333`                                     | `/metrics`            |

Spring 과 FastAPI 는 컨테이너가 아니라 호스트에서 돌므로 컨테이너 안에서 `host.docker.internal` 로 접근한다. 8001 은 두 번째 워커를 켰을 때만 UP 이다.

### 2-5. 대시보드 (G)

- 파일: `monitoring/grafana/dashboards/ai-serving.json` (25개 패널), `monitoring/grafana/provisioning/datasources/prometheus.yml`, `.../dashboards/dashboards.yml`

Grafana 가 시작할 때 `provisioning` 폴더를 읽어 Prometheus 데이터소스와 대시보드 JSON 을 자동으로 등록한다. 화면을 손으로 만들지 않아 컨테이너를 지우고 다시 띄워도 같은 대시보드가 나온다.

| 줄(row)                          | 패널과 핵심 질의(PromQL)                                                                                                                                                |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 대상 상태                        | 6개 stat 패널. `max(up{job="spring"})` 처럼 `up`(수집 성공 1, 실패 0)을 본다 (Spring, FastAPI 8000, 8001, Kafka exporter, Redis exporter, Qdrant)                          |
| Spring WAS — 접수 경로           | 접수 요청 처리량 `sum by (uri) (rate(http_server_requests_seconds_count{...}[30s]))`, 응답 시간 p95/p99 `histogram_quantile(0.95, ...)`, 5xx 에러, JVM 힙, CPU, Tomcat 스레드 |
| Kafka — 쌓임과 처리              | 컨슈머 그룹 lag `sum by (consumergroup) (kafka_consumergroup_lag{consumergroup="agent-workers",topic="agent.requests"})`, 유입 vs 처리 속도(토픽 offset 증가율 `kafka_topic_partition_current_offset`) |
| FastAPI 워커                     | 처리 건수 `sum by (instance, result) (rate(agent_worker_messages_total[30s]))`, 처리 시간 p50/p95 `histogram_quantile(..., agent_worker_handle_seconds_bucket)`, HTTP 요청 |
| Redis · Qdrant                   | Redis 명령 처리량, 메모리, 연결된 클라이언트                                                                                                                              |

`[30s]` 는 최근 30초 구간의 증가율을 구한다는 뜻이다. 5초마다 수집하므로 구간 안에 표본이 여섯 개쯤 들어가 값이 덜 출렁인다.


## 3. 시간 순서 (요청 1건이 지표가 되기까지)

```
t=0     k6 → Spring POST /api/agent/requests
        Spring: http_server_requests_seconds_count / _bucket 증가           (접수 처리량·응답 시간 패널)
        Spring → Kafka 에 메시지 → kafka_topic_partition_current_offset 증가   (유입 속도)
t=?     워커가 메시지를 읽음 → 처리 → agent_worker_messages_total{result="ok"} 증가, handle_seconds 에 기록
        commit → agent_worker_commits_total 증가 → kafka_consumergroup_lag 감소
t=5s마다 Prometheus 가 위 값들을 가져가 시계열로 저장
        Grafana 가 새로고침 주기마다 PromQL 로 질의해 패널을 그림
```


## 4. 실패 지점별 동작

| 상황                                  | 화면에서 보이는 모습                                                                                          |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Spring 이 꺼짐                        | `up{job="spring"}` 이 0 → 상태 패널이 DOWN. Spring 시계열은 끊긴다                                             |
| 워커가 Kafka 접속에 실패/종료         | `agent_worker_up` 이 0 으로 바뀌고 처리 건수가 0 이 된다. lag 는 늘어난다                                       |
| 워커를 하나 더 켬 (8001)              | `up{instance=~".*:8001"}` 이 1 이 되고 처리 건수 패널에 `instance` 별 줄이 하나 늘어난다                          |
| 대시보드 JSON 을 고침                 | Grafana 가 시작할 때 provisioning 폴더의 JSON 을 읽어 대시보드를 등록한다. 대시보드는 이 파일이 원본이다            |

상태 패널(stat)은 지금 값만 보여 주고 과거에 DOWN 이었던 기록을 남기지 않는다. 이 한계는 [DAY30_RESULT.md](DAY30_RESULT.md) 5절에 적었다.


## 5. 설계 선택과 이유

| 선택                                                       | 이유                                                                                                         |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 서버 코드는 놔두고 지표 코드만 추가                        | 지표가 동작을 바꾸면 측정 대상이 달라진다. Counter 증가와 Histogram 기록은 처리 흐름에 영향이 없다             |
| Kafka, Redis 는 exporter 사용                              | 두 서버는 Prometheus 형식으로 직접 내보내지 않는다. exporter 가 서버 상태를 읽어 변환해 준다                    |
| 5초 수집 + `rate()[30s]`                                   | 실험이 수 분 단위라 Prometheus 기본 수집 간격(1분)은 거칠다. 5초 수집에 30초 구간을 평균 내 부드럽게 만들었다                      |
| 워커 지표에 라벨은 `result` 하나만                         | 라벨 값이 많아지면 시계열이 늘어난다(카디널리티). 요청별 ID 같은 값은 라벨로 쓰지 않는다                          |
| Grafana provisioning                                       | 대시보드를 코드(JSON)로 관리해 Git 에 넣고 같은 화면을 재현한다                                                |


## 6. 코드를 따라 읽는 순서

1. `d2-ai-python/app/services/metrics.py` (지표 정의)
2. `d2-ai-python/app/services/kafka_worker.py` 에서 `metrics.` 를 검색 (기록하는 곳)
3. `d2-ai-python/app/main.py` 의 `Instrumentator`
4. `d3-was-spring/src/main/resources/application.properties` 의 `management.*`
5. `monitoring/prometheus.yml`
6. `monitoring/grafana/dashboards/ai-serving.json` 에서 패널 제목과 `expr`
7. [DAY30_RESULT.md](DAY30_RESULT.md) 에서 화면 캡처와 해석
