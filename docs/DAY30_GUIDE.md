# Day 30 가이드 — Prometheus + Grafana 모니터링 대시보드

> 상태: **①②③ 실행 완료**(결과는 [DAY30_RESULT.md](DAY30_RESULT.md)). 아래 기대값은 모두 "확인 전 예상"이다.
> 포트: **8080 = Spring WAS**, **8000, 8001 = FastAPI 워커**, **9090 = Prometheus**, **3000 = Grafana**, 9308 = kafka-exporter, 9121 = redis-exporter, 9092 = Kafka, 6379 = Redis(redis1 컨테이너), 6333 = Qdrant.
> 앞선 결과: [DAY27_RESULT.md](DAY27_RESULT.md)(워커 2개, 가짜 모델 처리), [DAY28_RESULT.md](DAY28_RESULT.md)(접수 경로 한계 탐색).

**전체 목적:** 지금까지는 k6 요약과 터미널 로그, `docker stats` 같은 "끝난 뒤의 숫자"로만 시스템을 봤다. 오늘은 Spring, Kafka, FastAPI 워커, Redis, Qdrant 의 지표를 Prometheus 가 5초마다 모으고 Grafana 대시보드로 시간 흐름에 따라 보게 만든다. 이후 Day 31~36 의 실험(재시도, DLQ, 락 스트레스, 파티션 수, 서킷 브레이커)에서 부하를 걸 때 요청 처리량, 응답 시간, Kafka 에 쌓인 요청(lag), 워커 처리 속도를 한 화면에서 본다.


## 1. 오늘 목표

| 확인할 것                                                                                                              | 실험 |
| ---------------------------------------------------------------------------------------------------------------------- | ---- |
| Spring, FastAPI, Kafka, Redis, Qdrant 의 지표가 Prometheus 에 모이고(`/targets` 가 UP), 대시보드에 값이 나오는지           | ①    |
| 부하를 거는 동안 접수 처리량, 접수 응답 시간, Kafka lag, 워커 처리 속도가 대시보드에서 예상한 모양으로 움직이는지          | ②    |
| 부하 중 워커 하나를 끄면 대시보드에 어떻게 나타나는지(워커 상태, 처리 속도, lag 기울기)                                   | ③    |


## 2. 개념

| 항목                | 내용                                                                                                                                                                         |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 지표(metric)        | 서버가 숫자로 노출하는 상태. 텍스트로 `이름{라벨} 값` 한 줄씩 `/metrics` 같은 주소에 나온다                                                                                    |
| Counter, Gauge      | Counter 는 계속 늘기만 하는 누적값(요청 수, 처리 건수). 초당 값은 `rate()` 로 계산한다. Gauge 는 오르내리는 현재값(메모리, 스레드 수, lag)                                        |
| Histogram           | 값(응답 시간 등)을 구간(버킷)별 개수로 센 지표. `histogram_quantile(0.95, ...)` 로 p95 를 계산한다. 버킷 경계 사이의 값은 보간이라 정확한 값이 아니다                           |
| Prometheus          | 정해진 주기(5초)로 대상의 지표 주소를 가져가(scrape) 시계열로 저장하는 서버. 대상별로 `up`(가져오기 성공 1, 실패 0) 지표가 생긴다                                              |
| exporter            | 자체 지표 주소가 없는 시스템(Kafka, Redis)의 상태를 대신 읽어 지표로 바꿔 주는 작은 서버. kafka-exporter(9308), redis-exporter(9121)                                          |
| Grafana             | Prometheus 에서 PromQL 로 값을 질의해 그래프로 그리는 화면. 이번에는 설정 파일(provisioning)로 데이터소스와 대시보드를 자동 등록한다                                           |
| Kafka lag           | 토픽 끝 offset − 컨슈머 그룹이 commit 한 offset. 워커가 처리 못 하고 쌓인 요청 수. Day 27 에서 `kafka-consumer-groups --describe` 로 보던 LAG 와 같은 값이다                    |
| 접수 응답 시간 p95  | Spring 의 `http_server_requests_seconds` 히스토그램에서 `uri="/api/agent/requests"` 만 뽑아 계산한다. k6 가 재는 값(클라이언트 관점)과 조금 다를 수 있다(서버 안에서만 잰 시간) |


## 3. 바뀐 것

| 파일                                                       | 내용                                                                                                                                    |
| ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `d3-was-spring/build.gradle`                               | `spring-boot-starter-actuator`, `micrometer-registry-prometheus` 추가                                                                    |
| `d3-was-spring/src/main/resources/application.properties`  | `/actuator/prometheus` 노출, 응답 시간 히스토그램 버킷, Tomcat 스레드 지표, 공통 태그 `application=was`                                  |
| `d2-ai-python/requirements.txt`                        | `prometheus_client==0.26.0`, `prometheus-fastapi-instrumentator==8.1.0` 추가                                                            |
| `d2-ai-python/app/main.py`                                 | `GET /metrics` 열기(HTTP 요청 수와 응답 시간)                                                                                            |
| `d2-ai-python/app/services/metrics.py`                     | 새 파일. 워커 지표 정의: `agent_worker_messages_total{result}`, `agent_worker_handle_seconds`, `agent_worker_commits_total`, `agent_worker_up` |
| `d2-ai-python/app/services/kafka_worker.py`                | 메시지를 처리할 때와 commit 할 때 위 지표를 기록 (기존 동작은 그대로)                                                                    |
| `docker-compose.yml`                                       | `prometheus`(9090), `grafana`(3000), `kafka-exporter`(9308), `redis-exporter`(9121) 추가. 데이터는 `docker-data/prometheus`, `docker-data/grafana` |
| `monitoring/prometheus.yml`                                | 5초마다 가져올 대상 목록(spring, fastapi 8000/8001, kafka, redis, qdrant)                                                                |
| `monitoring/grafana/`                                      | 데이터소스 자동 등록, 대시보드 폴더 등록, 대시보드 `AI 에이전트 서빙 개요`(25개 패널)                                                    |


## 4. 준비

1. 파이썬 가상환경(venv)을 켠 터미널에서 새 의존성을 설치한다.

```bash
pip install -r d2-ai-python/requirements.txt
```

2. Spring 을 다시 빌드해서 띄운다(`build.gradle` 이 바뀌었다). 평소 실행하던 방법 그대로 하면 된다. 의존성 이름이 Spring Boot 4.1.1 에서 맞는지는 아직 확인하지 못했다(6번 참고).

3. Day 28 에서 쌓인 Kafka 요청(약 640만 건)을 비운다. 안 비우면 워커를 켜는 순간 쌓인 요청부터 처리하려 해서 lag 그래프가 의미가 없다. 워커를 모두 끈 상태(그룹이 비어 있음)에서 실행한다.

```bash
# agent-workers 그룹이 읽을 위치를 토픽 끝으로 옮긴다 (워커가 모두 꺼져 있어야 실행된다)
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --topic agent.requests --reset-offsets --to-latest --execute
```

```bash
# 그룹 LAG 가 0 인지 확인한다
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group agent-workers
```

4. 모니터링 컨테이너를 띄운다(프로젝트 루트에서).

```bash
docker compose up -d prometheus grafana kafka-exporter redis-exporter
```


## 5. 실행과 확인

### ① 지표가 모이는지 확인

**목적:** 각 시스템의 지표 주소가 열려 있고, Prometheus 가 모두 가져오며, Grafana 대시보드에 값이 나오는지 확인한다. 부하는 걸지 않는다. 이 실험은 워커를 켜지 않고(8000, 8001 모두 꺼진 상태) Spring 만 켠 채 시작한다.

```bash
# Spring 지표 주소: http_server_requests, jvm_, tomcat_ 지표가 줄줄이 나온다
curl -s localhost:8080/actuator/prometheus | head -20
```

워커 한 개를 켠 뒤(8000) FastAPI 지표 주소를 본다.

```bash
cd d2-ai-python && FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1 uvicorn app.main:app --port 8000
```

```bash
# FastAPI 지표 주소: http_requests_total, agent_worker_ 지표
curl -s localhost:8000/metrics | grep -E '^(agent_worker|http_requests_total)'
```

브라우저에서 두 곳을 연다.

| 주소                          | 볼 것                                                     |
| ----------------------------- | --------------------------------------------------------- |
| http://localhost:9090/targets | 대상별 State (UP 인지)                                    |
| http://localhost:3000         | 대시보드 `AI 에이전트 서빙 개요` (왼쪽 메뉴 Dashboards)   |

확인 전 예상:

| 항목                                | 예상                                                                                           |
| ----------------------------------- | ---------------------------------------------------------------------------------------------- |
| `/targets` 의 상태                  | spring, fastapi(8000), kafka, redis, qdrant 가 UP. fastapi(8001)는 두 번째 워커를 켜지 않았으니 DOWN |
| 대시보드 맨 위 "대상 상태" 6칸      | 8001 칸만 DOWN(빨강)이고 나머지는 UP(초록)                                                     |
| 부하가 없을 때 처리량, lag 패널     | 0 또는 값 없음. lag 는 0                                                                       |
| `agent_worker_up` (워커 8000)       | 1                                                                                              |


### ② 부하를 거는 동안 대시보드 관찰

**목적:** 접수 처리량, 접수 응답 시간, Kafka 에 쌓인 요청(lag), 워커 처리 속도가 부하를 걸었을 때와 끝난 뒤에 어떻게 움직이는지 본다. 워커는 두 개(8000, 8001)를 켜고, 가짜 모델 지연을 1초로 줄여 lag 가 쌓였다가 줄어드는 모양을 몇 분 안에 본다.

워커 두 개를 각각 다른 터미널에서 켠다.

```bash
cd d2-ai-python && FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1 uvicorn app.main:app --port 8000
```

```bash
cd d2-ai-python && FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1 uvicorn app.main:app --port 8001
```

Grafana 대시보드를 연 채로(시간 범위 Last 15 minutes, 5초마다 새로고침) 부하를 건다. 프로젝트 루트에서 실행한다.

```bash
# VU 5 까지 10초에 올리고 60초 유지, 요청 사이 1초 쉼 (접수 초당 약 5건)
k6 run -e MAX_VUS=5 -e RAMP_UP=10s -e HOLD=60s -e RAMP_DOWN=5s scripts/k6/ramping-async.js
```

k6 가 끝난 뒤에도 2~3분 더 대시보드를 본다(lag 가 줄어드는 모양을 보기 위해).

확인 전 예상:

| 패널                               | 예상                                                                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| 접수 요청 처리량                   | 올라가는 10초 동안 0 → 약 5 req/s, 유지 60초 동안 약 5 req/s, 끝나면 0                                                                  |
| 접수 응답 시간 p95 / p99           | 수십 ms 이하(Day 27 에서 k6 로 잰 접수 p(95)는 약 22~26ms)                                                                             |
| 에러 응답(5xx)                     | 0                                                                                                                                      |
| 유입 vs 처리 속도                  | 유입은 약 5 msg/s. 처리는 워커 둘이 가짜 모델 1초씩 걸려 약 2 msg/s 이하(워커 하나가 한 번에 한 건씩 처리하기 때문)                       |
| 컨슈머 그룹 lag                    | 부하 동안 늘어 최대 약 150~200건(약 (5 − 2) × 60초)에 이르고, 부하가 끝난 뒤 약 1~2분에 걸쳐 0 으로 줄어든다                              |
| 워커 처리 건수                     | 8000, 8001 각각 result=ok 가 약 1 msg/s. failed 와 skipped 는 0                                                                         |
| 워커 메시지 처리 시간 p50 / p95    | 약 1초(가짜 모델 지연)와 근처. 히스토그램 버킷 경계(0.5, 1, 2.5)에 걸려 보간된 값이다                                                    |
| Kafka lag 가 0 에 도달한 뒤        | 처리 속도는 0 으로 돌아가고 워커 상태는 UP 으로 남는다                                                                                  |


### ③ 부하 중 워커 하나 끄기

**목적:** 워커가 하나 사라졌을 때 대시보드에서 무엇이 어떻게 보이는지 확인한다. 장애를 대시보드로 알아차릴 수 있는지를 본다.

② 와 같은 부하를 다시 건다. 워커 둘(8000, 8001)이 켜져 있어야 한다.

```bash
k6 run -e MAX_VUS=5 -e RAMP_UP=10s -e HOLD=60s -e RAMP_DOWN=5s scripts/k6/ramping-async.js
```

k6 가 유지 구간에 들어가고 약 20초 뒤, 8001 워커 터미널에서 Ctrl+C 로 끈다. k6 가 끝난 뒤에도 2~3분 더 대시보드를 본다. 끝나면 8001 워커를 다시 켜서(② 의 8001 명령) 남은 lag 를 처리하게 해도 된다.

확인 전 예상:

| 패널                          | 예상                                                                                                                                            |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| 대상 상태 "FastAPI 8001" 칸   | 끈 뒤 5~10초 안에 DOWN(빨강)                                                                                                                    |
| 워커 처리 건수                | 8001 의 선이 끊기고, 8000 이 8001 의 파티션까지 맡아 처리 속도는 약 1 msg/s 로 줄어듦. 리밸런싱 때문에 몇 초간 처리가 멈출 수 있다              |
| 유입 vs 처리 속도             | 처리 속도가 약 2 → 약 1 msg/s 로 내려감                                                                                                         |
| 컨슈머 그룹 lag               | 끈 뒤 증가 기울기가 가팔라지고(약 3 → 약 4 건/초), 부하가 끝난 뒤 0 으로 줄어드는 데 ② 보다 오래 걸린다(처리 속도가 절반이므로 약 2배)           |


### 정리

부하가 끝나고 lag 가 0 이 되면 워커를 끈다. 모니터링 컨테이너는 Day 31 이후 실험에서도 쓰므로 그대로 둔다. 내리려면 아래를 쓴다.

```bash
# 모니터링 컨테이너만 내린다 (데이터는 docker-data 에 남는다)
docker compose stop prometheus grafana kafka-exporter redis-exporter
```


## 6. 확인하지 않은 것

| 항목                                                              | 이유                                                                                              |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Spring Boot 4.1.1 에서 `/actuator/prometheus` 가 열리는지          | Spring 은 샌드박스에서 빌드할 수 없다. 의존성 이름과 설정 키가 맞는지 실행해 봐야 안다                  |
| kafka-exporter, redis-exporter 의 지표 이름                       | `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `redis_commands_processed_total` 등 문서 기준 이름이다. 실제 출력과 대조하지 않았다 |
| Qdrant `/metrics` 를 Prometheus 가 가져오는지                     | 컨테이너에서 접근해 보지 않았다                                                                    |
| 대시보드 JSON 이 Grafana 에서 그대로 열리는지                      | JSON 문법과 PromQL 은 확인했지만 Grafana 에 불러오지는 않았다                                       |
| 컨테이너 안에서 `host.docker.internal` 로 Spring, FastAPI 에 접근  | Docker Desktop 기준 동작을 가정했다. 실제 연결은 `/targets` 로 확인한다                              |


## 7. 알려진 한계

- 샌드박스에서는 Prometheus 실행 파일(3.5.0)로 설정 파일 문법과, FastAPI 지표(미니 앱)를 가져와 PromQL 이 동작하는지만 확인했다. Docker, Spring, Kafka 를 쓰는 구성은 확인하지 못했다.
- 5초 간격으로 가져오기 때문에 몇 초 안에 끝나는 짧은 변화는 그래프에 보이지 않는다.
- 응답 시간 p95 는 히스토그램 버킷 사이를 보간한 값이라 k6 의 p(95)와 정확히 같지 않다. FastAPI 의 `http_request_duration_seconds` 는 기본 버킷이 거칠다(0.1, 0.5, 1초).
- 로컬 실험용 설정이다. Grafana 는 로그인 없이 보기만 가능하게 열려 있고(편집은 admin / admin), 컨테이너는 root 로 실행한다.
- 이 대시보드는 한 PC 에서 모든 것이 도는 환경 기준이다. 부하가 큰 실험에서는 모니터링 자체가 자원을 일부 쓴다.


## 8. 체크리스트

- [x] 새 의존성 설치(`pip install -r`), Spring 재빌드
- [x] Kafka 쌓인 요청 비우기(그룹 LAG 0 확인)
- [x] 모니터링 컨테이너 기동
- [x] ① 지표 주소와 `/targets` 확인
- [x] ② 부하 중 대시보드 관찰
- [x] ③ 부하 중 워커 하나 끄기
- [x] DAY30_RESULT.md 작성
