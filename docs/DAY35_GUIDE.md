# Day 35 가이드 — 전체 시스템 통합 재검증

> 그동안 만든 기능을 한꺼번에 다시 돌려서 서로 깨뜨린 곳이 없는지 보는 통합 재검증

> 상태: **1, 2단계는 사용자 PC 에서 실행해 PASS 18, FAIL 0, SKIP 0(2026-10-19). 3단계(실제 모델)는 스크립트를 작성하고 가짜 서버로 동작만 확인했고, 실제 Claude 로는 아직 실행 전**. 아래 기대값은 이전 Day 의 결과에서 가져온 예상이다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커**, 9092 = Kafka(호스트), 6379 = Redis, 6333 = Qdrant, 9090 = Prometheus, 3000 = Grafana.
> 앞선 결과: [DAY34_1_RESULT.md](DAY34_1_RESULT.md).
> 코드 흐름은 [DAY35_CODE_GUIDE.md](DAY35_CODE_GUIDE.md).

> **다시 재검증(재측정)시 4절 ① 전체 실행 한 줄(`python -m app.checks.system_verify`)만 하면 된다.** 현재 코드 수정한 단계에서는 실제 모델(3단계)까지 스크립트에 들어가 있어 1, 2, 3단계가 모두 실행된다(3단계는 Claude 를 실제로 부르므로 호출이 싫으면 `--only offline`, `--only live`). 준비(3절)는 평소 설정으로 되돌려 놓은 상태여야 한다.

**전체 목적:** Day 1~34 에 걸쳐 기능을 하나씩 만들고 그때마다 따로 확인했다. 오늘은 (1) 그동안 만든 점검을 한꺼번에 다시 돌려 이전 Day 의 수정이 다른 Day 의 기능을 깨뜨리지 않았는지 보고, (2) 평소 설정으로 켠 실제 구성 요소(Spring → Kafka → 워커 → Redis, Prometheus, Grafana)가 한 줄로 이어져 있는지 확인한다. 어긋나는 것이 나오면 고치고 다시 돌린다(버그 수정).

## 1. 오늘 목표

| 확인할 것                                                                                                                                      | 단계  |
| ---------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| Redis, 결과 저장, 만료 정책, 분산 락, Self-Correction, DLQ 점검이 지금 코드에서도 PASS                                                         | 1단계 |
| 락 스트레스(baseline, timeout, N=50)가 지금도 PASS                                                                                             | 1단계 |
| 워커가 평소 설정(토픽 `agent.requests`, 그룹 `agent-workers`)으로 떠 있고 Redis 가 연결됨                                                      | 2단계 |
| Spring 상태와 지표(`/actuator/prometheus`), 워커 지표(`/metrics`)가 열려 있음                                                                  | 2단계 |
| 가짜 모델 요청이 접수(202)부터 DONE 까지 이어지고 워커, 파티션, offset 이 결과에 남음                                                          | 2단계 |
| 같은 sessionId 로 두 번 보내면 대화가 이어지고(턴 2), 삭제하면 404                                                                             | 2단계 |
| 빈 question 은 400                                                                                                                             | 2단계 |
| Prometheus 대상(spring, kafka, redis)이 up, Grafana 응답                                                                                       | 2단계 |
| 컨슈머 그룹 `agent-workers` 의 lag 가 0                                                                                                        | 2단계 |
| 실제 모델(Claude) 질문이 접수(202)부터 DONE 까지 이어지고, 답이 비어 있지 않고 가짜 답(`[fake]`)이 아니며, 워커, 파티션, offset 이 결과에 남음 | 3단계 |
| Spring 동기 경로 `/api/agent/sync` 가 실제 모델로 200 과 답을 돌려줌                                                                           | 3단계 |

## 2. 바뀐 것

| 파일                                                   | 내용                                                                                                                                                                                                                       |
| ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `d2-ai-python/app/checks/system_verify.py` (새 파일) | 1단계는 기존 점검 스크립트를 하나씩 실행해 종료 코드로 판정, 2단계는 실제 서버에 가짜 모델 요청을 보내 확인, 3단계는 실제 Claude 로 비동기 질문 n건(기본 3)과 동기 1건을 보내 확인. 항목마다 PASS/FAIL/SKIP, 마지막에 요약 |
| 기존 코드                                              | 바꾸지 않음                                                                                                                                                                                                                |

판정 방식: 1단계는 각 점검 스크립트의 종료 코드(0 이면 PASS, 2 는 SKIP, 그 밖은 FAIL). 2단계는 응답 코드와 값을 직접 비교한다. 3단계는 2단계와 같은 방식으로 판정하되 질문이 모두 DONE 이고 답이 비어 있지 않고 `[fake]` 가 아니어야 PASS 다(하나라도 아니면 FAIL). FAIL 이 하나라도 있으면 마지막 줄이 `== 결과: FAIL` 이다. SKIP(켜 있지 않은 선택 요소)은 실패로 세지 않는다.

## 3. 준비 (Day 34.1 설정에서 평소 설정으로 되돌리기)

Day 34.1 에서 Spring 과 워커를 실험용 토픽으로 띄웠으므로 평소 설정으로 돌려놓는다.

1. 실험 워커를 끈다: `bash scripts/bench-workers.sh stop` (끝에 `없음` 이 나와야 한다).
2. Spring 을 끈다(터미널 A 에서 Ctrl+C). IntelliJ 실행 설정에 인자를 넣었다면 비운다.
3. (선택) 실험 토픽을 지운다. 위치: `docker exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic agent.bench.p1` (p3, p6 도 같게).
4. 컨테이너를 켠다: Kafka, Redis, Qdrant 와 모니터링(`docker compose up -d prometheus grafana kafka-exporter redis-exporter`). 이미 켜져 있으면 그대로 둔다.
5. `agent-workers` 그룹의 lag 가 0 인지 본다(0 이 아니면 워커를 모두 끄고 offset 을 최신으로 리셋).
    ```
    docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --describe
    ```
6. Spring 을 **인자 없이** 띄운다. `d3-was-spring` 에서 `./gradlew bootRun` (또는 IntelliJ 로 인자 없이). 로그에 기동 완료가 보이면 된다.
7. 워커 1대를 평소 설정으로 가짜 모델을 켜서 띄운다(파이썬 가상환경을 켠 터미널에서):
    ```
    cd d2-ai-python
    export KAFKA_WORKER_ID=w1
    export FAKE_LLM_ENABLED=true
    export FAKE_LLM_DELAY_SEC=1
    uvicorn app.main:app --port 8000
    ```
    로그에 `파티션 배정 [0, 1, 2]` 와 `Kafka 접속 완료, 수신 대기` 가 보이면 된다(토픽 `agent.requests`, 파티션 3개).
8. 다른 터미널에서 가상환경을 켜고 `cd d2-ai-python` 로 간다.

## 4. 실험

### ① 전체 실행

```
python -m app.checks.system_verify
```

1단계(기존 점검 7개)는 몇 분 걸린다(lock_stress 가 가장 길다). 이어서 2단계, 3단계가 실행된다. **3단계는 Claude 를 실제로 부르고(비동기 3건 + 동기 1건, 토큰 소량) 검색 도구가 Gemini 임베딩도 부른다.** `ANTHROPIC_API_KEY`, `GOOGLE_API_KEY` 가 워커 환경에 있어야 한다.

**기대(확인 전 예상):**

- 1단계: 7개 모두 PASS. `lock_check` 는 langchain 등이 없으면 일부 항목을 건너뛰지만 종료 코드는 0 이다.
- 2단계: 워커 `/health`, Spring 상태와 지표, 워커 `/metrics`, end-to-end, 같은 세션 대화, 세션 삭제 404, 입력 검증 400 이 PASS. Prometheus 대상과 Grafana 는 컨테이너가 켜져 있으면 PASS 이고 안 켜져 있으면 SKIP. lag 는 PASS(합계 0).
- 3단계: 실제 모델 end-to-end(비동기)와 동기 경로 PASS. 워커 1대가 질문 3건을 차례로 처리하므로 접수부터 완료까지는 마지막 건이 약 10초 안팎일 것으로 예상한다(Day 34.2 에서 건당 평균 3.4초, 계산값).
- 마지막 줄: `== 결과: PASS`.

**실패했을 때:**

- 워커 `/health` 에서 `topic=...` 이 `agent.requests` 가 아니면 Day 34.1 실험 설정이 남아 있는 것이다(3절 1~2, 7번을 다시 한다).
- end-to-end 에서 접수는 되는데 DONE 이 안 되면 워커 로그와 `FAKE_LLM_ENABLED=true` 를 확인한다.
- 3단계에서 비동기가 FAIL 이고 `status=FAILED` 와 사유가 나오면 워커 로그에서 Claude(Anthropic) 쪽 오류인지 검색 도구의 Gemini 임베딩 쪽 오류인지 본다. `가짜 모델 답이 왔다` 는 provider 가 claude 로 전달되지 않았다는 뜻이다. 동기가 FAIL 이면 본문의 `http=502/504` 를 본다(Spring 30초 제한을 넘기면 504).
- Prometheus 대상에서 `fastapi@host.docker.internal:8001` 이 down 인 것은 정상이다(8001 워커를 안 켠 것). 판정은 spring, kafka, redis 만 한다.
- 1단계의 FAIL 은 해당 스크립트를 따로 실행해 어느 항목인지 본다(예: `python -m app.checks.ttl_check`).

### ② 단계만 실행 (필요할 때)

```
python -m app.checks.system_verify --only offline     # 1단계만
python -m app.checks.system_verify --only live        # 2단계만
python -m app.checks.system_verify --only real        # 3단계만 (Claude 를 실제로 부른다)
python -m app.checks.system_verify --only real --real-n 5   # 3단계, 비동기 질문 5건
python -m app.checks.system_verify --only live --workers localhost:8000,localhost:8001   # 워커 2대
```

### ③ 버그가 나왔을 때

1. FAIL 항목, 증상, 원인, 고친 곳을 `DAY35_RESULT.md` 의 "실행하며 발견해 고친 것" 표에 적는다.
2. 고친 뒤 같은 항목을 다시 실행해 PASS 를 확인한다.
3. 고친 것이 다른 Day 의 기능에 영향을 주지 않는지 ① 을 한 번 더 돌린다.

## 5. 이미 알고 있는 확인 대상 (이전 Day 에서 남은 것)

| 출처      | 내용                                                                                                                 | 오늘 할 일                                                                                               |
| --------- | -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Day 33    | Redis 연결 풀이 모자라면(N=200, 풀 100) 락이 우회되고 대화 읽기, 저장이 실패하는데 응답은 성공한다(`fail_open=True`) | 코드는 바꾸지 않았다. 고칠지(풀 크기를 환경변수로 받기, 풀 부족일 때 락을 우회하지 않기) 사용자가 정한다 |
| Day 30~32 | `agent_tool_*`, `agent_give_ups_total`, `agent_worker_dlq_total` 지표에 Grafana 패널이 없다                          | 패널을 추가할지는 사용자가 정한다                                                                        |
| Day 30    | `DAY30_GUIDE.md` 6절 "확인하지 않은 것" 삭제 여부                                                                    | 사용자가 정한다                                                                                          |

## 6. 체크리스트

- [ ] 준비: 평소 설정으로 되돌림(워커, Spring, lag 0)
- [ ] ① 1단계: 기존 점검 7개 PASS
- [ ] ① 2단계: 워커, Spring, 지표, end-to-end, 세션, 입력 검증 PASS
- [ ] Prometheus 대상(spring, kafka, redis), Grafana, lag
- [ ] ① 3단계: 실제 모델 비동기, 동기 PASS
- [ ] 버그가 나오면 고치고 다시 PASS
- [ ] DAY35_RESULT.md 작성
