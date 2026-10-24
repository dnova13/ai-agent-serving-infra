# Day 32 가이드 — Kafka DLQ (처리하지 못한 요청 보관과 재처리)

> 상태: **①②③④ 실행 완료([DAY32_RESULT.md](DAY32_RESULT.md))**.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **9092 = Kafka**(컨테이너 안에서는 `kafka:29092`), 6379 = Redis(`redis1`), 6333 = Qdrant. ① 은 이 중 아무것도 필요 없다.
> 앞선 결과: [DAY31_RESULT.md](DAY31_RESULT.md)(Self-Correction).

**전체 목적:** 지금 워커는 에이전트 처리가 실패하거나 메시지를 해석할 수 없으면 로그만 남기고 건너뛰어, 그 요청의 내용이 어디에도 남지 않는다(Day 16). 오늘은 건너뛴 메시지를 `agent.requests.dlq` 토픽에 원본과 실패 정보를 붙여 보관하고, 원인을 고친 뒤 다시 처리할 수 있는 도구를 만든다. 실패한 요청이 DLQ 에 남는지, 한 건 때문에 뒤의 요청이 막히지 않는지, 보관한 요청을 다시 보내 처리되는지 확인한다.


## 1. 오늘 목표

| 확인할 것                                                                                              | 실험 |
| ------------------------------------------------------------------------------------------------------ | ---- |
| Kafka 없이 가짜 producer 로 워커의 DLQ 경로(성공, 처리 실패, 해석 불가, 전송 실패 등)가 기대대로인지     | ①    |
| 처리 실패한 요청이 DLQ 토픽에 보관되고, 뒤의 정상 요청은 막히지 않으며, 지표와 `/health` 에 잡히는지     | ②    |
| 해석할 수 없는 메시지(JSON 이 아님)가 DLQ 에 `parse_error` 로 보관되는지                                | ③    |
| DLQ 에 보관한 요청을 `dlq_tool` 로 다시 보내(provider 를 고쳐서) 정상 처리되는지                        | ④    |


## 2. 개념

| 항목                | 내용                                                                                                                                                                  |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DLQ(Dead Letter Queue) | 처리하지 못한 메시지를 따로 모아 두는 토픽. 여기서는 `agent.requests.dlq`(파티션 1개). 버리지 않고 보관해 원인을 보고 다시 처리할 수 있게 한다                           |
| 처리 실패(`handler_error`) | 메시지는 정상인데 에이전트 처리 중 예외가 나 끝내지 못한 경우. 예: 지원하지 않는 provider, 모델 API 오류, 타임아웃. Day 31 의 재시도와 정정을 모두 쓰고도 예외로 끝난 경우만 해당한다 |
| 해석 불가(`parse_error`)   | 메시지가 JSON 이 아니거나 필수 필드가 없어 `AgentRequestMessage` 로 읽지 못한 경우. 다시 보내도 같은 결과라 재처리 대상이 아니다                                       |
| 보관 정보(봉투)     | DLQ 메시지는 JSON 이고 `reason`, `error`, `requestId`(해석 불가면 null), `worker`, `failedAt`, `source`(토픽, 파티션, offset), `originalValue`(원래 메시지 문자열)를 담는다 |
| 보관 뒤 commit      | 순서는 "처리 실패 → DLQ 에 보관(브로커 응답을 기다림) → 원래 메시지 commit". 보관하기 전에는 commit 하지 않는다                                                          |
| 보관 실패 시        | DLQ 전송이 실패하면 예외가 올라가 워커가 재접속하고, commit 하지 않은 그 메시지를 다시 받아 처리한다. 메시지를 잃는 것보다 다시 처리하는 쪽을 택했다(같은 요청이 두 번 처리될 수 있다) |
| 포기와의 차이       | Day 31 의 포기 답("죄송합니다, N번 시도했지만 ...")은 처리가 정상 끝난 것이라 DLQ 로 가지 않는다. DLQ 는 처리가 예외로 끝난 요청만 받는다                               |
| 재처리(replay)      | DLQ 에서 `handler_error` 요청의 원본을 꺼내 원래 토픽 `agent.requests` 로 같은 `requestId` 로 다시 보낸다. 결과는 같은 키에 덮어쓴다                                      |
| 환경변수            | `KAFKA_DLQ_TOPIC`(기본 `agent.requests.dlq`), `KAFKA_DLQ_ENABLED`(기본 true. false 면 예전처럼 건너뛰기만 한다)                                                        |


## 3. 바뀐 것

| 파일                                              | 내용                                                                                                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `d2-ai-python/app/services/kafka_worker.py`       | DLQ producer 를 consumer 와 같은 접속 수명으로 열고 닫음, `_send_to_dlq`(보관), 처리 실패와 해석 불가 두 곳에서 호출, `snapshot()`(`/health`)에 `dlq_topic`, `dlq_sent` 추가 |
| `d2-ai-python/app/services/metrics.py`            | `agent_worker_dlq_total{reason}` 추가                                                                                                              |
| `d2-ai-python/app/checks/dlq_tool.py`           | 새 파일. `list`(DLQ 내용 표 출력), `replay`(원래 토픽으로 다시 보내기. `--request-id`, `--all`, `--provider`, `--dry-run`)                         |
| `d2-ai-python/app/checks/dlq_check.py`          | 새 파일. ① 의 확인 스크립트                                                                                                                         |
| `docs/DAY32_GUIDE.md`                             | 이 문서                                                                                                                                            |

기존 동작은 그대로다. 처리 실패와 해석 불가 메시지를 건너뛰고 commit 하는 것은 같고, 그 사이에 DLQ 보관이 한 단계 들어간다. 새 패키지는 없어서 `pip install` 은 필요 없다.


## 4. 준비

① 은 파이썬 가상환경(venv)을 켠 터미널이면 된다. ②③④ 는 Kafka, Redis, Qdrant, Spring 을 띄우고 아래를 한다.

1. DLQ 토픽을 만든다(파티션 1개). 이미 있으면 그대로 둔다.

```bash
# 자동 생성에 맡기지 않고 파티션 1개로 명시해 만든다
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic agent.requests.dlq --partitions 1 --replication-factor 1
```

```bash
# 토픽이 만들어졌는지 확인한다
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic agent.requests.dlq
```

2. `agent-workers` 그룹에 쌓인 요청이 없는지 본다(워커는 꺼 둔 상태). LAG 가 0 이 아니면 Day 31 가이드 4번의 offset 리셋을 한다.

```bash
# 그룹 LAG 가 0 인지 확인한다
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group agent-workers
```

**워커가 켜져 있는지는 같은 명령의 출력으로 구분한다.** 그룹 조회에는 지금 파티션을 맡아 읽는 컨슈머 정보가 나온다. 컨슈머가 있으면 `CONSUMER-ID`, `HOST`, `CLIENT-ID` 칸에 값이 찍히고, 없으면 그룹 아래에 `has no active members` 가 나오고 세 칸이 `-` 로 나온다.

| 구분           | 그룹 조회 출력                                                                                                   | 판단            |
| -------------- | ---------------------------------------------------------------------------------------------------------------- | --------------- |
| 켜져 있을 때   | 파티션 0, 1, 2 줄 모두 `CONSUMER-ID` 가 `aiokafka-0.12.0-c7bd8fda-...`, `HOST` 가 `/192.168.65.1`, `CLIENT-ID` 가 `aiokafka-0.12.0` | 워커가 3개를 모두 맡고 있음 |
| 꺼져 있을 때   | `Consumer group 'agent-workers' has no active members.` 문구가 나오고 세 칸이 모두 `-`                            | 맡은 워커가 없음 |

![켜져 있을 때: 파티션마다 CONSUMER-ID, HOST, CLIENT-ID 가 채워져 있다](images/day32/prep-group-with-worker.png)

위 화면에서는 세 파티션이 같은 `CONSUMER-ID` 라서 워커 1개가 3개를 모두 맡고 있었다. 그래서 워커를 끄고 같은 명령을 다시 실행했다.

![꺼진 뒤: no active members, 세 칸이 모두 -](images/day32/prep-group-no-worker.png)

두 화면 모두 파티션 0, 1, 2 의 `LAG` 는 0 이고, `CURRENT-OFFSET`(2128875, 2125211, 2126236)이 같아서 그사이 새로 쌓인 요청은 없었다. 이것으로 준비 조건(워커 꺼짐, LAG 0)을 충족했다.

이 방법으로 알 수 있는 것은 "Kafka 가 보기에 그룹에 컨슈머가 있는가"다. 워커 프로세스를 직접 본 것은 아니다. 워커가 떠 있어도 아직 Kafka 에 접속하지 못했거나 접속이 끊긴 상태면 `no active members` 로 나올 수 있으므로, 실험 전에는 이 명령으로 그룹 상태를 보는 것을 기준으로 한다. 참고로 `CLIENT-ID` 에서 사용자 환경의 aiokafka 가 0.12.0 임을 알 수 있다.


## 5. 실행과 확인

### ① 가짜 producer 로 DLQ 경로 확인

**목적:** Kafka 없이 워커의 메시지 처리(`_handle`)만 돌려, 실패한 메시지가 DLQ 로 보내지고 그 내용이 맞는지, 전송이 실패하면 예외가 올라가는지 확인한다. 1초 안에 끝난다.

```bash
# d2-ai-python 폴더에서 실행한다
cd d2-ai-python
```

```bash
# 항목 A~K 를 돌려 PASS/FAIL 을 출력한다
python -m app.checks.dlq_check
```

기대(샌드박스에서 확인한 값. 사용자 PC 에서도 같은 결과가 나왔다):

| 항목 | 기대                                                                                                                      |
| ---- | ------------------------------------------------------------------------------------------------------------------------- |
| A    | 성공한 메시지는 DLQ 로 가지 않는다                                                                                         |
| B    | 처리 실패 → DLQ 토픽(`agent.requests.dlq`)에 1건. 키는 원래 키, `reason`=`handler_error`, `requestId`, 오류 내용, 원래 위치(파티션 1, offset 42), 원본 문자열이 그대로 담김 |
| C    | JSON 이 깨진 메시지 → `reason`=`parse_error`, `requestId`=null, 원본 `not-json`                                           |
| D    | UTF-8 이 아닌 바이트도 보관됨(대체 문자로 바뀜)                                                                           |
| E    | DLQ 전송이 실패하면 예외가 올라가고 보관 건수와 지표는 늘지 않는다                                                         |
| F    | DLQ 를 끄면 보내지 않고 건너뛰기만 한다                                                                                    |
| G    | 서버 종료(`CancelledError`)는 DLQ 로 보내지 않고 그대로 올라간다                                                           |
| H    | DLQ 가 켜져 있는데 producer 가 없으면 예외가 올라간다                                                                      |
| I    | `snapshot()` 에 `dlq_topic`, `dlq_sent`                                                                                    |
| J    | `dlq_tool` 의 해석, 재처리 대상 고르기(`--request-id`, `--all`, parse_error 는 제외), 재처리 메시지 만들기(`--provider` 만 바뀜) |
| K    | 지표 `agent_worker_dlq_total` 이 handler_error 1, parse_error 2                                                            |
| 마지막 줄 | "모든 항목 PASS"                                                                                                      |


### ② 처리 실패한 요청이 DLQ 에 보관되는지

**목적:** 에이전트 처리가 예외로 끝난 요청이 DLQ 토픽에 보관되고, 같은 파티션의 뒤에 오는 정상 요청은 막히지 않으며, `/health` 와 지표에 잡히는지 확인한다. 처리 실패는 "지원하지 않는 provider" 로 만든다(`no-such-provider` 는 모델을 고르는 단계에서 예외가 난다).

```bash
# 워커 1개를 띄운다 (fake-agent 로 정상 요청도 보내려고 가짜 모델을 켠다)
cd d2-ai-python && FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC=1 uvicorn app.main:app --port 8000
```

다른 터미널에서 요청 두 건을 순서대로 보낸다. 각 응답의 `requestId` 를 적어 둔다.

```bash
# 요청 1: 실패할 요청 (지원하지 않는 provider)
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"no-such-provider"}'
```

```bash
# 요청 2: 정상 요청
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"fake-agent"}'
```

```bash
# 각 요청의 결과 조회 (<requestId> 에 위에서 적은 값)
curl -s localhost:8000/api/v1/agent/results/<requestId>
```

```bash
# DLQ 에 쌓인 것을 본다 (d2-ai-python 폴더에서)
cd d2-ai-python && python -m app.checks.dlq_tool list
```

```bash
# 워커 상태(kafka_consumer 항목)
curl -s localhost:8000/health
```

```bash
curl -s localhost:8000/metrics | grep -E '^agent_worker_(dlq|messages)'
```

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group agent-workers
```

확인 전 예상:

| 항목                                      | 예상                                                                                                                               |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| 요청 1 결과                               | `FAILED`, 오류에 `ValueError: 지원하지 않는 provider입니다: no-such-provider`                                                       |
| 요청 2 결과                               | `DONE`, 답이 "[fake-agent] 검색 결과 ...자를 받아 답했습니다". 요청 1 이 실패해도 막히지 않는다                                     |
| `dlq_tool list`                           | `DLQ 토픽 agent.requests.dlq ... 1건`. `reason`=`handler_error`, 요청 1 의 `requestId`, 원래 위치 `<파티션>:<offset>`, 오류 `ValueError: 지원하지 않는 provider입니다: no-such-provider`, 실패 시각 |
| `/health` 의 `kafka_consumer`             | `failed` 1, `dlq_sent` 1, `dlq_topic` `agent.requests.dlq`                                                                          |
| `agent_worker_dlq_total{reason="handler_error"}` | 1.0                                                                                                                         |
| `agent_worker_messages_total`             | `result="failed"` 1.0, `result="ok"` 1.0                                                                                            |
| 그룹 LAG                                  | 모든 파티션 0 (실패한 메시지도 DLQ 에 보관한 뒤 commit 했다)                                                                         |


### ③ 해석할 수 없는 메시지 보관

**목적:** JSON 이 아닌 메시지가 워커를 멈추지 않고 DLQ 에 `parse_error` 로 보관되는지 확인한다. ② 의 워커를 그대로 쓴다.

```bash
# JSON 이 아닌 문자열 한 줄을 agent.requests 토픽에 직접 넣는다
echo 'not-json' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic agent.requests
```

```bash
# DLQ 확인 (d2-ai-python 폴더에서)
cd d2-ai-python && python -m app.checks.dlq_tool list
```

```bash
curl -s localhost:8000/health
```

```bash
curl -s localhost:8000/metrics | grep -E '^agent_worker_(dlq|messages)'
```

확인 전 예상:

| 항목                                      | 예상                                                                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| 워커 로그                                 | `해석 불가 메시지 건너뜀 ...` 다음 줄에 `DLQ 보관 reason=parse_error requestId=None ...`                                   |
| `dlq_tool list`                           | 2건. 새로 생긴 행은 `reason`=`parse_error`, `requestId` 는 `-`, 오류는 JSON 해석 오류 문구                                  |
| `/health` 의 `kafka_consumer`             | `skipped` 1, `dlq_sent` 2 (② 의 1건과 합친 값)                                                                              |
| `agent_worker_dlq_total{reason="parse_error"}` | 1.0 (handler_error 는 1.0 그대로)                                                                                      |
| 워커                                      | 계속 `running`. 이 메시지 때문에 멈추지 않는다                                                                              |


### ④ DLQ 에서 다시 처리

**목적:** DLQ 에 보관한 요청을 원인을 고쳐(provider 를 `fake-agent` 로) 다시 보내 정상 처리되는지, parse_error 는 다시 보내지 않는지 확인한다. ② ③ 의 워커를 그대로 쓴다.

```bash
# 보내지 않고 대상만 본다 (<requestId> 는 ② 의 요청 1)
cd d2-ai-python && python -m app.checks.dlq_tool replay --request-id <requestId> --provider fake-agent --dry-run
```

```bash
# 다시 보낸다
cd d2-ai-python && python -m app.checks.dlq_tool replay --request-id <requestId> --provider fake-agent
```

```bash
# 몇 초 뒤 같은 requestId 의 결과를 다시 조회한다
curl -s localhost:8000/api/v1/agent/results/<requestId>
```

```bash
# --all 은 다시 보낼 수 있는 것과 건너뛰는 것을 어떻게 나누는지 본다 (보내지 않음)
cd d2-ai-python && python -m app.checks.dlq_tool replay --all --dry-run
```

확인 전 예상:

| 항목                                    | 예상                                                                                                                              |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `--dry-run` 출력                        | "다시 보낼 대상 1건, 건너뜀 0건"과 요청 1 의 `requestId`. "--dry-run 이라 보내지 않음"                                              |
| 다시 보냄 출력                          | `보냄 requestId=... -> agent.requests partition=<번호> offset=<번호> provider=fake-agent`                                          |
| 결과 조회                               | 같은 `requestId` 가 `FAILED` 에서 `DONE` 으로 바뀜(덮어씀). 답이 "[fake-agent] 검색 결과 ...자를 받아 답했습니다"                    |
| `replay --all --dry-run`                | "다시 보낼 대상 1건, 건너뜀 1건"(대상은 DLQ 에 그대로 남아 있는 handler_error 1건, 건너뜀은 parse_error 1건). DLQ 는 지워지지 않는다 |

확인이 끝나면 워커를 끈다(Ctrl+C).


## 6. 알려진 한계

- ① 은 가짜 producer 로 워커의 판단(무엇을 보낼지, 예외를 올릴지)을 확인한다. 실제 브로커와의 전송, `dlq_tool` 의 Kafka 읽기와 쓰기는 ②③④ 에서만 본다.
- DLQ 토픽은 지워지지 않고 쌓이기만 한다. `replay` 를 반복하면 같은 요청을 반복해서 보내고, 보관 기간(retention)이나 정리 기능은 만들지 않았다.
- DLQ 전송이 실패하면 워커가 재접속하고 같은 메시지를 다시 처리한다. 그 사이 에이전트가 다시 실행되어 모델 호출 비용이 한 번 더 든다. 브로커가 계속 응답하지 않으면 그 파티션은 계속 재처리를 반복한다.
- 처리 실패 요청이 DLQ 로 가려면 워커가 예외를 받아야 한다. 에이전트 안에서 재시도와 정정이 끝난 뒤의 포기 답은 정상 응답으로 끝나므로 DLQ 에 가지 않는다(Day 31).
- 워커가 여러 개여도 DLQ 토픽은 하나다. 어느 워커가 보냈는지는 봉투의 `worker` 에 있다.


## 7. 체크리스트

- [x] ① `python -m app.checks.dlq_check` 가 모든 항목 PASS
- [x] ② 처리 실패 요청이 DLQ 에 보관되고 뒤의 요청이 막히지 않음
- [x] ③ 해석 불가 메시지가 `parse_error` 로 보관됨
- [x] ④ DLQ 에서 다시 보내 정상 처리됨
- [x] DAY32_RESULT.md 작성
