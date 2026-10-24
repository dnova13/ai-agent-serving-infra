"""
Prometheus 지표 정의 (Day 30, Day 31 에 도구 실패 지표, Day 32 에 DLQ 지표, Day 36 에 모델 폴백 지표 추가)

[이 모듈이 하는 일]
  Kafka 워커가 메시지를 처리한 결과와 처리 시간을 Prometheus 지표로 기록한다. 기록만 하고 서버는 열지 않는다.
  지표는 FastAPI 의 /metrics (main.py 에서 열림)에서 Prometheus 가 가져간다.
  HTTP 요청 수와 응답 시간은 main.py 의 Instrumentator 가 따로 기록한다.

[지표]
  agent_worker_messages_total{result}   워커가 끝낸 메시지 수. result = ok(성공), failed(처리 실패로 건너뜀), skipped(해석 불가로 건너뜀)
  agent_worker_handle_seconds           메시지 한 건을 처리하는 데 걸린 시간(에이전트 호출 포함). 성공과 실패 모두 기록
  agent_worker_commits_total            commit 에 성공한 횟수
  agent_worker_up                       워커가 Kafka 에 접속해 수신 대기 중이면 1, 아니면 0
  agent_worker_dlq_total{reason}        DLQ 토픽에 보관한 메시지 수 (Day 32). reason = handler_error(처리 실패) / parse_error(해석 불가)
  agent_tool_failures_total{tool,kind}  도구 실행 실패 횟수 (Day 31). kind = transient(일시 장애, 같은 호출 재실행) / input(입력 오류, 모델이 고쳐 재호출)
  agent_tool_recoveries_total{via}      실패 뒤 도구가 성공한 횟수 (Day 31). via = retry(일시 장애 재실행으로 복구) / correction(입력 정정으로 복구)
  agent_model_fallbacks_total{from_model,reason}  앞 모델 대신 다음 모델로 넘어간 횟수 (Day 36). reason = error(앞 모델 실패) / breaker_open(브레이커가 열려 건너뜀)
  agent_model_served_total{model}       폴백이 켜진 요청(provider 에 ">")에서 답한 모델의 호출 수 (Day 36)
  agent_give_ups_total{reason}          한도를 넘겨 포기한 횟수 (Day 31). reason = retry_exhausted / correction_exhausted

[주의]
  워커를 2개 띄우면(8000, 8001) 프로세스마다 지표가 따로이고, Prometheus 가 instance 라벨(포트)로 구분한다.
"""

from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter(
    "agent_worker_messages_total",
    "워커가 끝낸 Kafka 메시지 수",
    ["result"],
)

# 가짜 모델(5초 지연)부터 실제 LLM(수십 초)까지 보이도록 구간을 넓게 잡았다.
HANDLE_SECONDS = Histogram(
    "agent_worker_handle_seconds",
    "메시지 한 건 처리 시간(초)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 7.5, 10, 15, 30, 60),
)

COMMITS = Counter("agent_worker_commits_total", "commit 성공 횟수")

WORKER_UP = Gauge("agent_worker_up", "워커가 Kafka 수신 대기 중이면 1")

# Day 31: 도구 실패와 Self-Correction. agent_graph.py 의 tool_node, give_up_node 가 센다
TOOL_FAILURES = Counter(
    "agent_tool_failures_total",
    "도구 실행 실패 횟수",
    ["tool", "kind"],
)

TOOL_RECOVERIES = Counter(
    "agent_tool_recoveries_total",
    "실패 뒤 도구가 성공한 횟수",
    ["via"],
)

GIVE_UPS = Counter(
    "agent_give_ups_total",
    "재시도 또는 정정 한도를 넘겨 포기한 횟수",
    ["reason"],
)

# Day 32: DLQ 보관. kafka_worker.py 의 _send_to_dlq 가 센다(브로커가 받았다는 응답을 받은 뒤에 센다)
DLQ = Counter(
    "agent_worker_dlq_total",
    "DLQ 토픽에 보관한 메시지 수",
    ["reason"],
)

# Day 36: 모델 폴백. model_fallback.py 의 FallbackChatModel 이 센다
MODEL_FALLBACKS = Counter(
    "agent_model_fallbacks_total",
    "앞 모델 대신 다음 모델로 넘어간 횟수",
    ["from_model", "reason"],
)
MODEL_SERVED = Counter(
    "agent_model_served_total",
    "폴백이 켜진 요청에서 실제로 답한 모델의 호출 수",
    ["model"],
)
