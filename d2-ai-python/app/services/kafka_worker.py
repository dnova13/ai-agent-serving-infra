"""Day 15 - FastAPI 서버 안에서 도는 Kafka Consumer 워커.

서버가 켜질 때(lifespan 시작) 백그라운드 태스크로 시작하고, 꺼질 때(lifespan 종료) 멈춘다.
Spring WAS 가 agent.requests 토픽에 넣은 메시지를 받아 handler 에 넘긴다.
Day 15 의 handler 는 로그만 남긴다. 에이전트 호출로 바꾸는 것은 Day 16.

설계
- Kafka 가 꺼져 있어도 서버(/health 등)는 뜬다. 워커가 백그라운드에서 재접속을 계속 시도한다.
- 서버를 끌 때 태스크를 취소하고 consumer.stop() 으로 그룹에서 정상 탈퇴한다(다른 워커가 바로 이어받는다. Day 18).
- 해석할 수 없는 메시지는 로그만 남기고 건너뛴다(한 건 때문에 워커가 멈추면 뒤의 메시지가 모두 막힌다).
- Day 32: 건너뛴 메시지(처리 실패, 해석 불가)는 버리지 않고 DLQ 토픽(agent.requests.dlq)에 원본과 실패 정보를 붙여 보관한다.
  보관한 뒤에 원래 메시지를 commit 한다. 보관에 실패하면 commit 하지 않고 재접속해 같은 메시지를 다시 받는다(손실보다 재처리를 택함).
- 상태(state)와 받은 건수를 snapshot() 으로 노출해 /health 에서 보여 준다.
"""
import asyncio
import json
import logging
import os
import time  # Day 30: 메시지 한 건 처리 시간 측정
from datetime import datetime, timezone  # Day 32: DLQ 에 실패 시각을 남긴다
from typing import Awaitable, Callable, Optional

# Day 32: 실패한 메시지를 DLQ 토픽으로 보낼 producer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
from aiokafka.abc import ConsumerRebalanceListener  # Day 18: 파티션이 배정/회수될 때 알림을 받는다
# Day 18: 리밸런싱 중에 commit 하면 CommitFailedError 가 난다
from aiokafka.errors import CommitFailedError, KafkaConnectionError
from pydantic import ValidationError

from app.schemas.models import AgentRequestMessage
from app.services import metrics  # Day 30: Prometheus 지표(처리 건수, 처리 시간)

# uvicorn 은 자기 로거(uvicorn.*)만 설정하고 루트 로거는 WARNING 이라, 설정 없이는 이 모듈의 INFO 로그
#       ("워커 시작", "Kafka 접속 완료", "[recv]")가 화면에 나오지 않는다. 이 로거에 직접 INFO 와 출력 핸들러를 건다.
logger = logging.getLogger("kafka_worker")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(levelname)s:     [kafka_worker] %(message)s"))
    logger.addHandler(_handler)
    logger.propagate = False  # 루트 로거에도 핸들러가 있으면 같은 줄이 두 번 나오는 것을 막는다

# 환경변수로 바꿀 수 있고, 없으면 Day 13/14 에서 쓴 값을 쓴다.
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "agent.requests")
GROUP = os.getenv("KAFKA_GROUP", "agent-workers")
# Day 16: 에이전트(LLM)를 붙이면 새 그룹이 토픽 전체를 읽는 순간 옛 메시지마다 LLM 호출이 나가므로 latest 로 바꿨다.
#       이미 commit 된 위치가 있는 그룹(agent-workers)은 이 값과 상관없이 저장된 위치부터 이어서 읽는다.
OFFSET_RESET = os.getenv("KAFKA_OFFSET_RESET", "latest")
# Day 16: commit 방식. manual = 처리가 끝난 뒤 commit(기본), auto = Day 13/15 처럼 읽으면 주기적으로 자동 commit
COMMIT_MODE = os.getenv("KAFKA_COMMIT_MODE", "manual")
# Day 18: 같은 그룹에 워커를 여러 개 띄울 때 로그와 /health 에서 구분하려는 이름. 없으면 프로세스 번호로 만든다.
WORKER_ID = os.getenv("KAFKA_WORKER_ID", f"worker-{os.getpid()}")
RETRY_SEC = float(os.getenv("KAFKA_RETRY_SEC", "5"))
ENABLED = os.getenv("KAFKA_CONSUMER_ENABLED", "true").lower() == "true"
# Day 32: 실패한 메시지를 보관하는 토픽(Dead Letter Queue)과 켜고 끄는 스위치. 끄면 예전처럼 건너뛰기만 한다.
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "agent.requests.dlq")
DLQ_ENABLED = os.getenv("KAFKA_DLQ_ENABLED", "true").lower() == "true"

Handler = Callable[[AgentRequestMessage, ConsumerRecord], Awaitable[None]]


async def log_only_handler(message: AgentRequestMessage, record: ConsumerRecord) -> None:
    """Day 15 의 처리: 받았다는 로그만 남긴다. Day 16 에서 에이전트 호출로 교체."""
    logger.info(
        "[recv] partition=%s offset=%s requestId=%s provider=%s createdAt=%s question=%s",
        record.partition, record.offset, message.requestId, message.provider,
        message.createdAt.isoformat(), message.question,
    )


class _RebalanceLogger(ConsumerRebalanceListener):
    """Day 18: 그룹의 구성원이 바뀌어 파티션이 다시 나뉠 때(리밸런싱) 이 워커가 어떤 파티션을 맡는지 기록한다."""

    def __init__(self, worker: "KafkaWorker") -> None:
        self._worker = worker

    def on_partitions_revoked(self, revoked) -> None:
        # 리밸런싱이 시작되면 맡고 있던 파티션을 모두 내놓는다(이후 on_partitions_assigned 에서 새로 받는다).
        if revoked:
            logger.info("[%s] 파티션 회수 %s", WORKER_ID, sorted(tp.partition for tp in revoked))
        self._worker.partitions = []

    def on_partitions_assigned(self, assigned) -> None:
        self._worker.partitions = sorted(tp.partition for tp in assigned)
        self._worker.rebalances += 1
        logger.info("[%s] 파티션 배정 %s (리밸런싱 %s번째)", WORKER_ID, self._worker.partitions, self._worker.rebalances)


class KafkaWorker:
    def __init__(self, handler: Handler = log_only_handler) -> None:
        self._handler = handler
        self._task: Optional[asyncio.Task] = None
        self.state = "stopped"  # stopped | connecting | running | error
        self.received = 0       # handler 까지 성공한 건수
        self.skipped = 0        # 해석하지 못해 건너뛴 건수
        self.failed = 0         # Day 16: handler(에이전트)가 실패해 건너뛴 건수
        self.committed = 0      # Day 16: 수동 commit 을 한 횟수
        self.current_request_id: Optional[str] = None  # Day 16: 지금 처리 중인 요청(없으면 None)
        self.last_error: Optional[str] = None
        self.partitions: list = []   # Day 18: 지금 이 워커가 맡은 파티션 번호
        self.rebalances = 0          # Day 18: 파티션을 새로 배정받은 횟수(처음 가입 포함)
        self.dlq_sent = 0            # Day 32: DLQ 토픽에 보관한 건수
        self._producer: Optional[AIOKafkaProducer] = None  # Day 32: DLQ 로 보내는 producer(접속 중에만 있음)

    async def start(self) -> None:
        """백그라운드 태스크로 시작하고 바로 반환한다(Kafka 접속을 기다리지 않는다)."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="kafka-worker")
        logger.info("워커 시작 요청 topic=%s group=%s bootstrap=%s", TOPIC, GROUP, BOOTSTRAP)

    async def stop(self) -> None:
        """태스크를 취소하고 끝날 때까지 기다린다. _run() 의 finally 가 consumer.stop() 을 호출한다."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self.state = "stopped"
        logger.info("워커 종료 received=%s skipped=%s", self.received, self.skipped)

    def snapshot(self) -> dict:
        return {
            "state": self.state,
            "topic": TOPIC,
            "group": GROUP,
            "received": self.received,
            "skipped": self.skipped,
            "failed": self.failed,                          # Day 16
            "committed": self.committed,                    # Day 16
            "commit_mode": COMMIT_MODE,                     # Day 16
            "current_request_id": self.current_request_id,  # Day 16
            "worker_id": WORKER_ID,                         # Day 18
            "partitions": self.partitions,                  # Day 18: 맡은 파티션 (워커가 파티션보다 많으면 빈 목록인 워커가 생긴다)
            "rebalances": self.rebalances,                  # Day 18
            "dlq_topic": DLQ_TOPIC if DLQ_ENABLED else None,  # Day 32: 꺼져 있으면 None
            "dlq_sent": self.dlq_sent,                      # Day 32
            "last_error": self.last_error,
        }

    async def _run(self) -> None:
        # 접속이 끊기거나 처음부터 안 되면 RETRY_SEC 간격으로 다시 시도한다.
        while True:
            # Day 18: 토픽은 아래에서 subscribe(listener=...) 로 구독해 파티션 배정을 기록한다.
            consumer = AIOKafkaConsumer(
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP,
                auto_offset_reset=OFFSET_RESET,  # 이 group 이 처음이면 어디서부터 읽을지
                # Day 16: 기본은 수동 commit. 처리가 끝난 메시지까지만 위치를 저장한다(처리 도중 죽으면 다음에 같은 메시지를 다시 받는다).
                enable_auto_commit=(COMMIT_MODE == "auto"),
            )
            consumer.subscribe([TOPIC], listener=_RebalanceLogger(self))
            producer = None  # Day 32
            try:
                self.state = "connecting"
                await consumer.start()
                # Day 32: DLQ 로 보낼 producer. consumer 와 같은 접속 수명으로 열고 닫는다(시작에 실패하면 아래 재접속 루프로 간다)
                if DLQ_ENABLED:
                    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
                    await producer.start()
                    self._producer = producer
                self.state = "running"
                metrics.WORKER_UP.set(1)  # Day 30
                self.last_error = None
                logger.info("[%s] Kafka 접속 완료, 수신 대기", WORKER_ID)
                async for record in consumer:
                    await self._handle(record)
                    # Day 16: 처리(성공, 실패로 건너뜀, 해석 불가로 건너뜀 모두)가 끝난 뒤에 이 메시지까지 commit 한다.
                    #       처리 중에 서버가 꺼지면(CancelledError 등) 여기까지 오지 못하므로 commit 되지 않는다.
                    if COMMIT_MODE == "manual":
                        # Day 18: 처리하는 사이 리밸런싱이 일어나 이 파티션이 다른 워커에게 넘어갔으면 commit 이 거절된다.
                        #       이 메시지는 commit 되지 않은 채라서 새로 맡은 워커가 다시 받아 처리한다(같은 요청이 두 번 처리될 수 있다).
                        try:
                            await consumer.commit()
                            self.committed += 1
                            metrics.COMMITS.inc()  # Day 30
                        except CommitFailedError:
                            logger.warning("[%s] commit 거절(리밸런싱으로 파티션을 잃음), 새 담당 워커가 다시 처리할 수 있다 partition=%s offset=%s",
                                           WORKER_ID, record.partition, record.offset)
            except asyncio.CancelledError:
                raise
            except (KafkaConnectionError, OSError) as e:
                self.state = "error"
                # 메시지가 예외 이름으로 시작하면 이름을 다시 붙이지 않는다
                self.last_error = str(e) if str(e).startswith(type(e).__name__) else f"{type(e).__name__}: {e}"
                logger.warning("Kafka 접속 실패, %s초 뒤 재시도 (%s)", RETRY_SEC, self.last_error)
            except Exception as e:
                self.state = "error"
                self.last_error = f"{type(e).__name__}: {e}"
                logger.exception("워커 오류, %s초 뒤 재시도", RETRY_SEC)
            finally:
                self.partitions = []  # Day 18: consumer 를 닫으면 맡은 파티션이 없다
                # Day 32: producer 도 닫는다
                self._producer = None
                if producer is not None:
                    try:
                        await producer.stop()
                    except Exception:
                        logger.debug("producer.stop() 중 오류 무시", exc_info=True)
                metrics.WORKER_UP.set(0)  # Day 30
                # 시작에 실패했어도 내부 연결을 정리한다. 취소로 들어와도 실행된다.
                try:
                    await consumer.stop()
                except Exception:
                    logger.debug("consumer.stop() 중 오류 무시", exc_info=True)
            await asyncio.sleep(RETRY_SEC)

    async def _send_to_dlq(self, record: ConsumerRecord, reason: str, error: BaseException,
                           message: Optional[AgentRequestMessage] = None) -> None:
        """Day 32: 처리하지 못한 메시지를 DLQ 토픽에 보관한다. 보관이 끝나야(브로커가 받았다는 응답) 돌아온다.

        보관하는 것: 원본 메시지 문자열(originalValue), 실패 종류(reason: handler_error 처리 실패 / parse_error 해석 불가),
        오류 내용, 요청 번호(해석 불가면 None), 원래 위치(토픽, 파티션, offset), 실패한 워커와 시각.
        보내다 실패하면 예외를 그대로 올린다. 호출한 쪽(_handle)을 지나 _run 까지 올라가 재접속하고, commit 하지 않은 이 메시지를 다시 받는다.
        """
        if not DLQ_ENABLED:
            return
        if self._producer is None:
            # 접속 중에는 producer 가 항상 있어야 한다. 없으면 보관하지 못한 채 commit 되지 않게 예외로 올린다.
            raise RuntimeError("DLQ producer 가 없다")
        envelope = {
            "reason": reason,
            "error": f"{type(error).__name__}: {error}",
            "requestId": message.requestId if message is not None else None,
            "worker": WORKER_ID,
            "failedAt": datetime.now(timezone.utc).isoformat(),
            "source": {"topic": record.topic, "partition": record.partition, "offset": record.offset},
            # 깨진 바이트가 있어도 보관은 되도록 대체 문자로 바꿔 문자열로 만든다
            "originalValue": record.value.decode("utf-8", errors="replace"),
        }
        key = record.key if record.key is not None else (message.requestId.encode("utf-8") if message is not None else None)
        await self._producer.send_and_wait(DLQ_TOPIC, value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"), key=key)
        self.dlq_sent += 1
        metrics.DLQ.labels(reason=reason).inc()
        logger.warning("DLQ 보관 reason=%s requestId=%s partition=%s offset=%s", reason, envelope["requestId"], record.partition, record.offset)

    async def _handle(self, record: ConsumerRecord) -> None:
        try:
            message = AgentRequestMessage.model_validate(json.loads(record.value.decode("utf-8")))
        except (ValueError, ValidationError, UnicodeDecodeError) as e:
            # json.JSONDecodeError 와 pydantic ValidationError 는 모두 ValueError 의 하위 클래스다.
            self.skipped += 1
            metrics.MESSAGES.labels(result="skipped").inc()  # Day 30
            logger.warning("해석 불가 메시지 건너뜀 partition=%s offset=%s (%s)", record.partition, record.offset, e)
            await self._send_to_dlq(record, "parse_error", e)  # Day 32: 건너뛰되 버리지 않고 DLQ 에 보관
            return
        # Day 16: 에이전트가 실패(LLM 오류, 타임아웃 등)해도 워커가 재접속 루프로 빠지지 않게 여기서 잡는다.
        #       실패한 메시지는 failed 로 세고 건너뛴다(계속 재시도하면 이 메시지 하나 때문에 뒤의 요청이 모두 막힌다).
        #       실패한 요청을 따로 보관하는 DLQ 는 Day 32(아래 except 에서 _send_to_dlq). 서버 종료(CancelledError)는 여기서 잡지 않는다.
        self.current_request_id = message.requestId
        t0 = time.perf_counter()  # Day 30
        try:
            await self._handler(message, record)
            self.received += 1
            # Day 30: 성공 건수와 처리 시간 기록
            metrics.MESSAGES.labels(result="ok").inc()
            metrics.HANDLE_SECONDS.observe(time.perf_counter() - t0)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # Day 32: DLQ 에 오류 내용을 남기려고 e 를 받는다
            self.failed += 1
            # Day 30: 실패 건수와 처리 시간 기록
            metrics.MESSAGES.labels(result="failed").inc()
            metrics.HANDLE_SECONDS.observe(time.perf_counter() - t0)
            logger.exception("처리 실패, 건너뜀 requestId=%s partition=%s offset=%s",
                             message.requestId, record.partition, record.offset)
            await self._send_to_dlq(record, "handler_error", e, message)  # Day 32: 건너뛰되 버리지 않고 DLQ 에 보관
        finally:
            self.current_request_id = None
