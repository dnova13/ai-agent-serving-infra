"""Day 16 - Kafka 메시지를 에이전트에 연결하는 handler.

KafkaWorker(handler=agent_handler) 로 넣으면, 토픽에서 받은 요청마다 run_agent() 를 호출한다.
결과는 로그로 남기고, Day 17 부터는 result_store 에도 둔다(Day 17~22 는 프로세스 메모리, Day 23 부터는 Redis).

필요한 환경: Qdrant 컨테이너(검색 도구), .env 의 GOOGLE_API_KEY (Day 10~11 에서 쓰던 것과 같다).
"""
import asyncio
import logging
import os
import time

from aiokafka import ConsumerRecord

from app.schemas.models import AgentRequestMessage
from app.services.agent_graph import run_agent
from app.services.result_store import store  # Day 17: 답을 임시 보관 (Day 23 부터 Redis)
from app.services.kafka_worker import WORKER_ID  # Day 18: 어느 워커가 처리했는지 로그와 결과에 남긴다

# kafka_worker 로거의 하위 로거. 출력 핸들러는 부모(kafka_worker)의 것을 쓴다.
logger = logging.getLogger("kafka_worker.agent")

# 에이전트 한 건에 허용하는 최대 시간(초). 넘으면 실패로 처리하고 다음 메시지로 넘어간다.
AGENT_TIMEOUT_SEC = float(os.getenv("AGENT_TIMEOUT_SEC", "120"))
# 실험용: 에이전트를 부르기 전에 이만큼 기다린다. "처리 중" 상태를 길게 만들어 그 사이에 서버를 끄는 실험을 하려는 것(기본 0).
DEBUG_DELAY_SEC = float(os.getenv("AGENT_DEBUG_DELAY_SEC", "0"))


async def agent_handler(message: AgentRequestMessage, record: ConsumerRecord) -> None:
    # Day 18: 어느 워커(worker)가 받았는지 추가
    logger.info("[agent] 시작 worker=%s requestId=%s partition=%s offset=%s provider=%s sessionId=%s",
                WORKER_ID, message.requestId, record.partition, record.offset, message.provider, message.sessionId)
    # Day 17: 받은 즉시 PROCESSING (실험용 대기 중에도 조회된다). Day 18: 워커, 파티션, offset 도 함께 남긴다
    # Day 23: store 가 Redis 를 부르는 동기 함수라 스레드에서 실행한다(이벤트 루프를 막지 않는다)
    await asyncio.to_thread(store.set_processing, message.requestId, worker=WORKER_ID, partition=record.partition, offset=record.offset)
    if DEBUG_DELAY_SEC > 0:
        await asyncio.sleep(DEBUG_DELAY_SEC)

    started = time.perf_counter()
    # run_agent 는 동기 함수(LLM 호출을 기다리며 스레드를 막는다)라서 이벤트 루프에서 직접 부르면
    # 그동안 /health 같은 HTTP 요청도 멈춘다. 별도 스레드에서 실행한다.
    # 주의: 타임아웃이 나도 스레드 안의 호출 자체는 멈추지 못하고 결과만 버려진다.
    # Day 17: 성공/실패를 store 에 남긴다. 실패는 다시 던져서 워커가 failed 로 세게 한다(Day 16 동작 그대로).
    try:
        answer = await asyncio.wait_for(
            # Day 20: sessionId 를 넘겨 같은 세션의 대화를 Redis 에서 이어간다
            # Kafka 메시지의 sessionId 가 그대로 run_agent 의 session_id 가 된다 (null 이면 새 대화). 흐름은 agent_graph.run_agent 주석 참고
            asyncio.to_thread(run_agent, message.question, message.provider, message.sessionId),
            timeout=AGENT_TIMEOUT_SEC,
        )
    except Exception as e:  # CancelledError(서버 종료)는 Exception 이 아니라서 여기서 잡히지 않는다
        reason = str(e) or (f"{AGENT_TIMEOUT_SEC:g}초 안에 끝나지 않음" if isinstance(e, asyncio.TimeoutError) else "원인 메시지 없음")
        await asyncio.to_thread(store.set_failed, message.requestId, f"{type(e).__name__}: {reason}")  # Day 23
        raise
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    await asyncio.to_thread(store.set_done, message.requestId, str(answer), elapsed_ms)  # Day 23

    preview = " ".join(str(answer).split())[:200]
    # Day 18: worker 추가
    logger.info("[agent] 완료 worker=%s requestId=%s elapsed_ms=%s answer_len=%s answer=%s",
                WORKER_ID, message.requestId, elapsed_ms, len(str(answer)), preview)
