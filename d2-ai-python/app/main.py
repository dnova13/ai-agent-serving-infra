"""
FastAPI 앱 진입점

- 라우터: /health, 에이전트 직접 실행(/api/v1/agent/run), RAG 비교(/api/v1/rag/*), 결과 조회, 대화 기록
- Day 15: 서버가 뜰 때 Kafka Consumer 워커를 함께 띄운다(lifespan). 요청은 Spring → Kafka → 이 워커로 온다.
- Day 30: /metrics 로 Prometheus 지표를 연다.

실행: d2-ai-python 폴더에서 uvicorn app.main:app --port 8000
"""

from contextlib import asynccontextmanager  # Day 15: lifespan
import os  # Day 16: KAFKA_HANDLER

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator  # Day 30: HTTP 요청 수와 응답 시간 지표
# Day 10: Qdrant 사용 여부 비교 라우터(compare) 추가
# Day 17: 임시 결과 조회(results) 추가
# Day 20: 대화 기록 확인(sessions) 추가
from app.routers import health, agent, compare, results, sessions
from app.services import kafka_worker  # Day 15: Kafka Consumer 워커


# ── Day 15: 서버 시작/종료에 맞춰 Kafka Consumer 를 켜고 끈다 ──────────
# yield 앞이 서버 시작 때, 뒤가 서버 종료 때 실행된다. start() 는 Kafka 접속을 기다리지 않고
# 바로 돌아오므로 Kafka 가 꺼져 있어도 서버는 뜬다. KAFKA_CONSUMER_ENABLED=false 면 워커를 만들지 않는다.
@asynccontextmanager
async def lifespan(app: FastAPI):
    worker = None
    if kafka_worker.ENABLED:
        # Day 16: 기본은 에이전트 호출. KAFKA_HANDLER=log 이면 Day 15 처럼 받았다는 로그만 남긴다(LLM 호출 없이 배선만 볼 때)
        if os.getenv("KAFKA_HANDLER", "agent") == "log":
            worker = kafka_worker.KafkaWorker(handler=kafka_worker.log_only_handler)
        else:
            from app.services.agent_handler import agent_handler
            worker = kafka_worker.KafkaWorker(handler=agent_handler)
        app.state.kafka_worker = worker  # /health 에서 상태를 읽는다
        await worker.start()
    yield
    if worker is not None:
        await worker.stop()


# lifespan 인자 추가
app = FastAPI(
    title="AI Agent Backend",
    description="Kafka/Redis/Qdrant 기반 고성능 AI 에이전트 서빙 프로젝트",
    version="0.1.0",
    lifespan=lifespan,
)

# ── 라우터 등록 ──────────────────────────────
app.include_router(health.router)
app.include_router(agent.router)
app.include_router(compare.router)  # Day 10: POST /api/v1/rag/compare
app.include_router(results.router)  # Day 17: GET /api/v1/agent/results/{requestId} (메모리 임시 조회)
app.include_router(sessions.router)  # Day 20: GET/DELETE /api/v1/agent/sessions/{sessionId} (Redis 대화 기록)

# Day 30: GET /metrics 로 Prometheus 지표를 연다(HTTP 요청 수와 응답 시간 + services/metrics.py 의 워커 지표).
#       /metrics 자신의 요청은 지표에서 뺀다. Swagger(/docs) 목록에도 나오지 않게 한다.
Instrumentator(excluded_handlers=["/metrics"]).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

