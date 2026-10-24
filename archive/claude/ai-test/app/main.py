"""
FastAPI 앱 진입점

Day 2 목표: 서버가 뜨고, /health가 응답하고,
지금까지 만든 에이전트 호출 로직이 API 엔드포인트로 노출되는 것까지 확인.
"""

from fastapi import FastAPI
from routers import health, agent

app = FastAPI(
    title="AI Agent Backend",
    description="Kafka/Redis/Qdrant 기반 고성능 AI 에이전트 서빙 프로젝트",
    version="0.1.0",
)

# ── 라우터 등록 ──────────────────────────────
app.include_router(health.router)
app.include_router(agent.router)


# 실행: uvicorn main:app --reload --port 8000
