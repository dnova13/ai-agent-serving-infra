"""
에이전트 실행 엔드포인트

Day 1~2 사이에 만든 agent_interactions_api.py의 run_agent()를
FastAPI 엔드포인트로 그대로 이식.
(2주 차에 Qdrant 실제 연동되면 execute_search_vector_db 내부만 교체됨)
"""

from fastapi import APIRouter
from schemas.models import AgentRequest, AgentResponse
from services.agent_core import run_agent

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    answer = run_agent(req.query, provider=req.provider)
    return AgentResponse(answer=answer, provider=req.provider)
