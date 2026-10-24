"""
에이전트 직접 실행 엔드포인트 (Day 5)

POST /api/v1/agent/run  질문을 받아 에이전트(services/agent_graph.py)를 실행하고 답을 바로 돌려준다.
Kafka 를 거치지 않는 동기 경로다. Spring 의 동기 프록시(Day 26)가 이 주소를 부른다.
"""

from fastapi import APIRouter, HTTPException
from app.schemas.models import AgentRequest, AgentResponse
from app.services.agent_graph import run_agent
from app.services.redis_lock import LockTimeout  # Day 21: 같은 세션의 앞 요청이 오래 걸려 락을 못 잡았을 때

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.post("/agent/run", response_model=AgentResponse)
async def run_agent_endpoint(req: AgentRequest):
    # Day 20: session_id 를 넘기면 이전 대화를 이어간다
    # 주의: run_agent 는 동기 함수라 끝날 때까지(20~30초) 이 서버 프로세스의 이벤트 루프가 막힌다(Day 16 의 to_thread 를 쓰지 않는 옛 경로)
    # Day 21: 같은 session_id 의 앞 요청이 끝나길 기다리다 시간이 지나면 500 대신 409(충돌)로 알린다
    try:
        answer = run_agent(req.query, provider=req.provider, session_id=req.session_id)
    except LockTimeout as e:
        raise HTTPException(status_code=409, detail=f"같은 세션의 앞 요청이 아직 처리 중이다: {e}")
    return AgentResponse(answer=answer, provider=req.provider)
