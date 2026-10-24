"""
대화 기록 확인용 엔드포인트 (Day 20)

GET    /api/v1/agent/sessions/{session_id}  저장된 대화와 남은 만료 시간. 없으면 404.
DELETE /api/v1/agent/sessions/{session_id}  대화를 지운다(새로 시작).

Redis 에 실제로 무엇이 저장됐는지 redis-cli 없이 HTTP 로 보려는 확인용이다.
Redis 호출은 동기 함수라 def 로 두면 FastAPI 가 스레드풀에서 실행한다(이벤트 루프를 막지 않는다).
"""

from fastapi import APIRouter, HTTPException

from app.services import conversation_store

router = APIRouter(prefix="/api/v1/agent", tags=["sessions"])


@router.get("/sessions/{session_id}")
def get_session(session_id: str):
    try:
        history = conversation_store.load_history(session_id)
        ttl = conversation_store.ttl_left(session_id)
    except Exception as e:  # Redis 장애
        raise HTTPException(status_code=503, detail=f"Redis 를 쓸 수 없음: {type(e).__name__}: {e}")
    if not history:
        raise HTTPException(status_code=404, detail="저장된 대화가 없다. 없는 sessionId 이거나 만료됐을 수 있다")
    return {"sessionId": session_id, "turns": len(history) // 2, "ttlSec": ttl, "messages": history}


@router.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    try:
        deleted = conversation_store.clear(session_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis 를 쓸 수 없음: {type(e).__name__}: {e}")
    return {"sessionId": session_id, "deleted": deleted}
