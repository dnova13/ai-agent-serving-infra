"""
헬스체크 엔드포인트

이후 Kafka/Redis/Qdrant 연동되면 여기서 각 인프라 연결 상태도 같이 체크하도록 확장 예정
(3~4주 차에 확장)
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    return {"status": "ok"}
