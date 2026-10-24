"""
헬스체크 엔드포인트

GET /health  서버가 살아 있으면 status 는 ok 다. Kafka 워커 상태(Day 15)와 Redis 연결 상태(Day 19)를 같이 보여 준다.
"""

# Day 15: Kafka 워커 상태를 보여 주려고 Request 추가
from fastapi import APIRouter, Request
import asyncio  # Day 19: Redis ping 은 동기 함수라 스레드에서 부른다
from app.services import redis_client  # Day 19

router = APIRouter(tags=["health"])


# 서버 자체가 살아 있으면 status 는 ok 다. Kafka 가 꺼져 있어도 ok 이고, 워커 상태는 kafka_consumer 에 따로 보인다.
# 워커를 끄면(KAFKA_CONSUMER_ENABLED=false) kafka_consumer 는 null.
@router.get("/health")
async def health_check(request: Request):
    worker = getattr(request.app.state, "kafka_worker", None)
    # Day 19: Redis 연결 상태. Redis 가 꺼져 있어도 /health 는 200 이고 status 는 ok 다(Kafka 와 같은 방침).
    #       ping 은 동기 호출이고 접속 실패 시 최대 1초(REDIS_CONNECT_TIMEOUT_SEC) 걸리므로 이벤트 루프를 막지 않게 스레드에서 부른다.
    redis_status = await asyncio.to_thread(redis_client.ping)
    return {
        "status": "ok",
        "kafka_consumer": worker.snapshot() if worker is not None else None,
        "redis": redis_status,
    }
