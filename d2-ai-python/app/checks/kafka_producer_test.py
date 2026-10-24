"""Day 13 - aiokafka Producer 단독 테스트.

에이전트나 WAS 와 연결하기 전에 Kafka 로 메시지가 들어가는지만 본다.
Spring Boot(Day 14)가 보낼 메시지와 같은 모양(JSON)으로 보낸다.

실행 (d2-ai-python 에서, Kafka 컨테이너가 떠 있어야 함):
    python -m app.checks.kafka_producer_test
    python -m app.checks.kafka_producer_test --count 5 --topic agent.requests
"""
import argparse
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

# 호스트에서 접속하는 주소. docker-compose 의 EXTERNAL 리스너(localhost:9092).
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = "agent.requests"


def make_message(i: int) -> dict:
    """에이전트 요청 메시지. 이 스키마를 Day 14 의 Spring Producer 도 그대로 쓴다."""
    return {
        "requestId": str(uuid.uuid4()),   # 요청 추적용 id (202 응답에 담아 돌려줄 값)
        "question": f"테스트 질문 {i}: 단순 변심으로 환불하면 배송비는 누가 내요?",
        "provider": "gemini",
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }


async def main(topic: str, count: int) -> None:
    # value_serializer: dict -> JSON 바이트. Kafka 는 바이트만 다룬다.
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()   # 브로커 연결. 실패하면 여기서 예외
    try:
        for i in range(1, count + 1):
            msg = make_message(i)
            
            print(f"[sending] msg={msg}")
            # key 가 같으면 같은 파티션으로 간다(순서 보장 단위). 여기서는 requestId 를 key 로 쓴다.
            meta = await producer.send_and_wait(topic, value=msg, key=msg["requestId"])
            print(f"[sent] partition={meta.partition} offset={meta.offset} requestId={msg['requestId']}")
    finally:
        await producer.stop()   # 남은 배치를 비우고 연결 종료


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Kafka Producer 단독 테스트")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--count", type=int, default=3, help="보낼 메시지 수")
    args = ap.parse_args()
    asyncio.run(main(args.topic, args.count))
