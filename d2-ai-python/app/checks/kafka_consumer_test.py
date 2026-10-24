"""Day 13 - aiokafka Consumer 단독 테스트.

토픽의 메시지를 받아 partition / offset / 내용을 출력한다. 에이전트는 아직 호출하지 않는다.

실행 (d2-ai-python 에서, Kafka 컨테이너가 떠 있어야 함):
    python -m app.checks.kafka_consumer_test
    python -m app.checks.kafka_consumer_test --group g1 --max 3 --timeout 20
"""
import argparse
import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = "agent.requests"


async def main(topic: str, group: str, max_messages: int, timeout: float) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,                       # 같은 group 끼리는 메시지를 나눠 받는다(Day 18)
        auto_offset_reset="earliest",         # 이 group 이 처음이면 토픽의 처음부터 읽는다
        enable_auto_commit=True,              # 읽은 위치(offset)를 자동 저장 -> 다시 실행하면 이어서 받음
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    print(f"[consumer] topic={topic} group={group} bootstrap={BOOTSTRAP} (최대 {max_messages}건, {timeout}초 대기)")
    received = 0
    try:
        while received < max_messages:
            try:
                # getone 이 timeout 안에 안 오면 그만 둔다(무한 대기 방지)
                msg = await asyncio.wait_for(consumer.getone(), timeout=timeout)
            except asyncio.TimeoutError:
                print("[consumer] 시간 초과, 새 메시지 없음")
                break
            received += 1
            v = msg.value
            print(f"[recv] partition={msg.partition} offset={msg.offset} key={msg.key} "
                  f"requestId={v.get('requestId')} question={v.get('question')}")
    finally:
        await consumer.stop()
    print(f"[consumer] 총 {received}건 수신")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Kafka Consumer 단독 테스트")
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--group", default="day13-test")
    ap.add_argument("--max", type=int, default=3, help="받을 최대 메시지 수")
    ap.add_argument("--timeout", type=float, default=15.0, help="새 메시지를 기다리는 최대 초")
    args = ap.parse_args()
    asyncio.run(main(args.topic, args.group, args.max, args.timeout))
