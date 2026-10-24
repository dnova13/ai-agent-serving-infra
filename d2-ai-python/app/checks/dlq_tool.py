"""
DLQ 확인과 재처리 도구 (Day 32)

워커가 처리하지 못해 agent.requests.dlq 토픽에 보관한 메시지를 읽어 보여 주고, 원래 토픽(agent.requests)으로 다시 보낸다.
DLQ 메시지는 워커의 kafka_worker.py(_send_to_dlq)가 만든 JSON 이다:
    reason(handler_error 처리 실패 / parse_error 해석 불가), error, requestId, worker, failedAt,
    source(topic, partition, offset), originalValue(원래 메시지 문자열)

명령 (d2-ai-python 폴더에서, 프로젝트 파이썬 가상환경)
    python -m app.checks.dlq_tool list                              # DLQ 에 쌓인 메시지를 처음부터 끝까지 표로 출력
    python -m app.checks.dlq_tool replay --request-id <requestId>   # 그 요청 한 건을 agent.requests 로 다시 보냄
    python -m app.checks.dlq_tool replay --all                      # 다시 보낼 수 있는 것을 전부 다시 보냄
    옵션: --provider <이름> 다시 보낼 때 provider 를 바꿈(예: 원인이 잘못된 provider 였을 때 고쳐서 보냄), --dry-run 보내지 않고 대상만 출력

주의
- 다시 보낼 수 있는 것은 reason 이 handler_error 인 것뿐이다. parse_error 는 원본 자체가 깨져 있어 다시 보내지 않는다.
- Kafka 토픽은 지워지지 않아서, replay 를 두 번 하면 두 번 보낸다. 같은 요청 번호로 다시 보내므로 결과는 덮어쓴다.
- 이 파일은 프로젝트의 다른 모듈을 불러오지 않는다(워커의 로그 설정 같은 부작용을 피하려는 것).
"""

import argparse
import asyncio
import json
import os
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "agent.requests")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "agent.requests.dlq")


# ── 순수 함수 (Kafka 없이 확인할 수 있는 부분) ──────────────────────────────

def parse_entry(dlq_offset: int, raw: bytes) -> dict:
    """DLQ 메시지 바이트를 사전으로 바꾸고 DLQ 안의 위치(dlqOffset)를 붙인다. JSON 이 아니면 reason 을 unknown 으로 둔다."""
    try:
        entry = json.loads(raw.decode("utf-8"))
        if not isinstance(entry, dict):
            raise ValueError("JSON 객체가 아님")
    except (ValueError, UnicodeDecodeError):
        entry = {"reason": "unknown", "error": "DLQ 메시지를 해석하지 못함", "requestId": None,
                 "originalValue": raw.decode("utf-8", errors="replace")}
    entry["dlqOffset"] = dlq_offset
    return entry


def select_replayable(entries: list, request_id: str | None = None, replay_all: bool = False) -> tuple[list, list]:
    """다시 보낼 항목과 건너뛸 항목을 나눈다. 다시 보낼 수 있는 것은 reason 이 handler_error 이고 requestId 가 있는 것뿐이다.
    request_id 를 주면 그 요청만, replay_all 이면 가능한 전부를 고른다. 둘 다 아니면 아무것도 고르지 않는다."""
    selected, skipped = [], []
    for e in entries:
        if request_id is not None and e.get("requestId") != request_id:
            continue
        if request_id is None and not replay_all:
            continue
        if e.get("reason") == "handler_error" and e.get("requestId"):
            selected.append(e)
        else:
            skipped.append(e)
    return selected, skipped


def build_replay_value(entry: dict, provider: str | None = None) -> bytes:
    """원본 메시지 문자열을 다시 보낼 바이트로 만든다. provider 를 주면 그 필드만 바꾼다."""
    message = json.loads(entry["originalValue"])
    if provider:
        message["provider"] = provider
    return json.dumps(message, ensure_ascii=False).encode("utf-8")


# ── Kafka 입출력 ───────────────────────────────────────────────────────────

async def read_all() -> list:
    """DLQ 토픽을 처음부터 현재 끝까지 읽는다(그룹 없이, 위치를 저장하지 않는다). 토픽이 없으면 빈 목록."""
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=None, enable_auto_commit=False)
    await consumer.start()
    try:
        parts = consumer.partitions_for_topic(DLQ_TOPIC)
        if not parts:
            await consumer.topics()  # 토픽 목록과 메타데이터를 새로 받아 온다
            parts = consumer.partitions_for_topic(DLQ_TOPIC)
        if not parts:
            return []
        tps = [TopicPartition(DLQ_TOPIC, p) for p in sorted(parts)]
        consumer.assign(tps)
        await consumer.seek_to_beginning(*tps)
        ends = await consumer.end_offsets(tps)
        entries = []
        for tp in tps:
            while await consumer.position(tp) < ends[tp]:
                batch = await consumer.getmany(tp, timeout_ms=1000)
                for rec in batch.get(tp, []):
                    entries.append(parse_entry(rec.offset, rec.value))
        return entries
    finally:
        await consumer.stop()


def print_table(entries: list) -> None:
    print(f"== DLQ 토픽 {DLQ_TOPIC} ({BOOTSTRAP}) {len(entries)}건")
    if not entries:
        return
    print(f"{'DLQ offset':>10}  {'reason':<13}  {'requestId':<36}  {'원래 위치':<10}  {'실패 시각':<19}  오류")
    for e in entries:
        src = e.get("source") or {}
        where = f"{src.get('partition', '-')}:{src.get('offset', '-')}"
        when = str(e.get("failedAt", "-"))[:19]
        error = " ".join(str(e.get("error", "-")).split())[:70]
        print(f"{e['dlqOffset']:>10}  {str(e.get('reason', '-')):<13}  {str(e.get('requestId') or '-'):<36}  {where:<10}  {when:<19}  {error}")


async def cmd_list() -> int:
    print_table(await read_all())
    return 0


async def cmd_replay(request_id: str | None, replay_all: bool, provider: str | None, dry_run: bool) -> int:
    if request_id is None and not replay_all:
        print("--request-id <requestId> 또는 --all 중 하나를 지정해야 한다")
        return 2
    entries = await read_all()
    selected, skipped = select_replayable(entries, request_id, replay_all)
    print(f"== 다시 보낼 대상 {len(selected)}건, 건너뜀 {len(skipped)}건 (parse_error 등은 다시 보내지 않는다)")
    for e in selected:
        print(f"  requestId={e['requestId']} (DLQ offset {e['dlqOffset']}, 원인: {' '.join(str(e.get('error')).split())[:60]})")
    if dry_run or not selected:
        print("== --dry-run 이라 보내지 않음" if dry_run else "== 보낼 것이 없음")
        return 0
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()
    try:
        for e in selected:
            meta = await producer.send_and_wait(TOPIC, value=build_replay_value(e, provider), key=e["requestId"].encode("utf-8"))
            print(f"  보냄 requestId={e['requestId']} -> {TOPIC} partition={meta.partition} offset={meta.offset}"
                  + (f" provider={provider}" if provider else ""))
    finally:
        await producer.stop()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="DLQ 확인과 재처리")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="DLQ 에 쌓인 메시지 출력")
    rp = sub.add_parser("replay", help="DLQ 의 처리 실패 메시지를 원래 토픽으로 다시 보냄")
    rp.add_argument("--request-id", default=None, help="이 요청 한 건만")
    rp.add_argument("--all", action="store_true", help="다시 보낼 수 있는 것을 전부")
    rp.add_argument("--provider", default=None, help="다시 보낼 때 provider 를 이 값으로 바꿈")
    rp.add_argument("--dry-run", action="store_true", help="보내지 않고 대상만 출력")
    args = ap.parse_args()
    if args.cmd == "list":
        return asyncio.run(cmd_list())
    return asyncio.run(cmd_replay(args.request_id, args.all, args.provider, args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
