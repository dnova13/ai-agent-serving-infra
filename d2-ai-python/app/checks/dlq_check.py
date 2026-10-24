"""
DLQ 동작 확인 (Day 32)

워커의 메시지 처리(_handle)가 실패한 메시지를 DLQ 토픽으로 보내는지, 가짜 Kafka producer 로 확인하고 PASS/FAIL 을 출력한다.
Kafka, Redis, Qdrant, 모델 API 가 모두 필요 없고 대기 시간도 없다. DLQ 도구(dlq_tool.py)의 순수 함수 부분도 함께 확인한다.
실제 Kafka 를 거치는 확인은 가이드 5번의 ①~③ 이다.

확인 항목
  A 성공한 메시지는 DLQ 로 가지 않음
  B 처리 실패(handler 가 예외) → DLQ 에 원본, 오류, 요청 번호, 원래 위치가 담겨 보관됨
  C 해석 불가(JSON 깨짐) → DLQ 에 parse_error 로 보관됨
  D 깨진 바이트(UTF-8 아님)도 보관됨(대체 문자로 바뀜)
  E DLQ 전송이 실패하면 예외가 올라감(그래야 commit 되지 않고 재접속해 다시 받는다)
  F DLQ 를 끄면(KAFKA_DLQ_ENABLED=false 와 같은 설정) 예전처럼 건너뛰기만 함
  G 서버 종료(CancelledError)는 DLQ 로 보내지 않고 그대로 올라감
  H DLQ 가 켜져 있는데 producer 가 없으면 예외가 올라감
  I /health 용 snapshot 에 DLQ 항목이 있음
  J dlq_tool 의 순수 함수(해석, 재처리 대상 고르기, 재처리 메시지 만들기)
  K 지표 합계

실행 (d2-ai-python 폴더에서, 파이썬 가상환경 켠 터미널):
    python -m app.checks.dlq_check
"""

import asyncio
import json
import logging
import sys
from types import SimpleNamespace

from prometheus_client import REGISTRY

from app.checks import dlq_tool
from app.services import kafka_worker
from app.services.kafka_worker import KafkaWorker

GOOD = json.dumps({"requestId": "req-1", "question": "환불 정책", "provider": "gemini",
                   "createdAt": "2026-10-17T14:41:16.949855Z"}, ensure_ascii=False)


class FakeProducer:
    """AIOKafkaProducer 흉내. 보낸 것을 모아 두고, fail 이면 전송에서 예외를 낸다."""

    def __init__(self, fail: bool = False) -> None:
        self.sent = []
        self.fail = fail

    async def send_and_wait(self, topic, value=None, key=None):
        if self.fail:
            raise RuntimeError("브로커 응답 없음(가짜)")
        self.sent.append({"topic": topic, "key": key, "value": value})


def record(value: bytes, key=b"req-1", partition=1, offset=42):
    return SimpleNamespace(topic="agent.requests", partition=partition, offset=offset, key=key, value=value, timestamp=0)


async def ok_handler(message, rec):
    return None


async def boom_handler(message, rec):
    raise ValueError("지원하지 않는 provider입니다: no-such")


async def cancel_handler(message, rec):
    raise asyncio.CancelledError()


def metric(reason: str) -> float:
    return REGISTRY.get_sample_value("agent_worker_dlq_total", {"reason": reason}) or 0.0


def run_handle(handler, rec, producer):
    w = KafkaWorker(handler=handler)
    w._producer = producer
    try:
        asyncio.run(w._handle(rec))
        return w, None
    except BaseException as e:  # CancelledError 도 잡아서 확인한다
        return w, e


def main() -> int:
    logging.getLogger("kafka_worker").setLevel(logging.CRITICAL)  # 일부러 낸 실패의 오류 로그(스택 추적)를 숨긴다
    fails = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal fails
        fails += 0 if ok else 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    print("== 워커의 DLQ 경로 (가짜 producer)")

    # A
    prod = FakeProducer()
    w, err = run_handle(ok_handler, record(GOOD.encode()), prod)
    check("A 성공: DLQ 로 안 감", err is None and not prod.sent and w.received == 1 and w.failed == 0 and w.dlq_sent == 0)

    # B
    prod = FakeProducer()
    w, err = run_handle(boom_handler, record(GOOD.encode()), prod)
    env = json.loads(prod.sent[0]["value"].decode("utf-8")) if prod.sent else {}
    check("B 처리 실패: DLQ 토픽으로 1건 보냄", err is None and len(prod.sent) == 1 and prod.sent[0]["topic"] == "agent.requests.dlq", f"토픽 {prod.sent[0]['topic'] if prod.sent else None}")
    check("B 키는 원래 메시지 키", bool(prod.sent) and prod.sent[0]["key"] == b"req-1")
    check("B reason, requestId", env.get("reason") == "handler_error" and env.get("requestId") == "req-1")
    check("B 오류 내용", str(env.get("error", "")).startswith("ValueError: 지원하지 않는 provider입니다"))
    check("B 원래 위치", env.get("source") == {"topic": "agent.requests", "partition": 1, "offset": 42})
    check("B 원본 문자열 그대로", env.get("originalValue") == GOOD)
    check("B 워커 이름과 실패 시각", env.get("worker") == kafka_worker.WORKER_ID and "T" in str(env.get("failedAt")))
    check("B 카운터(failed 1, dlq_sent 1)", w.failed == 1 and w.dlq_sent == 1)

    # C
    prod = FakeProducer()
    w, err = run_handle(ok_handler, record(b"not-json", key=None), prod)
    env = json.loads(prod.sent[0]["value"].decode("utf-8")) if prod.sent else {}
    check("C 해석 불가: parse_error 로 보관", err is None and env.get("reason") == "parse_error" and env.get("requestId") is None and env.get("originalValue") == "not-json")
    check("C skipped 1, dlq_sent 1", w.skipped == 1 and w.dlq_sent == 1)

    # D
    prod = FakeProducer()
    w, err = run_handle(ok_handler, record(b"\xff\xfe{", key=None), prod)
    env = json.loads(prod.sent[0]["value"].decode("utf-8")) if prod.sent else {}
    check("D 깨진 바이트도 보관됨(대체 문자)", err is None and env.get("reason") == "parse_error" and "�" in str(env.get("originalValue")))

    # E
    prod = FakeProducer(fail=True)
    before = metric("handler_error")
    w, err = run_handle(boom_handler, record(GOOD.encode()), prod)
    check("E DLQ 전송 실패 → 예외가 올라감", isinstance(err, RuntimeError) and w.dlq_sent == 0 and metric("handler_error") == before, f"예외 {type(err).__name__}")

    # F
    kafka_worker.DLQ_ENABLED = False
    try:
        prod = FakeProducer()
        w, err = run_handle(boom_handler, record(GOOD.encode()), prod)
        check("F DLQ 끔: 보내지 않고 건너뜀", err is None and not prod.sent and w.failed == 1 and w.dlq_sent == 0)
    finally:
        kafka_worker.DLQ_ENABLED = True

    # G
    prod = FakeProducer()
    w, err = run_handle(cancel_handler, record(GOOD.encode()), prod)
    check("G 서버 종료(CancelledError)는 그대로 올라가고 DLQ 로 안 감", isinstance(err, asyncio.CancelledError) and not prod.sent)

    # H
    w, err = run_handle(boom_handler, record(GOOD.encode()), None)
    check("H producer 없음 → 예외가 올라감", isinstance(err, RuntimeError) and "producer" in str(err))

    # I
    snap = KafkaWorker(handler=ok_handler).snapshot()
    check("I snapshot 에 dlq_topic, dlq_sent", snap.get("dlq_topic") == "agent.requests.dlq" and snap.get("dlq_sent") == 0)

    print("== dlq_tool 순수 함수")
    good_env = {"reason": "handler_error", "requestId": "req-1", "originalValue": GOOD, "source": {"partition": 1, "offset": 42}}
    e1 = dlq_tool.parse_entry(0, json.dumps(good_env).encode())
    e_bad = dlq_tool.parse_entry(1, b"\xff garbage")
    check("J parse_entry: 정상은 dlqOffset 이 붙음", e1["dlqOffset"] == 0 and e1["requestId"] == "req-1")
    check("J parse_entry: 깨진 DLQ 메시지는 reason unknown", e_bad["reason"] == "unknown" and e_bad["dlqOffset"] == 1)
    entries = [
        dlq_tool.parse_entry(0, json.dumps({**good_env, "requestId": "req-1"}).encode()),
        dlq_tool.parse_entry(1, json.dumps({**good_env, "requestId": "req-2"}).encode()),
        dlq_tool.parse_entry(2, json.dumps({"reason": "parse_error", "requestId": None, "originalValue": "not-json"}).encode()),
    ]
    sel, skip = dlq_tool.select_replayable(entries, request_id="req-2")
    check("J select: --request-id 는 그 요청만", [e["requestId"] for e in sel] == ["req-2"] and skip == [])
    sel, skip = dlq_tool.select_replayable(entries, replay_all=True)
    check("J select: --all 은 handler_error 2건, parse_error 는 건너뜀", len(sel) == 2 and len(skip) == 1)
    sel, skip = dlq_tool.select_replayable(entries)
    check("J select: 지정이 없으면 아무것도 안 고름", sel == [] and skip == [])
    same = json.loads(dlq_tool.build_replay_value(entries[0]))
    changed = json.loads(dlq_tool.build_replay_value(entries[0], provider="fake-agent"))
    check("J replay 메시지: 그대로는 원본과 같고, --provider 는 그 필드만 바꿈",
          same == json.loads(GOOD) and changed["provider"] == "fake-agent" and changed["requestId"] == "req-1" and changed["question"] == "환불 정책")

    print("== 지표 합계")
    check("K agent_worker_dlq_total handler_error 1, parse_error 2", metric("handler_error") == 1.0 and metric("parse_error") == 2.0,
          f"실제 {metric('handler_error'):g}, {metric('parse_error'):g}")

    print("== 요약")
    print(f"  {'모든 항목 PASS' if fails == 0 else f'FAIL {fails}개'}")
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
