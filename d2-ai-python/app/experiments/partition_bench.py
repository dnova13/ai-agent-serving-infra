"""
Kafka 파티션 수 vs 처리량 측정 (Day 34)

요청 N건(기본 60)을 Spring(8080)에 한꺼번에 보내고, 모두 끝날 때까지 걸린 시간으로 처리량(TPS)을 잰다.
파티션 수(P)와 워커 수(W)는 이 스크립트가 바꾸지 않는다. 사람이 토픽과 워커를 띄운 뒤 이 스크립트를 한 번씩 돌린다.
(가이드 DAY34_1_GUIDE.md 참고. 가짜 모델 provider=fake 를 쓰므로 돈이 들지 않는다.)

하는 일
 1) 워커 /health 를 읽어 토픽, 그룹, 워커별 맡은 파티션을 출력한다. 워커마다 토픽이 다르면 중단한다.
 2) 요청 N건을 동시에 보낸다(동시 연결 --concurrency). 접수(202)된 건수와 접수에 걸린 시간을 잰다.
 3) 결과를 --poll-sec 간격으로 읽어 모두 DONE/FAILED 가 될 때까지 기다린다(결과는 Redis 에 있어 아무 워커에게 물어도 된다).
 4) 첫 요청을 보낸 때부터 마지막 결과가 DONE 이 된 때까지를 "전체 시간"으로, 건수 / 전체 시간을 TPS 로 출력한다.
 5) 결과의 worker, partition 으로 워커별, 파티션별 처리 건수를 센다.

보는 숫자
 - TPS = 성공 건수 / 전체 시간. 워커 한 대는 한 번에 한 건씩 처리하므로 모델 대기 시간이 d 초면 한 대의 TPS 는 약 1/d.
 - 이론 하한 시간 = (가장 많이 맡은 워커의 건수) x (결과 elapsedMs 평균). 파티션 배정이 고르지 않으면 가장 바쁜 워커가 전체 시간을 정한다.
 - 놀고 있는 워커 수 = 처리 건수가 0 인 워커. 워커가 파티션보다 많으면 생긴다.

실행 (프로젝트 루트 또는 아무 곳에서나. 표준 라이브러리만 쓴다)
    python d2-ai-python/app/experiments/partition_bench.py --label p3w3 --workers localhost:8000,localhost:8001,localhost:8002
    python d2-ai-python/app/experiments/partition_bench.py --label p1w1 --workers localhost:8000 --n 30
"""

import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

QUESTIONS = [
    "단순 변심으로 환불하면 배송비는 누가 내요?",
    "환불은 며칠 안에 신청해야 하나요?",
    "교환 시 배송비는 누가 부담하나요?",
    "배송이 늦어지면 어떻게 하나요?",
    "주문을 취소하려면 어떻게 하나요?",
]


def http_json(method: str, url: str, body: dict | None = None, timeout: float = 10.0) -> tuple[int, dict | None]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:  # noqa: BLE001
            return e.code, None
    except Exception:  # noqa: BLE001
        return 0, None


def read_workers(hosts: list[str]) -> list[dict]:
    out = []
    for h in hosts:
        code, j = http_json("GET", f"http://{h}/health", timeout=5)
        if code != 200 or not isinstance(j, dict):
            raise SystemExit(f"FAIL: 워커 {h} 의 /health 를 읽지 못했다 (http={code})")
        # 워커 상태는 /health 의 kafka_consumer 아래에 있다(워커를 껐으면 null)
        kc = j.get("kafka_consumer")
        if not isinstance(kc, dict):
            raise SystemExit(f"FAIL: 워커 {h} 에 Kafka 워커 상태(kafka_consumer)가 없다. KAFKA_CONSUMER_ENABLED 를 확인한다")
        kc["_host"] = h
        out.append(kc)
    return out


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def main() -> int:
    ap = argparse.ArgumentParser(description="파티션 수 vs TPS 측정 (Day 34)")
    ap.add_argument("--label", required=True, help="이 실행의 이름 (예: p3w3)")
    ap.add_argument("--workers", default="localhost:8000", help="워커 주소 목록, 쉼표로")
    ap.add_argument("--spring", default="localhost:8080")
    ap.add_argument("--n", type=int, default=60, help="보낼 요청 수")
    ap.add_argument("--provider", default="fake")
    ap.add_argument("--concurrency", type=int, default=20, help="동시에 보내는 연결 수")
    ap.add_argument("--poll-sec", type=float, default=0.5)
    ap.add_argument("--timeout-sec", type=float, default=600.0, help="전체 완료를 기다리는 최대 시간")
    args = ap.parse_args()

    hosts = [h.strip() for h in args.workers.split(",") if h.strip()]
    print(f"== Day 34 파티션 vs TPS: {args.label}  (요청 {args.n}건, provider={args.provider}, 워커 {len(hosts)}대)")

    # 1) 워커 상태
    ws = read_workers(hosts)
    topics = {w.get("topic") for w in ws}
    groups = {w.get("group") for w in ws}
    if len(topics) != 1 or len(groups) != 1:
        print(f"FAIL: 워커마다 토픽 또는 그룹이 다르다. topic={topics} group={groups}")
        return 1
    all_parts = sorted(p for w in ws for p in (w.get("partitions") or []))
    print(f"   토픽 {topics.pop()}, 그룹 {groups.pop()}")
    for w in ws:
        print(f"   {w.get('worker_id')} ({w['_host']}): state={w.get('state')} 맡은 파티션 {w.get('partitions')}")
    p_count = len(all_parts)
    w_active = sum(1 for w in ws if w.get("partitions"))
    print(f"   파티션 {p_count}개(배정된 것 기준), 일하는 워커 {w_active}/{len(ws)}대")
    if p_count == 0:
        print("FAIL: 배정된 파티션이 없다. 워커가 그룹에 들어가 파티션을 받을 때까지(보통 몇 초) 기다린 뒤 다시 실행한다")
        return 1
    if len(set(all_parts)) != p_count:
        print(f"FAIL: 같은 파티션이 두 워커에 배정돼 있다 {all_parts}. 이전 워커가 남아 있거나 리밸런싱 중일 수 있다")
        return 1
    busy = [w.get("worker_id") for w in ws if w.get("current_request_id")]
    if busy:
        print(f"FAIL: 이미 처리 중인 워커가 있다 {busy}. 이전 실험의 요청이 남아 있다")
        return 1
    base_received = {w.get("worker_id"): (w.get("received", 0) + w.get("failed", 0)) for w in ws}

    # 2) 전송
    qs = [QUESTIONS[i % len(QUESTIONS)] for i in range(args.n)]
    sent: dict[int, dict] = {}

    def send(i: int) -> None:
        t = time.monotonic()
        code, j = http_json("POST", f"http://{args.spring}/api/agent/requests",
                            {"question": qs[i], "provider": args.provider}, timeout=15)
        sent[i] = {"code": code, "rid": (j or {}).get("requestId"), "ms": (time.monotonic() - t) * 1000}

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        list(ex.map(send, range(args.n)))
    accept_wall = time.monotonic() - t0
    ids = [s["rid"] for s in sent.values() if s["code"] == 202 and s["rid"]]
    rejected = args.n - len(ids)
    ams = sorted(s["ms"] for s in sent.values() if s["code"] == 202)
    print(f"   접수 {len(ids)}/{args.n} (거절 {rejected}), 접수에 걸린 시간 {accept_wall:.2f}초, 접수 응답 p50 {percentile(ams, 50):.0f}ms, 최대 {(ams[-1] if ams else 0):.0f}ms")
    if not ids:
        print("FAIL: 접수된 요청이 없다")
        return 1

    # 3) 완료 대기
    results: dict[str, dict] = {}
    done_at: dict[str, float] = {}
    pending = set(ids)
    last_print = 0.0
    while pending and time.monotonic() - t0 < args.timeout_sec:
        for rid in list(pending):
            code, j = http_json("GET", f"http://{hosts[0]}/api/v1/agent/results/{rid}", timeout=5)
            if code == 200 and isinstance(j, dict) and j.get("status") in ("DONE", "FAILED"):
                results[rid] = j
                done_at[rid] = time.monotonic()
                pending.discard(rid)
        now = time.monotonic() - t0
        if now - last_print >= 5:
            print(f"   {now:5.1f}초: 완료 {len(results)}/{len(ids)}")
            last_print = now
        if pending:
            time.sleep(args.poll_sec)
    timed_out = len(pending)

    # 4) 집계
    ok = [r for r in results.values() if r.get("status") == "DONE"]
    failed = [r for r in results.values() if r.get("status") == "FAILED"]
    wall = (max(done_at.values()) - t0) if done_at else 0.0
    tps = len(ok) / wall if wall else 0.0
    print()
    print(f"-- 결과: 성공 {len(ok)}, 실패 {len(failed)}, 시간 초과로 못 기다림 {timed_out}")
    print(f"   전체 시간(첫 전송 ~ 마지막 완료): {wall:.2f}초   TPS: {tps:.2f}건/초")

    el = sorted(r.get("elapsedMs", 0) / 1000 for r in ok if r.get("elapsedMs") is not None)
    mean_el = (sum(el) / len(el)) if el else 0.0
    if el:
        print(f"   건당 처리 시간(elapsedMs): 평균 {mean_el:.2f}초, p95 {percentile(el, 95):.2f}초, 최대 {el[-1]:.2f}초")

    by_worker: dict[str, int] = {}
    by_part: dict[int, int] = {}
    for r in results.values():
        by_worker[str(r.get("worker"))] = by_worker.get(str(r.get("worker")), 0) + 1
        if r.get("partition") is not None:
            by_part[r["partition"]] = by_part.get(r["partition"], 0) + 1
    print("   파티션별 건수: " + ", ".join(f"{p}={by_part[p]}" for p in sorted(by_part)))
    wnames = [w.get("worker_id") for w in ws]
    print("   워커별 건수: " + ", ".join(f"{n}={by_worker.get(n, 0)}" for n in wnames))
    idle = [n for n in wnames if by_worker.get(n, 0) == 0]
    print(f"   놀고 있던 워커: {len(idle)}대 {idle if idle else ''}")
    top = max(by_worker.values()) if by_worker else 0
    floor_sec = top * mean_el
    print(f"   가장 많이 맡은 워커의 건수 {top}  이론 하한 시간 {floor_sec:.2f}초 (= {top} x 평균 {mean_el:.2f}초)")
    ideal_even = math.ceil(len(ok) / max(1, min(p_count, len(ws)))) * mean_el
    print(f"   (참고) 고르게 나뉘었다면 {ideal_even:.2f}초 (= ceil({len(ok)}/{min(p_count, len(ws))}) x 평균 {mean_el:.2f}초)")
    if wall and floor_sec:
        print(f"   전체 시간 / 이론 하한 = {wall / floor_sec:.2f}배 (1.0 에 가까울수록 처리 외의 지연이 작다)")

    # 워커 /health 와 대조
    ws2 = read_workers(hosts)
    delta = {w.get("worker_id"): (w.get("received", 0) + w.get("failed", 0)) - base_received.get(w.get("worker_id"), 0) for w in ws2}
    mism = [n for n in wnames if delta.get(n, 0) != by_worker.get(n, 0)]
    print(f"   /health 의 처리 건수 증가분: {delta}  {'(결과의 worker 집계와 같음)' if not mism else '(다름: ' + str(mism) + ')'}")

    line = (f"SUMMARY\t{args.label}\tP={p_count}\tW={len(ws)}\tN={args.n}\tok={len(ok)}\tfail={len(failed)}\t"
            f"wall={wall:.2f}\ttps={tps:.2f}\ttop_worker={top}\tidle={len(idle)}")
    print()
    print(line)
    problems = []
    if rejected:
        problems.append(f"접수 거절 {rejected}건")
    if failed or timed_out:
        problems.append(f"실패 {len(failed)}, 못 기다림 {timed_out}")
    if mism:
        problems.append("결과 집계와 /health 증가분이 다름")
    print("== 결과: " + ("PASS (측정이 끝까지 정상으로 끝남. 처리량이 기대대로인지는 표로 판단)" if not problems else "FAIL (" + "; ".join(problems) + ")"))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
