"""
전체 시스템 통합 재검증 (Day 35)
(코드 흐름은 docs/DAY35_CODE_GUIDE.md, 실행 방법과 확인 항목은 docs/DAY35_GUIDE.md 참고)

Day 1~34 에 만든 점검을 한 번에 다시 돌리고, 실제로 켜 둔 구성 요소(Spring, Kafka, 워커, Redis, Prometheus, Grafana)를
가짜 모델(provider=fake)로 한 줄로 이어 확인한다. 항목마다 PASS / FAIL / SKIP 을 찍고 마지막에 요약한다.
1, 2단계는 가짜 모델만 쓰므로 돈이 들지 않는다. 3단계는 실제 Claude 와 검색 도구의 Gemini 임베딩을 소량(기본 3건 + 동기 1건) 부른다.

1단계 "기존 점검" (Redis 만 있으면 됨)
  redis_check, result_check, ttl_check, lock_check, selfcorrect_check, dlq_check 를 각각 따로 실행하고 종료 코드(0 이면 PASS)로 판정한다.
  lock_stress 는 baseline, timeout 만 N=50 으로 돌린다(N=100 은 Redis 연결 풀 한도(100)와 같아 Day 33 에서 본 경계에 걸릴 수 있다).
2단계 "실제 구성 요소" (평소 설정으로 켜져 있어야 함: Spring 인자 없이, 워커는 agent.requests / agent-workers, FAKE_LLM_ENABLED=true)
  워커 상태, Spring 상태와 지표, 가짜 모델 요청 end-to-end, 같은 세션 대화 이어가기, 입력 검증, Prometheus 대상, Grafana, 컨슈머 그룹 lag.

3단계 "실제 모델(Claude)" (2단계와 같은 설정이면 된다. 워커에 FAKE_LLM_ENABLED=true 가 켜져 있어도 provider=claude 는 실제 모델로 간다. ANTHROPIC_API_KEY, GOOGLE_API_KEY 필요)
  Spring → Kafka → 워커 → Claude → Redis 비동기 경로로 질문 n건, Spring 동기 경로(/api/agent/sync) 1건이 실제 답을 내는지 본다.

실행 (d2-ai-python 에서, 파이썬 가상환경)
    python -m app.checks.system_verify                      # 1, 2, 3단계 모두 (3단계는 Claude 를 실제로 부른다)
    python -m app.checks.system_verify --only offline       # 1단계만
    python -m app.checks.system_verify --only live          # 2단계만
    python -m app.checks.system_verify --only real          # 3단계만
    python -m app.checks.system_verify --only real --real-n 5   # 3단계, 비동기 질문 5건
    python -m app.checks.system_verify --workers localhost:8000,localhost:8001
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

# (이름, python 인자, 실행 위치). 실행 위치가 None 이면 지금 위치(d2-ai-python)에서 실행한다
CHECKS = [
    ("redis_check", ["-m", "app.checks.redis_check"], None),
    ("result_check", ["-m", "app.checks.result_check"], None),
    ("ttl_check", ["-m", "app.checks.ttl_check"], None),
    ("lock_check", ["-m", "app.checks.lock_check"], None),
    ("selfcorrect_check", ["-m", "app.checks.selfcorrect_check"], None),
    ("dlq_check", ["-m", "app.checks.dlq_check"], None),
    ("lock_stress (baseline, timeout, N=50)",
     ["-m", "app.experiments.lock_stress", "--scenarios", "baseline,timeout", "--n", "50"], None),
]

# 3단계(실제 모델)에 보낼 질문 (k6 ramping.js 와 같은 환불, 배송 FAQ)
REAL_QUESTIONS = [
    "단순 변심으로 환불하면 배송비는 누가 내요?",
    "환불은 며칠 안에 신청해야 하나요?",
    "교환 시 배송비는 누가 부담하나요?",
    "배송이 늦어지면 어떻게 하나요?",
    "주문을 취소하려면 어떻게 하나요?",
]

rows: list[tuple[str, str, str]] = []  # (이름, PASS/FAIL/SKIP, 설명)


def rec(name: str, status: str, detail: str = "") -> bool:
    rows.append((name, status, detail))
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail else ""))
    return status == "PASS"


def http(method: str, url: str, body: dict | None = None, timeout: float = 10.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            try:
                return r.status, json.loads(raw)
            except Exception:  # noqa: BLE001
                return r.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:  # noqa: BLE001
            return e.code, raw
    except Exception:  # noqa: BLE001
        return 0, None


def run_offline() -> None:
    print("\n== 1단계: 기존 점검을 다시 실행")
    for name, argv, cwd in CHECKS:
        t0 = time.monotonic()
        try:
            p = subprocess.run([sys.executable] + argv, capture_output=True, text=True, timeout=600, cwd=cwd)
        except subprocess.TimeoutExpired:
            rec(name, "FAIL", "600초 안에 끝나지 않음")
            continue
        sec = time.monotonic() - t0
        tail = [ln for ln in (p.stdout or "").strip().splitlines() if ln.strip()][-1:] or [""]
        if p.returncode == 0:
            rec(name, "PASS", f"{sec:.1f}초")
        elif p.returncode == 2:
            rec(name, "SKIP", f"종료 코드 2: {tail[0][:80]}")
        else:
            err = [ln for ln in (p.stderr or "").strip().splitlines() if ln.strip()][-1:] or [""]
            rec(name, "FAIL", f"종료 코드 {p.returncode}: {(err[0] or tail[0])[:100]}")


def poll_result(worker: str, rid: str, timeout: float = 60.0):
    t0 = time.monotonic()
    last = None
    while time.monotonic() - t0 < timeout:
        code, j = http("GET", f"http://{worker}/api/v1/agent/results/{rid}", timeout=5)
        if code == 200 and isinstance(j, dict):
            last = j
            if j.get("status") in ("DONE", "FAILED"):
                return j, time.monotonic() - t0
        time.sleep(0.5)
    return last, time.monotonic() - t0


def run_live(args) -> None:
    print("\n== 2단계: 실제 구성 요소 (평소 설정으로 켜져 있어야 함)")
    workers = [w.strip() for w in args.workers.split(",") if w.strip()]
    w0 = workers[0]

    # 2-1 워커 상태
    all_ok = True
    for w in workers:
        code, j = http("GET", f"http://{w}/health", timeout=5)
        if code != 200 or not isinstance(j, dict):
            rec(f"워커 {w} /health", "FAIL", f"http={code}. 워커가 안 떠 있다")
            all_ok = False
            continue
        kc = j.get("kafka_consumer") or {}
        probs = []
        if j.get("status") != "ok":
            probs.append("status != ok")
        if kc.get("state") != "running":
            probs.append(f"consumer state={kc.get('state')}")
        if kc.get("topic") != "agent.requests":
            probs.append(f"topic={kc.get('topic')} (agent.requests 아님. Day 34 실험 설정이 남았을 수 있다)")
        if kc.get("group") != "agent-workers":
            probs.append(f"group={kc.get('group')} (agent-workers 아님)")
        if not kc.get("partitions"):
            probs.append("맡은 파티션 없음")
        rd = j.get("redis")
        if not (isinstance(rd, dict) and rd.get("connected") is True):
            probs.append(f"redis={rd}")
        all_ok &= rec(f"워커 {w} /health", "FAIL" if probs else "PASS",
                      "; ".join(probs) if probs else f"{kc.get('worker_id')} 파티션 {kc.get('partitions')}")
    if not all_ok:
        print("  (워커 상태가 정상이 아니라 end-to-end 항목은 건너뜁니다)")
        for n in ("Spring 상태", "end-to-end 가짜 모델 요청", "같은 세션 대화 이어가기", "입력 검증(question 비어 있음)"):
            rec(n, "SKIP", "워커 상태 이상")
        return

    # 2-2 Spring 상태, 지표
    code, j = http("GET", f"http://{args.spring}/actuator/health", timeout=5)
    ok = code == 200 and isinstance(j, dict) and j.get("status") == "UP"
    rec("Spring /actuator/health", "PASS" if ok else "FAIL", f"http={code} {j if not ok else ''}".strip())
    code, txt = http("GET", f"http://{args.spring}/actuator/prometheus", timeout=5)
    rec("Spring /actuator/prometheus", "PASS" if code == 200 and isinstance(txt, str) and "http_server_requests" in txt else "FAIL", f"http={code}")
    code, txt = http("GET", f"http://{w0}/metrics", timeout=5)
    rec("워커 /metrics", "PASS" if code == 200 and isinstance(txt, str) and "agent_worker" in txt else "FAIL", f"http={code}")

    # 2-3 end-to-end
    sid = f"day35verify-{uuid.uuid4().hex[:8]}"
    t0 = time.monotonic()
    code, j = http("POST", f"http://{args.spring}/api/agent/requests",
                   {"question": "환불은 며칠 안에 신청해야 하나요?", "provider": "fake", "sessionId": sid})
    rid = (j or {}).get("requestId") if isinstance(j, dict) else None
    if code != 202 or not rid:
        rec("end-to-end 가짜 모델 요청", "FAIL", f"접수 http={code} {j}")
        for n in ("같은 세션 대화 이어가기",):
            rec(n, "SKIP", "첫 요청 실패")
    else:
        res, sec = poll_result(w0, rid)
        good = isinstance(res, dict) and res.get("status") == "DONE" and "[fake]" in str(res.get("answer"))
        missing = [k for k in ("worker", "partition", "offset") if isinstance(res, dict) and res.get(k) is None]
        rec("end-to-end 가짜 모델 요청",
            "PASS" if good and not missing else "FAIL",
            f"{sec:.1f}초, worker={res.get('worker') if res else None}, partition={res.get('partition') if res else None}"
            + (f", 빠진 필드 {missing}" if missing else "") + ("" if good else f", 결과={res}"))
        # 2-4 같은 세션 두 번째 요청
        code2, j2 = http("POST", f"http://{args.spring}/api/agent/requests",
                         {"question": "교환 시 배송비는 누가 부담하나요?", "provider": "fake", "sessionId": sid})
        rid2 = (j2 or {}).get("requestId") if isinstance(j2, dict) else None
        res2, _ = poll_result(w0, rid2) if rid2 else (None, 0)
        c3, s = http("GET", f"http://{w0}/api/v1/agent/sessions/{sid}", timeout=5)
        turns_ok = c3 == 200 and isinstance(s, dict) and s.get("turns") == 2
        done2 = isinstance(res2, dict) and res2.get("status") == "DONE"
        rec("같은 세션 대화 이어가기", "PASS" if done2 and turns_ok else "FAIL",
            f"두 번째 요청 {res2.get('status') if res2 else None}, 저장된 턴 {s.get('turns') if isinstance(s, dict) else s} (기대 2)")
        c4, d = http("DELETE", f"http://{w0}/api/v1/agent/sessions/{sid}", timeout=5)
        c5, _ = http("GET", f"http://{w0}/api/v1/agent/sessions/{sid}", timeout=5)
        rec("세션 삭제 뒤 조회가 404", "PASS" if c4 == 200 and c5 == 404 else "FAIL", f"DELETE http={c4}, GET http={c5}")

    # 2-5 입력 검증
    code, j = http("POST", f"http://{args.spring}/api/agent/requests", {"question": "   ", "provider": "fake"})
    rec("입력 검증(question 비어 있음)", "PASS" if code == 400 else "FAIL", f"http={code} (기대 400)")

    # 2-6 Prometheus 대상, Grafana
    code, j = http("GET", f"http://{args.prometheus}/api/v1/targets", timeout=5)
    if code != 200 or not isinstance(j, dict):
        rec("Prometheus 대상", "SKIP", f"http={code}. Prometheus(9090)가 안 떠 있다")
    else:
        act = j.get("data", {}).get("activeTargets", [])
        down = [f"{t['labels'].get('job')}@{t['labels'].get('instance')}" for t in act if t.get("health") != "up"]
        need = {"spring", "kafka", "redis"}
        up_jobs = {t["labels"].get("job") for t in act if t.get("health") == "up"}
        miss = sorted(need - up_jobs)
        rec("Prometheus 대상 (spring, kafka, redis 가 up)", "PASS" if not miss else "FAIL",
            f"up 이 아닌 대상: {down}" if down else "모두 up")
    code, j = http("GET", f"http://{args.grafana}/api/health", timeout=5)
    if code == 0:
        rec("Grafana /api/health", "SKIP", "Grafana(3000)가 안 떠 있다")
    else:
        rec("Grafana /api/health", "PASS" if code == 200 else "FAIL", f"http={code}")

    # 2-7 컨슈머 그룹 lag
    try:
        p = subprocess.run(["docker", "exec", "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:29092",
                            "--group", "agent-workers", "--describe"], capture_output=True, text=True, timeout=30)
        lag = 0
        seen = False
        for ln in p.stdout.splitlines():
            cols = ln.split()
            if len(cols) >= 6 and cols[0] == "agent-workers" and cols[5].lstrip("-").isdigit():
                seen = True
                lag += int(cols[5])
        if p.returncode != 0 or not seen:
            rec("컨슈머 그룹 agent-workers lag", "SKIP", "docker 로 읽지 못했다")
        else:
            rec("컨슈머 그룹 agent-workers lag", "PASS" if lag == 0 else "FAIL", f"lag 합계 {lag} (기대 0)")
    except Exception as e:  # noqa: BLE001
        rec("컨슈머 그룹 agent-workers lag", "SKIP", f"docker 를 쓸 수 없다: {type(e).__name__}")


def run_real(args) -> None:
    print("\n== 3단계: 실제 모델(Claude). Claude 와 검색 도구의 Gemini 임베딩을 실제로 부른다(소량)")
    workers = [w.strip() for w in args.workers.split(",") if w.strip()]
    w0 = workers[0]
    code, _ = http("GET", f"http://{w0}/health", timeout=5)
    if code != 200:
        for n in ("실제 모델 end-to-end (비동기)", "실제 모델 동기 경로 /api/agent/sync"):
            rec(n, "SKIP", f"워커 {w0} /health http={code}")
        return

    # 3-1 비동기 경로: Spring → Kafka → 워커 → Claude → Redis
    n = max(1, args.real_n)
    sent = []  # (requestId, sessionId, 접수 시각)
    bad = []
    for i in range(n):
        sid = f"day35real-{uuid.uuid4().hex[:8]}"
        code, j = http("POST", f"http://{args.spring}/api/agent/requests",
                       {"question": REAL_QUESTIONS[i % len(REAL_QUESTIONS)], "provider": "claude", "sessionId": sid})
        rid = j.get("requestId") if isinstance(j, dict) else None
        if code == 202 and rid:
            sent.append((rid, sid, time.monotonic()))
        else:
            bad.append(f"{i + 1}번 접수 http={code}")
    secs = []
    elapsed = []
    for rid, sid, t_sent in sent:
        res, _ = poll_result(w0, rid, timeout=120.0)
        secs.append(time.monotonic() - t_sent)
        ans = res.get("answer") if isinstance(res, dict) else None
        if not isinstance(res, dict) or res.get("status") != "DONE":
            st = res.get("status") if isinstance(res, dict) else None
            err = (res.get("error") or "") if isinstance(res, dict) else ""
            bad.append(f"{rid[:8]} status={st} {str(err)[:80]}".strip())
        elif not isinstance(ans, str) or not ans.strip():
            bad.append(f"{rid[:8]} 답이 비어 있다")
        elif "[fake]" in ans:
            bad.append(f"{rid[:8]} 가짜 모델 답이 왔다(provider=claude 가 가짜로 처리됨)")
        elif any(res.get(k) is None for k in ("worker", "partition", "offset")):
            bad.append(f"{rid[:8]} worker/partition/offset 이 빠짐")
        else:
            if isinstance(res.get("elapsedMs"), (int, float)):
                elapsed.append(res["elapsedMs"] / 1000)
        http("DELETE", f"http://{w0}/api/v1/agent/sessions/{sid}", timeout=5)
    detail = f"{n - len(bad)}/{n}건 DONE"
    if secs:
        detail += f", 접수부터 완료까지 평균 {sum(secs) / len(secs):.1f}초, 최대 {max(secs):.1f}초"
    if elapsed:
        detail += f", 워커 처리(elapsedMs) 평균 {sum(elapsed) / len(elapsed):.2f}초"
    if bad:
        detail += "; " + "; ".join(bad)
    rec("실제 모델 end-to-end (비동기)", "PASS" if not bad else "FAIL", detail)

    # 3-2 동기 경로: Spring → FastAPI(/run) → Claude
    t0 = time.monotonic()
    code, j = http("POST", f"http://{args.spring}/api/agent/sync",
                   {"question": REAL_QUESTIONS[0], "provider": "claude"}, timeout=60)
    sec = time.monotonic() - t0
    ans = j.get("answer") if isinstance(j, dict) else None
    ok = code == 200 and isinstance(ans, str) and ans.strip() != "" and "[fake]" not in ans
    rec("실제 모델 동기 경로 /api/agent/sync", "PASS" if ok else "FAIL",
        f"{sec:.1f}초, http={code}" + ("" if ok else f", 본문={str(j)[:100]}"))


def main() -> int:
    ap = argparse.ArgumentParser(description="전체 시스템 통합 재검증 (Day 35)")
    ap.add_argument("--only", choices=["offline", "live", "real"], help="한 단계만 실행 (offline=1단계, live=2단계, real=3단계)")
    ap.add_argument("--real-n", type=int, default=3, help="3단계에서 보낼 비동기 질문 수 (기본 3)")
    ap.add_argument("--workers", default="localhost:8000", help="워커 주소 목록, 쉼표로")
    ap.add_argument("--spring", default="localhost:8080")
    ap.add_argument("--prometheus", default="localhost:9090")
    ap.add_argument("--grafana", default="localhost:3000")
    args = ap.parse_args()

    print("== Day 35 전체 시스템 통합 재검증 (가짜 모델 + 실제 모델)")
    if args.only in (None, "offline"):
        run_offline()
    if args.only in (None, "live"):
        run_live(args)
    if args.only in (None, "real"):
        run_real(args)

    n = {s: sum(1 for r in rows if r[1] == s) for s in ("PASS", "FAIL", "SKIP")}
    print(f"\n== 요약: PASS {n['PASS']}, FAIL {n['FAIL']}, SKIP {n['SKIP']}")
    for name, st, detail in rows:
        if st != "PASS":
            print(f"  {st}: {name}  {detail}")
    print("== 결과: " + ("PASS" if n["FAIL"] == 0 else "FAIL"))
    return 0 if n["FAIL"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
