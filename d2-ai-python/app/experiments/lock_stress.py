"""
분산 락 스트레스 테스트 (Day 33)

같은 sessionId 로 요청 N 개(기본 100)가 한꺼번에 올 때 Redis 락이 어떻게 동작하는지 본다.
Day 22(race_test.py)는 N 이 2~10 이었다. 이 스크립트는 N 을 100 까지 키우고, 락이 깨지는 조건(기다림 한도, 락 만료)도 일부러 만든다.
이 프로세스 안에서 스레드로 run_agent 를 동시에 부른다. 모델은 가짜(대기 시간만 흉내)이고 Redis 는 진짜다. 돈이 들지 않는다.

시나리오
 - baseline: 락 있음, 기본 설정(기다림 한도 100초). 100개가 모두 끝나는지, 한 번에 하나씩만 실행되는지, 순서와 대화 저장이 맞는지 본다.
 - nolock:   락 없음. 같은 요청이 얼마나 겹쳐 실행되고 앞 대화를 놓치는지 본다(baseline 과 비교하는 정보용).
 - timeout:  락 있음, 기다림 한도를 짧게(기본 2초). 한도를 넘긴 요청은 LockTimeout(서버에서는 HTTP 409)이 된다.
             성공한 요청과 시간 초과한 요청의 합이 N 인지, 시간 초과한 요청이 모델을 부르지 않았는지, 락 키가 남지 않는지 본다.
 - ttl:      락 만료 시간(기본 0.3초)을 일 하는 시간(기본 0.5초)보다 짧게. 락이 먼저 풀려 겹쳐 실행되는 한계를 눈으로 본다(정보용).
 - sweep:    baseline 을 N=10, 50, 100, 200 으로 반복해 N 에 따라 대기 시간이 어떻게 늘어나는지 표로 본다(정보용).
 - pool:     N=200 을 Redis 연결 풀 기본 설정(redis-py 8.1.0 은 최대 100개)으로 한 번, 풀을 넉넉히(기본 300) 늘려 한 번 돌린다.
             연결이 모자라면 락을 못 잡는 오류가 나고, 락 코드가 fail_open=True 라 락 없이 계속 실행해 상호 배제가 깨지는지 본다.

보는 숫자
 - 동시 실행 최대: 모델 호출 구간에 동시에 들어간 요청 수의 최댓값. 락이 제대로 동작하면 1.
 - 앞 대화: 요청이 모델에 넘긴 "이전 대화 메시지 수". 차례로 처리되면 0, 2, 4, ... 이고 대화는 최근 MAX_TURNS 턴만 보관하므로 2*MAX_TURNS 에서 멈춘다.
 - 대기 시간: 요청을 보낸 뒤 락을 잡아 모델 호출에 들어가기까지의 시간.
 - 락 시도: SET NX 시도 횟수. 못 잡으면 0.1초(0.5~1.5배)마다 다시 시도하는 폴링 방식이라 기다리는 요청이 많을수록 Redis 에 부담이 간다.
 - 락 우회: Redis 오류로 락을 못 잡았는데도 락 없이 실행된 횟수(redis_lock 의 fail_open). 0 이어야 한다.
 - 선착순 상관: 요청을 보낸 순서와 락을 잡은 순서의 스피어만 순위 상관(1 이면 선착순, 0 이면 무관). 폴링 락은 순서를 보장하지 않는다.

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함)
    python -m app.experiments.lock_stress                          # baseline, nolock, timeout, ttl
    python -m app.experiments.lock_stress --scenarios baseline --n 100
    python -m app.experiments.lock_stress --scenarios sweep
    python -m app.experiments.lock_stress --scenarios timeout --wait-sec 5
    python -m app.experiments.lock_stress --scenarios pool
"""

import argparse
import contextlib
import io
import logging
import math
import statistics
import sys
import threading
import time
import uuid

from app.services import redis_client as rc
from app.services import conversation_store as cs
from app.services import redis_lock
def _load_agent_graph():
    from langchain_core.messages import AIMessage
    from app.services import agent_graph as ag
    return ag, AIMessage


def spearman(xs: list[float], ys: list[float]) -> float:
    """스피어만 순위 상관. 값이 모두 같으면 0."""
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        for rank, i in enumerate(order):
            r[i] = float(rank)
        return r
    n = len(xs)
    if n < 2:
        return 0.0
    rx, ry = ranks(xs), ranks(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    vx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    vy = math.sqrt(sum((b - my) ** 2 for b in ry))
    return cov / (vx * vy) if vx and vy else 0.0


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


class _WarnCounter(logging.Handler):
    """경고 로그를 센다(락이 풀기 전에 만료됨, 락 우회, 대화 읽기/저장 실패). 루트 로거에 달아 stderr 로 새지 않게도 한다."""
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.expired = 0
        self.bypassed = 0
        self.load_failed = 0
        self.save_failed = 0

    def emit(self, record):
        msg = record.getMessage()
        if "만료됨" in msg:
            self.expired += 1
        if "락 없이 계속" in msg:
            self.bypassed += 1
        if "대화 기록을 못 불러옴" in msg:
            self.load_failed += 1
        if "대화 기록을 못 저장함" in msg:
            self.save_failed += 1


class Result:
    def __init__(self):
        self.scenario = ""
        self.n = 0
        self.ok = 0
        self.timeout = 0
        self.error = 0
        self.wall = 0.0
        self.waits: list[float] = []
        self.max_inside = 0
        self.model_calls = 0
        self.attempts = 0
        self.redis_cmds = 0
        self.priors: list[int] = []       # 락(또는 모델 호출) 진입 순서대로
        self.spearman = 0.0
        self.stored_turns = 0
        self.expected_turns = 0
        self.pair_ok = True
        self.order_ok = True              # 저장된 마지막 턴들이 진입 순서와 같은가
        self.leftover_lock = False
        self.expired_warnings = 0
        self.bypassed = 0
        self.load_failed = 0
        self.save_failed = 0
        self.pool_max = 0
        self.lock_on = True
        self.max_prior = 0


def run_scenario(args, ag, AIMessage, name: str, n: int, latency: float, lock_on: bool,
                 wait_sec: float, ttl_sec: float) -> Result:
    res = Result()
    res.scenario, res.n, res.lock_on = name, n, lock_on
    session = f"day33stress:{uuid.uuid4().hex[:8]}:{name}:{n}"

    probe_lock = threading.Lock()
    state = {"inside": 0, "max_inside": 0, "calls": 0, "attempts": 0}
    records: dict[int, dict] = {}          # 요청 번호 -> 기록
    tls = threading.local()

    class FakeGraph:
        def invoke(self, graph_state):
            msgs = graph_state["messages"]
            now = time.monotonic()
            with probe_lock:
                state["inside"] += 1
                state["max_inside"] = max(state["max_inside"], state["inside"])
                state["calls"] += 1
                rec = records[tls.i]
                rec["enter"] = now
                rec["prior"] = len(msgs) - 1
            time.sleep(latency)  # 모델이 답하는 시간 대신
            with probe_lock:
                state["inside"] -= 1
            return {"messages": msgs + [AIMessage(content="답:" + msgs[-1].content)]}

    orig_graph = ag.AGENT_GRAPH
    orig_lock_on = ag.SESSION_LOCK_ENABLED
    orig_lock_fn = redis_lock.lock
    orig_try = redis_lock.try_acquire

    def lock_with_limits(lock_name, ttl_sec=ttl_sec, wait_sec=wait_sec, fail_open=False):
        return orig_lock_fn(lock_name, ttl_sec=ttl_sec, wait_sec=wait_sec, fail_open=fail_open)

    def counting_try(lock_name, ttl=ttl_sec):
        with probe_lock:
            state["attempts"] += 1
        return orig_try(lock_name, ttl)

    warn = _WarnCounter()
    logging.getLogger().addHandler(warn)
    ag.AGENT_GRAPH = FakeGraph()
    ag.SESSION_LOCK_ENABLED = lock_on
    redis_lock.lock = lock_with_limits
    redis_lock.try_acquire = counting_try

    barrier = threading.Barrier(n)

    def worker(i: int) -> None:
        tls.i = i
        records[i] = {"i": i, "outcome": None}
        barrier.wait()  # 모두 같은 순간에 출발
        records[i]["start"] = time.monotonic()
        try:
            ag.run_agent(f"Q{i}", "gemini", session)
            records[i]["outcome"] = "ok"
        except redis_lock.LockTimeout:
            records[i]["outcome"] = "timeout"
        except Exception as e:  # noqa: BLE001
            records[i]["outcome"] = "error"
            records[i]["err"] = f"{type(e).__name__}: {e}"
        records[i]["end"] = time.monotonic()

    client = rc.get_client()
    cmds_before = client.info("stats")["total_commands_processed"]
    ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    t0 = time.monotonic()
    try:
        # run_agent 가 요청마다 [timing] 줄을 찍으므로(100개면 200줄) 실행하는 동안은 출력을 버린다
        with contextlib.redirect_stdout(io.StringIO()):
            for t in ts:
                t.start()
            for t in ts:
                t.join()
        res.wall = time.monotonic() - t0
        cmds_after = client.info("stats")["total_commands_processed"]
        res.redis_cmds = cmds_after - cmds_before - 1  # 마지막 info 호출 자신 1개를 뺀다

        recs = list(records.values())
        res.ok = sum(1 for r in recs if r["outcome"] == "ok")
        res.timeout = sum(1 for r in recs if r["outcome"] == "timeout")
        res.error = sum(1 for r in recs if r["outcome"] == "error")
        entered = sorted((r for r in recs if "enter" in r), key=lambda r: r["enter"])
        res.waits = sorted(r["enter"] - r["start"] for r in entered)
        res.priors = [r["prior"] for r in entered]
        res.max_prior = max(res.priors) if res.priors else 0
        res.max_inside = state["max_inside"]
        res.model_calls = state["calls"]
        res.attempts = state["attempts"]
        res.expired_warnings = warn.expired
        res.bypassed = warn.bypassed
        res.load_failed = warn.load_failed
        res.save_failed = warn.save_failed
        res.pool_max = client.connection_pool.max_connections
        res.spearman = spearman([r["start"] for r in entered], [r["enter"] for r in entered])

        hist = cs.load_history(session)
        res.stored_turns = len(hist) // 2
        res.expected_turns = min(res.ok, cs.MAX_TURNS)
        res.pair_ok = all(hist[k]["role"] == "user" and hist[k + 1]["role"] == "assistant"
                          and hist[k + 1]["content"] == "답:" + hist[k]["content"]
                          for k in range(0, len(hist) - 1, 2))
        # 락이 있으면 저장 순서가 진입 순서다. 저장된 마지막 턴들의 질문이 진입 순서의 마지막 ok 요청들과 같아야 한다
        ok_entered = [r for r in entered if r["outcome"] == "ok"]
        want = [f"Q{r['i']}" for r in ok_entered][-cs.MAX_TURNS:]
        got = [hist[k]["content"] for k in range(0, len(hist), 2)]
        res.order_ok = (got == want)
        res.leftover_lock = bool(client.exists(redis_lock.lock_key(f"conversation:{session}")))
    finally:
        ag.AGENT_GRAPH = orig_graph
        ag.SESSION_LOCK_ENABLED = orig_lock_on
        redis_lock.lock = orig_lock_fn
        redis_lock.try_acquire = orig_try
        logging.getLogger().removeHandler(warn)
        try:
            cs.clear(session)
            client.delete(redis_lock.lock_key(f"conversation:{session}"))
        except Exception:  # noqa: BLE001
            pass
    return res


def set_pool(max_connections: int | None):
    """Redis 클라이언트를 같은 설정에 연결 풀 크기만 바꿔 다시 만든다. 이전 클라이언트를 돌려준다(복원용)."""
    import redis
    old = rc._client
    rc._client = redis.Redis(host=rc.HOST, port=rc.PORT, db=rc.DB, decode_responses=True,
                             socket_connect_timeout=rc.CONNECT_TIMEOUT_SEC, socket_timeout=rc.SOCKET_TIMEOUT_SEC,
                             max_connections=max_connections)
    return old


def ideal_priors(ok: int) -> list[int]:
    return [min(2 * k, 2 * cs.MAX_TURNS) for k in range(ok)]


def print_result(res: Result, latency: float, wait_sec: float, ttl_sec: float) -> None:
    w = res.waits
    print(f"-- {res.scenario}: N={res.n}, 락 {'있음' if res.lock_on else '없음'}, 모델 대기 {latency:g}초, "
          f"락 기다림 한도 {wait_sec:g}초, 락 만료 {ttl_sec:g}초")
    print(f"   응답: 성공 {res.ok}, 시간 초과(LockTimeout) {res.timeout}, 그 밖의 오류 {res.error}   전체 시간 {res.wall:.2f}초")
    print(f"   동시 실행 최대: {res.max_inside}   모델 호출 {res.model_calls}번")
    if w:
        print(f"   대기 시간(진입한 {len(w)}건): p50 {percentile(w, 50):.2f}초, p95 {percentile(w, 95):.2f}초, 최대 {w[-1]:.2f}초")
    print(f"   앞 대화: 진입 순서의 처음 5개 {res.priors[:5]}, 마지막 5개 {res.priors[-5:]}, 최대 {res.max_prior}")
    print(f"   저장된 턴: {res.stored_turns} (기대 {res.expected_turns}, 보관 한도 {cs.MAX_TURNS}턴)   "
          f"질문-답 쌍 {'맞음' if res.pair_ok else '어긋남'}   저장 순서 {'진입 순서와 같음' if res.order_ok else '진입 순서와 다름'}")
    per = res.attempts / res.ok if res.ok else 0
    print(f"   락 시도(SET NX): {res.attempts}번 (성공 요청 1건당 {per:.1f}번)   Redis 전체 명령 {res.redis_cmds}개")
    print(f"   선착순 상관(보낸 순서 대 락 잡은 순서): {res.spearman:+.2f}   락 만료 경고 {res.expired_warnings}번   "
          f"락 키 {'남음' if res.leftover_lock else '안 남음'}")
    print(f"   락 우회(Redis 오류로 락 없이 실행): {res.bypassed}번   대화 읽기 실패 {res.load_failed}번, 저장 실패 {res.save_failed}번   "
          f"Redis 연결 풀 최대 {res.pool_max}개")


def judge_baseline(res: Result, n: int) -> list[str]:
    f = []
    if res.ok != n:
        f.append(f"baseline: 성공 {res.ok}/{n}")
    if res.timeout or res.error:
        f.append(f"baseline: 시간 초과 {res.timeout}, 오류 {res.error}")
    if res.max_inside != 1:
        f.append(f"baseline: 동시 실행 최대 {res.max_inside} (기대 1)")
    if res.priors != ideal_priors(res.ok):
        f.append("baseline: 앞 대화가 차례로 쌓이지 않음")
    if res.stored_turns != res.expected_turns or not res.pair_ok or not res.order_ok:
        f.append("baseline: 저장된 대화가 기대와 다름")
    if res.leftover_lock:
        f.append("baseline: 끝난 뒤 락 키가 남음")
    if res.bypassed:
        f.append(f"baseline: 락 우회 {res.bypassed}번 (Redis 오류로 락 없이 실행됨)")
    return f


def judge_timeout(res: Result, n: int) -> list[str]:
    f = []
    if res.ok + res.timeout != n or res.error:
        f.append(f"timeout: 성공 {res.ok} + 시간 초과 {res.timeout} != {n} (오류 {res.error})")
    if res.ok == 0 or res.timeout == 0:
        f.append(f"timeout: 성공 {res.ok}, 시간 초과 {res.timeout} (둘 다 있어야 한도 시험이 됨)")
    if res.max_inside != 1:
        f.append(f"timeout: 동시 실행 최대 {res.max_inside} (기대 1)")
    if res.model_calls != res.ok:
        f.append(f"timeout: 모델 호출 {res.model_calls} != 성공 {res.ok} (시간 초과한 요청이 모델을 불렀음)")
    if res.priors != ideal_priors(res.ok):
        f.append("timeout: 앞 대화가 차례로 쌓이지 않음")
    if res.stored_turns != res.expected_turns or not res.pair_ok or not res.order_ok:
        f.append("timeout: 저장된 대화가 기대와 다름")
    if res.leftover_lock:
        f.append("timeout: 끝난 뒤 락 키가 남음")
    return f


def main() -> int:
    p = argparse.ArgumentParser(description="분산 락 스트레스 테스트 (Day 33)")
    p.add_argument("--scenarios", default="baseline,nolock,timeout,ttl", help="baseline,nolock,timeout,ttl,sweep,pool (쉼표로)")
    p.add_argument("--n", type=int, default=100, help="같은 세션 동시 요청 수")
    p.add_argument("--latency", type=float, default=0.05, help="모델 대기 시간(초)")
    p.add_argument("--wait-sec", type=float, default=2.0, help="timeout 시나리오의 락 기다림 한도(초)")
    p.add_argument("--ttl-sec", type=float, default=0.3, help="ttl 시나리오의 락 만료(초)")
    p.add_argument("--ttl-latency", type=float, default=0.5, help="ttl 시나리오의 모델 대기 시간(초)")
    p.add_argument("--ttl-n", type=int, default=5, help="ttl 시나리오의 동시 요청 수")
    p.add_argument("--sweep", default="10,50,100,200", help="sweep 시나리오의 N 목록")
    p.add_argument("--pool-n", type=int, default=200, help="pool 시나리오의 동시 요청 수")
    p.add_argument("--pool-size", type=int, default=300, help="pool 시나리오에서 늘린 연결 풀 최대 크기")
    args = p.parse_args()

    try:
        rc.get_client().ping()
    except Exception as e:  # noqa: BLE001
        print(f"Redis 에 접속하지 못했다: {type(e).__name__}: {e}")
        return 2

    ag, AIMessage = _load_agent_graph()
    default_wait, default_ttl = redis_lock.LOCK_WAIT_SEC, redis_lock.LOCK_TTL_SEC
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    failures: list[str] = []
    print(f"== 같은 세션 동시 요청 락 스트레스, 가짜 모델, 실제 Redis (대화 보관 {cs.MAX_TURNS}턴, "
          f"연결 풀 최대 {rc.get_client().connection_pool.max_connections}개)")
    print()

    for sc in scenarios:
        if sc == "baseline":
            r = run_scenario(args, ag, AIMessage, "baseline", args.n, args.latency, True, default_wait, default_ttl)
            print_result(r, args.latency, default_wait, default_ttl)
            failures += judge_baseline(r, args.n)
        elif sc == "nolock":
            r = run_scenario(args, ag, AIMessage, "nolock", args.n, args.latency, False, default_wait, default_ttl)
            print_result(r, args.latency, default_wait, default_ttl)
            stale = sum(1 for a, b in zip(r.priors, ideal_priors(len(r.priors))) if a < b)
            print(f"   (정보) 앞 대화를 이상적인 순서보다 적게 본 요청: {stale}/{len(r.priors)}")
            if r.max_inside <= 1:
                print("   (참고) 락이 없는데 동시 실행 최대가 1 이다. 기대와 다르다")
        elif sc == "timeout":
            r = run_scenario(args, ag, AIMessage, "timeout", args.n, args.latency, True, args.wait_sec, default_ttl)
            print_result(r, args.latency, args.wait_sec, default_ttl)
            failures += judge_timeout(r, args.n)
        elif sc == "ttl":
            r = run_scenario(args, ag, AIMessage, "ttl", args.ttl_n, args.ttl_latency, True, default_wait, args.ttl_sec)
            print_result(r, args.ttl_latency, default_wait, args.ttl_sec)
            if r.max_inside <= 1:
                print("   (참고) 락 만료가 일보다 짧은데 겹치지 않았다. 기대와 다르다")
        elif sc == "sweep":
            rows = []
            for n in [int(x) for x in args.sweep.split(",") if x.strip()]:
                rows.append(run_scenario(args, ag, AIMessage, "sweep", n, args.latency, True, default_wait, default_ttl))
            print(f"-- sweep: 락 있음, 모델 대기 {args.latency:g}초 (정보용: 판정하지 않는다)")
            print(f"   {'N':>4} {'성공':>5} {'전체 시간':>9} {'이상적(N×대기)':>14} {'대기 p50':>9} {'대기 p95':>9} {'대기 최대':>9} "
                  f"{'동시 최대':>8} {'락 우회':>7} {'락 시도':>8} {'시도/건':>7} {'선착순':>7}")
            for r in rows:
                w = r.waits
                print(f"   {r.n:>4} {r.ok:>5} {r.wall:>8.2f}초 {r.n * args.latency:>13.2f}초 {percentile(w, 50):>8.2f}초 "
                      f"{percentile(w, 95):>8.2f}초 {(w[-1] if w else 0):>8.2f}초 {r.max_inside:>8} {r.bypassed:>7} {r.attempts:>8} "
                      f"{(r.attempts / r.ok if r.ok else 0):>7.1f} {r.spearman:>+7.2f}")
            broken = [r.n for r in rows if r.max_inside != 1 or r.bypassed]
            if broken:
                print(f"   (참고) N={broken} 에서 동시 실행 최대가 1 이 아니거나 락 우회가 있었다. pool 시나리오에서 원인을 본다")
        elif sc == "pool":
            r1 = run_scenario(args, ag, AIMessage, "pool-기본", args.pool_n, args.latency, True, default_wait, default_ttl)
            print_result(r1, args.latency, default_wait, default_ttl)
            print()
            old = set_pool(args.pool_size)
            try:
                r2 = run_scenario(args, ag, AIMessage, "pool-확대", args.pool_n, args.latency, True, default_wait, default_ttl)
            finally:
                rc._client.close()
                rc._client = old
            print_result(r2, args.latency, default_wait, default_ttl)
            if r1.max_inside == 1 and not r1.bypassed:
                print("   (참고) 기본 풀에서도 락이 깨지지 않았다. 이 환경에서는 N 이 더 커야 연결이 모자란다")
            failures += [f"pool 확대 N={args.pool_n}: {m}" for m in judge_baseline(r2, args.pool_n)]
        else:
            print(f"알 수 없는 시나리오: {sc}")
            return 2
        print()

    print("읽는 법: baseline 과 timeout 은 판정한다(동시 실행 최대 1, 앞 대화가 차례로 쌓임, 저장 순서, 락 키 정리).")
    print("         nolock 과 ttl 은 락이 없거나 깨졌을 때의 모습을 보는 정보용 숫자다.")
    print()
    if failures:
        print("== 결과: FAIL")
        for f in failures:
            print("  -", f)
        return 1
    print("== 결과: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
