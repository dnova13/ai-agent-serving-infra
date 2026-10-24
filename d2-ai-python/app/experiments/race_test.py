"""
같은 세션 동시 요청 테스트: 락 있음 / 없음 비교 (Day 22)

같은 sessionId 로 요청 N 개가 동시에 올 때 락이 있으면 무엇이 달라지는지를 여러 번(라운드) 반복해서 숫자로 본다.
Day 21 에서 한 번씩 본 것(DAY21_RESULT 3-3, 3-5)을 반복 가능한 형태로 만든 것이다.

두 가지 방식
 - inproc (기본): 이 프로세스 안에서 스레드로 run_agent 를 동시에 부른다. 모델은 가짜(대기 시간만 흉내)라서 빠르고 돈이 안 든다.
                  Redis 는 진짜다. 앞 대화를 몇 개 보고 답했는지(prior)도 잴 수 있다.
 - http:          떠 있는 서버(워커)에 실제 HTTP 요청을 동시에 보낸다. 실제 Gemini 를 쓰므로 느리고 호출이 든다.
                  락을 켜고 끄는 것은 서버를 띄울 때의 SESSION_LOCK_ENABLED 로 한다(이 스크립트가 바꾸지 못한다).
                  앞 대화 수(prior)는 서버 로그의 "[timing] 이전 대화 N개" 로만 볼 수 있다.
                  주의: POST /api/v1/agent/run 이 async def 인데 안에서 동기 함수(run_agent)를 불러, 서버 한 대는 한 번에 한 요청만 처리한다
                  (이벤트 루프가 막힌다, Day 17~18 의 알려진 한계). 그래서 서버가 2대면 실제로 동시에 처리되는 것은 2건이다.
                  락 효과를 깨끗이 보려면 N 을 서버 수와 같게(기본값) 둔다.

시나리오 (inproc)
 - fixed:  같은 세션 N 개, 모델 대기 시간이 모두 같다(0.4초). 모두 같은 순간에 끝나 저장이 겹치기 가장 쉬운 경우.
 - jitter: 같은 세션 N 개, 대기 시간이 요청마다 다르다(0.3~1.2초). 실제 모델처럼 끝나는 시각이 흩어진 경우.
 - diff:   서로 다른 세션 N 개. 락이 세션별이라 서로 기다리지 않아야 한다(락이 처리량을 깎지 않는지).

보는 숫자
 - 저장된 턴: N 개를 보냈는데 몇 턴이 남았나. 모자라면 유실(읽기-수정-쓰기 경합).
 - 앞 대화 반영: 각 요청이 모델에 넘긴 "이전 대화 메시지 수". 차례로 처리됐다면 0, 2, 4, ... 이므로 합이 N*(N-1) 이다.
                 실제 합 / N*(N-1) 을 "반영률" 로 본다. 1.00 이면 모두 앞 대화를 보고 답한 것.
 - 라운드 시간: 요청 N 개를 보내고 모두 끝날 때까지. 락이 있으면 같은 세션은 약 N 배가 된다.

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함)
    python -m app.experiments.race_test                                  # inproc: N=4, 10라운드, 시나리오 fixed,jitter,diff
    python -m app.experiments.race_test --n 6 --rounds 20
    python -m app.experiments.race_test --mode http --urls http://localhost:8000,http://localhost:8001 --rounds 3 --label lock-on   # n 기본 = 서버 수(2)
    python -m app.experiments.race_test --mode http --rounds 2 --provider claude --label lock-on-claude   # Claude 로 (기본은 gemini)
"""

import argparse
import json
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

from app.services import redis_client as rc
from app.services import conversation_store as cs

# http 방식에서 질문을 돌려 쓴다(같은 세션에 서로 다른 질문이 들어가야 저장된 턴을 구분할 수 있다)
QUESTIONS = [
    "환불은 며칠 안에 신청해야 하나요?",
    "교환 배송비는 누가 내나요?",
    "주문 취소는 어떻게 하나요?",
    "배송은 보통 얼마나 걸리나요?",
    "포인트는 언제 적립되나요?",
    "영수증 재발급은 어떻게 하나요?",
    "회원 탈퇴하면 정보는 어떻게 되나요?",
    "쿠폰은 중복 사용이 되나요?",
    "해외 배송도 되나요?",
    "품절 상품은 재입고 알림이 되나요?",
]


# ── inproc ─────────────────────────────────────────────────────────────────────────────

def _load_agent_graph():
    from langchain_core.messages import AIMessage
    from app.services import agent_graph as ag
    return ag, AIMessage


class _Round:
    """한 라운드의 결과."""
    def __init__(self, turns, priors, wall, pair_ok):
        self.turns = turns        # 저장된 턴 수
        self.priors = priors      # 모델이 받은 이전 대화 메시지 수(정렬)
        self.wall = wall          # 라운드 시간(초)
        self.pair_ok = pair_ok    # 저장된 (질문, 답) 쌍이 모두 짝이 맞는가


def _run_inproc(args) -> int:
    ag, AIMessage = _load_agent_graph()
    rng = random.Random(args.seed)
    seen: dict[str, list[int]] = {}      # 세션 -> 모델이 받은 이전 대화 메시지 수 목록
    seen_lock = threading.Lock()
    latency_of = {"fixed": lambda: args.latency,
                  "jitter": lambda: rng.uniform(args.jitter_lo, args.jitter_hi),
                  "diff": lambda: args.latency}
    current = {"scenario": "fixed"}

    _tls = threading.local()   # 요청 스레드가 자기 세션 이름을 모델 쪽에 알리는 용도(어느 세션이 앞 대화를 몇 개 봤는지 기록)

    class FakeGraph:
        def invoke(self, state):
            msgs = state["messages"]
            with seen_lock:
                seen.setdefault(_tls.session, []).append(len(msgs) - 1)
                delay = latency_of[current["scenario"]]()
            time.sleep(delay)  # 모델이 답하는 시간 대신
            return {"messages": msgs + [AIMessage(content="답:" + msgs[-1].content)]}

    ag.AGENT_GRAPH = FakeGraph()
    lock_saved = ag.SESSION_LOCK_ENABLED
    prefix = f"day22race:{uuid.uuid4().hex[:8]}"
    sessions: list[str] = []

    def one_round(scenario: str, lock_on: bool, r: int) -> _Round:
        ag.SESSION_LOCK_ENABLED = lock_on
        current["scenario"] = scenario
        n = args.n
        names = ([f"{prefix}:{scenario}:{int(lock_on)}:{r}"] * n if scenario != "diff"
                 else [f"{prefix}:{scenario}:{int(lock_on)}:{r}:{i}" for i in range(n)])
        for s in set(names):
            sessions.append(s)
            seen[s] = []
        barrier = threading.Barrier(n)

        def worker(i: int) -> None:
            _tls.session = names[i]
            barrier.wait()  # 모두 같은 순간에 출발
            ag.run_agent(f"Q{i}", "gemini", names[i])

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        t0 = time.monotonic()
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        wall = time.monotonic() - t0
        if scenario == "diff":
            turns = sum(len(cs.load_history(s)) // 2 for s in set(names))
            priors = sorted(c for s in set(names) for c in seen[s])
            hist = [m for s in set(names) for m in cs.load_history(s)]
        else:
            hist = cs.load_history(names[0])
            turns = len(hist) // 2
            priors = sorted(seen[names[0]])
        pair_ok = all(hist[i]["role"] == "user" and hist[i + 1]["role"] == "assistant"
                      and hist[i + 1]["content"] == "답:" + hist[i]["content"]
                      for i in range(0, len(hist) - 1, 2))
        return _Round(turns, priors, wall, pair_ok)

    results: dict[tuple[str, bool], list[_Round]] = {}
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    try:
        for scenario in scenarios:
            for lock_on in (True, False):
                rounds = []
                for r in range(args.rounds):
                    rounds.append(one_round(scenario, lock_on, r))
                results[(scenario, lock_on)] = rounds
                print(f"  .. {scenario} 락 {'있음' if lock_on else '없음'} {args.rounds}라운드 끝", flush=True)
    finally:
        ag.SESSION_LOCK_ENABLED = lock_saved
        for s in sessions:
            try:
                cs.clear(s)
            except Exception:  # noqa: BLE001
                pass

    return _report_inproc(args, results)


def _report_inproc(args, results) -> int:
    n = args.n
    ideal = n * (n - 1)
    print()
    print(f"== 같은 세션 동시 요청 N={n}, {args.rounds}라운드, 가짜 모델 (fixed/diff {args.latency:g}초, "
          f"jitter {args.jitter_lo:g}~{args.jitter_hi:g}초), 실제 Redis")
    print()
    head = (f"{'시나리오':<8} {'락':<4} {'저장된 턴(평균/최소, 기대 '+str(n)+')':<26} {'유실 라운드':<10} "
            f"{'앞 대화 반영률':<14} {'완벽한 라운드':<12} {'라운드 시간(평균/최대)':<22}")
    print(head)
    print("-" * len(head))
    failures = []
    for (scenario, lock_on), rounds in results.items():
        turns = [r.turns for r in rounds]
        loss_rounds = sum(1 for t in turns if t < n)
        if scenario == "diff":
            ratio_txt, perfect_txt = "-", "-"
        else:
            ratios = [sum(r.priors) / ideal for r in rounds] if ideal else [1.0]
            perfect = sum(1 for r in rounds if r.priors == [2 * i for i in range(n)])
            ratio_txt = f"{statistics.mean(ratios):.2f}"
            perfect_txt = f"{perfect}/{len(rounds)}"
        walls = [r.wall for r in rounds]
        print(f"{scenario:<8} {'있음' if lock_on else '없음':<4} "
              f"{statistics.mean(turns):>5.2f} / {min(turns):<3}{'':<14} "
              f"{loss_rounds}/{len(rounds):<8} {ratio_txt:<14} {perfect_txt:<12} "
              f"{statistics.mean(walls):.2f}초 / {max(walls):.2f}초")
        if lock_on:  # 락 있음은 판정한다. 락 없음은 매번 다르므로 정보용이다
            if loss_rounds:
                failures.append(f"{scenario} 락 있음: 유실 {loss_rounds}라운드")
            if scenario != "diff" and any(r.priors != [2 * i for i in range(n)] for r in rounds):
                failures.append(f"{scenario} 락 있음: 앞 대화 반영이 완벽하지 않은 라운드가 있음")
            if not all(r.pair_ok for r in rounds):
                failures.append(f"{scenario} 락 있음: 저장된 질문-답 쌍이 어긋남")
            if scenario == "diff" and max(walls) > args.latency * 2.5:
                failures.append(f"diff 락 있음: 다른 세션인데 오래 걸림 ({max(walls):.2f}초)")
    print()
    print("읽는 법: 락 있음은 판정(유실 0, 반영률 1.00, 완벽한 라운드 전부). 락 없음은 매번 다른 정보용 숫자다.")
    print("         fixed 는 끝나는 순간이 같아 겹치기 쉽고, jitter 는 흩어져서 유실은 드물지만 앞 대화를 놓치는 일은 남는다.")
    print()
    if failures:
        print("== 결과: FAIL")
        for f in failures:
            print("  -", f)
        return 1
    print("== 결과: PASS (락 있음의 유실 0, 앞 대화 반영 완벽, 다른 세션은 서로 안 기다림)")
    return 0


# ── http ───────────────────────────────────────────────────────────────────────────────

def _http(method: str, url: str, body: dict | None = None, timeout: float = 130.0):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "null"), time.monotonic() - t0
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode("utf-8") or "null")
        except Exception:  # noqa: BLE001
            payload = None
        return e.code, payload, time.monotonic() - t0
    except Exception as e:  # noqa: BLE001
        return 0, {"error": f"{type(e).__name__}: {e}"}, time.monotonic() - t0


def _run_http(args) -> int:
    urls = [u.strip().rstrip("/") for u in args.urls.split(",") if u.strip()]
    if not urls:
        print("--urls 가 비어 있다")
        return 2
    n = args.n
    if n > len(urls):
        print(f"!! N={n} 이 서버 수({len(urls)})보다 많다. 서버 한 대는 한 번에 한 요청만 처리하므로(async def 가 막힘) "
              f"같은 서버에 몰린 요청은 락이 없어도 차례로 처리된다. 락 있음/없음 차이가 작아 보일 수 있다.")
    print(f"== http 방식: {len(urls)}개 서버({', '.join(urls)})에 같은 세션 요청 N={n} 을 동시에, {args.rounds}라운드, 이름 '{args.label}', provider {args.provider}")
    print("   서버를 SESSION_LOCK_ENABLED 를 어떻게 하고 띄웠는지는 이 스크립트가 모른다. --label 에 직접 적어 구분한다.")
    print(f"   라운드마다 실제 모델 호출 {n}번. 락이 있으면 같은 세션 {n}건이 차례로 처리되어 한 라운드가 약 {n}배 걸린다.")
    print()
    rows = []
    for r in range(args.rounds):
        session = f"day22http-{uuid.uuid4().hex[:8]}"
        _http("DELETE", f"{urls[0]}/api/v1/agent/sessions/{session}")   # 깨끗이 시작
        out: list = [None] * n
        barrier = threading.Barrier(n)

        def worker(i: int) -> None:
            base = urls[i % len(urls)]
            barrier.wait()
            # Day 22: 요청에 provider 를 실어 보낸다(gemini 또는 claude). 기본 gemini 라 기존 동작은 같다.
            out[i] = _http("POST", f"{base}/api/v1/agent/run",
                           {"query": QUESTIONS[i % len(QUESTIONS)], "session_id": session,
                            "provider": args.provider})

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        t0 = time.monotonic()
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        wall = time.monotonic() - t0
        status, hist, _ = _http("GET", f"{urls[0]}/api/v1/agent/sessions/{session}")
        turns = hist.get("turns") if status == 200 and isinstance(hist, dict) else None
        ok200 = sum(1 for o in out if o[0] == 200)
        codes = sorted(o[0] for o in out)
        times = sorted(o[2] for o in out)
        rows.append((r + 1, ok200, turns, codes, times, wall))
        print(f"  라운드 {r + 1}: 200 응답 {ok200}/{n}, 저장된 턴 {turns}, 상태코드 {codes}, "
              f"요청별 시간 {', '.join(f'{t:.1f}' for t in times)}초, 라운드 {wall:.1f}초", flush=True)
        for o in out:   # 200 이 아닌 응답은 본문을 짧게 보여 준다(원인은 해당 워커 터미널의 오류 로그에 있다)
            if o[0] != 200:
                print(f"      └ 상태 {o[0]}: {str(o[1])[:150]}", flush=True)
        _http("DELETE", f"{urls[0]}/api/v1/agent/sessions/{session}")
    print()
    # 저장된 턴이 200 응답 수보다 적으면 유실. 성공(200)이 하나도 없으면 저장될 것이 없으니 유실이 아니다(turns None = 저장된 대화 없음)
    loss = sum(1 for _, ok200, turns, *_ in rows if (turns or 0) < ok200)
    failed_rounds = sum(1 for _, ok200, *_ in rows if ok200 < n)
    print(f"== [{args.label}] 라운드 {len(rows)}개 중 요청이 실패한(200 이 아닌) 라운드 {failed_rounds}개, 유실(저장된 턴 < 200 응답 수) {loss}개, "
          f"라운드 시간 평균 {statistics.mean(w for *_, w in rows):.1f}초 / 최대 {max(w for *_, w in rows):.1f}초")
    print("   앞 대화를 몇 개 보고 답했는지는 각 워커 터미널의 '[timing] 이전 대화 N개 메시지' 로그를 본다")
    print("   (락 있음이면 0, 2, 4, ... 가 한 번씩, 락 없음이면 대부분 0).")
    return 0


# ── 진입점 ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    p = argparse.ArgumentParser(description="같은 세션 동시 요청 테스트: 락 있음/없음 비교")
    p.add_argument("--mode", choices=["inproc", "http"], default="inproc")
    p.add_argument("--n", type=int, default=None, help="동시 요청 수(2~10, 대화 보관 한도가 10턴). 기본: inproc 4, http 는 서버 수")
    p.add_argument("--rounds", type=int, default=10)
    p.add_argument("--scenarios", default="fixed,jitter,diff", help="inproc 시나리오(쉼표로)")
    p.add_argument("--latency", type=float, default=0.4, help="inproc fixed/diff 의 모델 대기 시간(초)")
    p.add_argument("--jitter-lo", type=float, default=0.3)
    p.add_argument("--jitter-hi", type=float, default=1.2)
    p.add_argument("--seed", type=int, default=22)
    p.add_argument("--urls", default="http://localhost:8000,http://localhost:8001", help="http 방식의 서버들(쉼표로)")
    p.add_argument("--label", default="lock-?", help="http 결과에 붙일 이름(예: lock-on, lock-off)")
    p.add_argument("--provider", choices=["gemini", "claude"], default="gemini",
                   help="http 방식에서 요청에 실을 모델 공급자(기본 gemini). claude 는 서버 .env 에 ANTHROPIC_API_KEY 가 있어야 한다")
    args = p.parse_args()
    if args.n is None:
        args.n = 4 if args.mode == "inproc" else max(2, len([u for u in args.urls.split(",") if u.strip()]))
    if not 2 <= args.n <= 10:
        print("--n 은 2~10 이어야 한다(대화는 최근 10턴만 저장되므로 그 이상은 턴 수를 해석할 수 없다)")
        return 2
    health = rc.ping()   # ping 은 예외 없이 {"connected": ...} 를 돌려준다
    if not health.get("connected"):
        print(f"Redis 에 연결하지 못함: {health.get('error')}")
        return 2
    return _run_http(args) if args.mode == "http" else _run_inproc(args)


if __name__ == "__main__":
    sys.exit(main())
