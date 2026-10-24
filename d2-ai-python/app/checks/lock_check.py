"""
분산 락 동작 확인 (Day 21)

redis_lock 이 실제 Redis 에서 기대대로 동작하는지 차례로 확인하고 PASS/FAIL 을 출력한다.
테스트 키는 "agent:lock:day21check:..." 와 "agent:conversation:day21check:..." 아래에만 만들고 끝나면 지운다.

확인 항목
 1) 연결          2) 잡기(NX): 토큰 저장과 만료    3) 이미 잡혀 있으면 못 잡음
 4) 풀기: 남의 토큰으로는 안 풀리고 내 토큰으로만 풀림
 5) 기다렸다 잡기  6) 오래 못 잡으면 LockTimeout      7) 만료된 락을 늦게 푸는 쪽이 남의 락을 지우지 않음
 8) 같은 세션에 저장을 동시에 하면(락 없음/있음) 턴이 몇 개 남는가 (락 없음은 정보용, 있음은 판정)
 9) Redis 가 안 될 때: fail_open=True 면 락 없이 진행, False 면 예외
10) run_agent 에 연결: 같은 세션 동시 요청이 차례로 처리되고 앞 답을 보는가, 다른 세션은 서로 안 기다리는가
    (가짜 모델을 쓴다. langchain 등이 없으면 이 항목은 건너뜀)

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함):
    python -m app.checks.lock_check
"""

import sys
import threading
import time
import uuid

import redis

from app.services import redis_client as rc
from app.services import redis_lock as rl
from app.services import conversation_store as cs
def main() -> int:
    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    def info(text: str) -> None:
        print(f"  [INFO] {text}")

    run_id = uuid.uuid4().hex[:8]
    prefix = f"day21check:{run_id}"
    sessions: list[str] = []   # 만든 대화 키를 끝에 지우려고 모아 둔다
    lock_names: list[str] = []

    def new_name(tag: str) -> str:
        n = f"{prefix}:{tag}"
        lock_names.append(n)
        return n

    print(f"== Redis {rc.HOST}:{rc.PORT} db={rc.DB}")

    # 1) 연결
    p = rc.ping()
    check("1) 연결(ping)", p["connected"], str(p))
    if not p["connected"]:
        print("RESULT: FAIL (Redis 에 연결하지 못해 나머지는 건너뜀. docker ps 로 redis1 컨테이너 확인)")
        return 1

    saved_max_turns = cs.MAX_TURNS
    saved_client = rc._client
    try:
        # 2) 잡기
        n = new_name("basic")
        token = rl.try_acquire(n, ttl_sec=5)
        stored = rc.get_client().get(rl.lock_key(n))
        ttl = rc.get_client().pttl(rl.lock_key(n))
        check("2) try_acquire: 토큰을 돌려주고, 같은 값이 Redis 에 저장됨", token is not None and stored == token, f"token={token}")
        check("2) 만료 시간(PX)이 걸림", 0 < ttl <= 5000, f"pttl={ttl}ms")

        # 3) 이미 잡혀 있으면 못 잡음
        check("3) 이미 잡혀 있으면 try_acquire 가 None", rl.try_acquire(n, ttl_sec=5) is None)

        # 4) 풀기
        check("4) 남의 토큰으로는 안 풀림(False), 락은 그대로", rl.release(n, "wrong-token") is False and rc.get_client().get(rl.lock_key(n)) == token)
        check("4) 내 토큰으로는 풀림(True)", rl.release(n, token) is True and rc.get_client().get(rl.lock_key(n)) is None)
        again = rl.try_acquire(n, ttl_sec=5)
        check("4) 풀린 뒤에는 다시 잡을 수 있음", again is not None)
        rl.release(n, again)

        # 5) 기다렸다 잡기
        n = new_name("wait")
        holder = rl.try_acquire(n, ttl_sec=10)
        threading.Timer(0.5, lambda: rl.release(n, holder)).start()
        t0 = time.monotonic()
        with rl.lock(n, ttl_sec=5, wait_sec=3) as t:
            waited = time.monotonic() - t0
            check("5) 앞 사람이 풀 때까지 기다렸다가 잡음", t is not None and 0.4 <= waited < 2.5, f"기다린 시간 {waited:.2f}초")
        check("5) with 블록을 나오면 락이 풀림", rc.get_client().get(rl.lock_key(n)) is None)

        # 6) 오래 못 잡으면 LockTimeout
        n = new_name("timeout")
        holder = rl.try_acquire(n, ttl_sec=10)
        t0 = time.monotonic()
        try:
            with rl.lock(n, ttl_sec=5, wait_sec=0.5):
                got = True
        except rl.LockTimeout:
            got = False
        elapsed = time.monotonic() - t0
        check("6) wait_sec 안에 못 잡으면 LockTimeout", got is False and 0.4 <= elapsed < 2, f"기다린 시간 {elapsed:.2f}초")
        check("6) 타임아웃 뒤에도 쥐고 있던 쪽의 락은 그대로", rc.get_client().get(rl.lock_key(n)) == holder)
        rl.release(n, holder)

        # 7) 만료된 락을 늦게 푸는 쪽이 남의 락을 지우면 안 됨
        n = new_name("expire")
        a = rl.try_acquire(n, ttl_sec=0.3)
        time.sleep(0.5)  # a 의 락이 만료된다
        b = rl.try_acquire(n, ttl_sec=10)
        check("7) a 의 락이 만료되면 b 가 잡을 수 있음", a is not None and b is not None and a != b)
        released = rl.release(n, a)  # 늦게 끝난 a 가 푼다
        check("7) 늦게 푸는 a 는 아무것도 지우지 못하고(False), b 의 락이 남음", released is False and rc.get_client().get(rl.lock_key(n)) == b)
        rl.release(n, b)

        # 8) 같은 세션에 저장을 동시에: 락 없음 vs 있음
        cs.MAX_TURNS = 1000  # 자르기 때문에 개수가 줄어 보이지 않게
        workers = 20

        def append_many(session: str, use_lock: bool) -> None:
            barrier = threading.Barrier(workers)

            def one(i: int) -> None:
                barrier.wait()  # 모두 같은 순간에 시작
                if use_lock:
                    with rl.lock(f"conversation:{session}", ttl_sec=10, wait_sec=20):
                        cs.append_turn(session, f"Q{i}", f"A{i}")
                else:
                    cs.append_turn(session, f"Q{i}", f"A{i}")

            ts = [threading.Thread(target=one, args=(i,)) for i in range(workers)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()

        s_no = f"{prefix}:nolock"
        s_yes = f"{prefix}:lock"
        sessions += [s_no, s_yes]
        append_many(s_no, use_lock=False)
        turns_no = len(cs.load_history(s_no)) // 2
        info(f"락 없이 {workers}개 스레드가 같은 세션에 동시에 저장: {turns_no}턴 남음 (유실 {workers - turns_no}). 매번 다르고 0 일 수도 있다")
        append_many(s_yes, use_lock=True)
        h = cs.load_history(s_yes)
        questions = {m["content"] for m in h if m["role"] == "user"}
        check(f"8) 락을 걸고 {workers}개 스레드가 동시에 저장하면 {workers}턴이 모두 남음", len(h) == workers * 2 and len(questions) == workers, f"{len(h) // 2}턴, 서로 다른 질문 {len(questions)}개")
        cs.MAX_TURNS = saved_max_turns

        # 9) Redis 가 안 될 때
        n = new_name("down")
        rc._client = redis.Redis(host=rc.HOST, port=1, socket_connect_timeout=0.3, socket_timeout=0.3)  # 닫힌 포트
        ran = []
        try:
            with rl.lock(n, fail_open=True) as t:
                ran.append(t)
        except Exception as e:  # noqa: BLE001
            ran.append(e)
        check("9) Redis 가 안 되면 fail_open=True 는 락 없이(토큰 None) 블록을 실행", ran == [None], str(ran))
        try:
            with rl.lock(n, fail_open=False):
                pass
            raised = None
        except redis.RedisError as e:
            raised = type(e).__name__
        check("9) fail_open=False 는 Redis 예외를 그대로 올림", raised is not None, str(raised))
        rc._client = saved_client

        # 10) run_agent 에 연결
        try:
            from langchain_core.messages import AIMessage
            from app.services import agent_graph as ag
        except Exception as e:  # noqa: BLE001
            info(f"10) 건너뜀: agent_graph 를 불러오지 못함 ({type(e).__name__}: {e})")
            ag = None

        if ag is not None:
            lock_saved = ag.SESSION_LOCK_ENABLED
            graph_saved = ag.AGENT_GRAPH
            model_seen: list[tuple[str, int]] = []   # (질문, 모델이 받은 이전 대화 메시지 수)
            seen_lock = threading.Lock()

            class FakeGraph:
                def invoke(self, state):
                    msgs = state["messages"]
                    with seen_lock:
                        model_seen.append((msgs[-1].content, len(msgs) - 1))
                    time.sleep(0.4)  # 모델이 답하는 시간 대신
                    return {"messages": msgs + [AIMessage(content="답:" + msgs[-1].content)]}

            ag.AGENT_GRAPH = FakeGraph()
            try:
                def burst(session: str, count: int) -> float:
                    barrier = threading.Barrier(count)

                    def one(i: int) -> None:
                        barrier.wait()
                        ag.run_agent(f"Q{i}", "gemini", session)

                    ts = [threading.Thread(target=one, args=(i,)) for i in range(count)]
                    t0 = time.monotonic()
                    for t in ts:
                        t.start()
                    for t in ts:
                        t.join()
                    return time.monotonic() - t0

                # 락 있음: 같은 세션 4개
                s1 = f"{prefix}:run1"
                sessions.append(s1)
                ag.SESSION_LOCK_ENABLED = True
                model_seen.clear()
                took = burst(s1, 4)
                priors = sorted(c for _, c in model_seen)
                turns = len(cs.load_history(s1)) // 2
                check("10) 락 있음: 같은 세션 4개 요청이 차례로 처리되어 4턴이 모두 저장됨", turns == 4, f"{turns}턴, 걸린 시간 {took:.2f}초")
                check("10) 락 있음: 뒤 요청일수록 앞 대화를 더 봄 (이전 메시지 수 0, 2, 4, 6)", priors == [0, 2, 4, 6], f"{priors}")
                check("10) 락 있음: 차례로 처리되어 시간이 4건 합(약 1.6초) 이상", took >= 1.5, f"{took:.2f}초")

                # 락 없음: 같은 세션 4개 (정보용)
                s2 = f"{prefix}:run2"
                sessions.append(s2)
                ag.SESSION_LOCK_ENABLED = False
                model_seen.clear()
                took2 = burst(s2, 4)
                priors2 = sorted(c for _, c in model_seen)
                turns2 = len(cs.load_history(s2)) // 2
                info(f"락 없음: 같은 세션 4개 동시 → 저장 {turns2}턴, 모델이 받은 이전 메시지 수 {priors2}, 걸린 시간 {took2:.2f}초")

                # 다른 세션은 서로 안 기다림
                ag.SESSION_LOCK_ENABLED = True
                sa, sb = f"{prefix}:runA", f"{prefix}:runB"
                sessions += [sa, sb]
                done = []

                def run_other(sess: str) -> None:
                    ag.run_agent("QX", "gemini", sess)
                    done.append(sess)

                ta = threading.Thread(target=run_other, args=(sa,))
                tb = threading.Thread(target=run_other, args=(sb,))
                t0 = time.monotonic()
                ta.start()
                tb.start()
                ta.join()
                tb.join()
                took3 = time.monotonic() - t0
                check("10) 다른 세션 2개는 서로 기다리지 않음 (한 건 시간 0.4초 근처)", took3 < 0.75, f"{took3:.2f}초")
                check("10) 요청이 끝나면 세션 락이 남지 않음", rc.get_client().get(rl.lock_key(f"conversation:{s1}")) is None)
            finally:
                ag.SESSION_LOCK_ENABLED = lock_saved
                ag.AGENT_GRAPH = graph_saved
    finally:
        cs.MAX_TURNS = saved_max_turns
        rc._client = saved_client
        for s in sessions:   # 중간에 실패해도 테스트 키는 남기지 않는다
            try:
                cs.clear(s)
                rc.get_client().delete(rl.lock_key(f"conversation:{s}"))
            except Exception:  # noqa: BLE001
                pass
        for n in lock_names:
            try:
                rc.get_client().delete(rl.lock_key(n))
            except Exception:  # noqa: BLE001
                pass

    failed = [n for n, ok, _ in results if not ok]
    print(f"== 결과: {len(results) - len(failed)}/{len(results)} 통과")
    print("RESULT: PASS" if not failed else "RESULT: FAIL (" + ", ".join(failed) + ")")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
