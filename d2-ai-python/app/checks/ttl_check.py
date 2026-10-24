"""
Redis 만료(TTL) 정책 확인 (Day 24)

ttl_policy.py 의 값대로 실제 Redis 키에 TTL 이 걸리는지 차례로 확인하고 PASS/FAIL 을 출력한다.
테스트 키는 "day24check-..." 이름으로만 만들고 끝나면 지운다. 현재 Redis 에 있는 agent:* 키를 읽기만 하는 점검(10번)도 있다.

확인 항목
 1) 연결
 2) 정책 값끼리 어긋나지 않는다 (PROCESSING TTL > 에이전트 제한 시간)
 3) 결과 PROCESSING 은 PROCESSING TTL 로 저장된다
 4) DONE 으로 바뀌면 TTL 이 DONE 값으로 새로 시작된다 (PROCESSING 의 짧은 값이 길어진다)
 5) FAILED 는 FAILED TTL 로 저장된다
 6) 워커가 죽어 PROCESSING 으로 남은 결과는 PROCESSING TTL 이 지나면 사라진다
 7) 대화는 기본 TTL(CONVERSATION_TTL_SEC)로 저장된다
 8) 대화 TTL 은 저장할 때마다 새로 시작되고(슬라이딩), 마지막 저장 뒤 TTL 이 지나면 사라진다
 9) 락은 잡는 동안 TTL 이 걸려 있고, 풀면 키가 사라진다
10) 지금 Redis 의 agent:* 키 중 TTL 이 없는(영구) 키가 없다  ← 읽기만 한다

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함. 약 10초):
    python -m app.checks.ttl_check
"""

import sys
import time
import uuid

from app.services import conversation_store as cs
from app.services import redis_client as rc
from app.services import redis_lock as rl
from app.services import result_store as rs
from app.services import ttl_policy
TOLERANCE_SEC = 5  # 저장하고 TTL 을 읽는 사이에 지나는 시간(초). 이만큼까지는 같은 값으로 본다


def near(ttl, expected: int) -> bool:
    """남은 TTL 이 expected 에서 TOLERANCE_SEC 이내로 줄어든 값인가."""
    return ttl is not None and expected - TOLERANCE_SEC <= ttl <= expected


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    run_id = uuid.uuid4().hex[:8]
    keys: list[str] = []  # 만든 키를 끝에 지우려고 모아 둔다

    def result_id(tag: str) -> str:
        rid = f"day24check-{run_id}-{tag}"
        keys.append(rs.result_key(rid))
        return rid

    def session_id(tag: str) -> str:
        sid = f"day24check-{run_id}-{tag}"
        keys.append(cs.conversation_key(sid))
        return sid

    print(f"== Redis {rc.HOST}:{rc.PORT} db={rc.DB}")
    print(f"   정책: 결과 PROCESSING {ttl_policy.RESULT_PROCESSING_TTL_SEC}초 / DONE {ttl_policy.RESULT_DONE_TTL_SEC}초"
          f" / FAILED {ttl_policy.RESULT_FAILED_TTL_SEC}초, 대화 {ttl_policy.CONVERSATION_TTL_SEC}초, 락 {rl.LOCK_TTL_SEC:g}초")

    # 1) 연결
    p = rc.ping()
    check("1) 연결", p.get("connected") is True, str(p))
    if not p.get("connected"):
        print("Redis 에 연결할 수 없어 나머지는 건너뜀")
        return 1

    try:
        # 2) 정책 값
        found = ttl_policy.problems()
        check("2) 정책 값끼리 어긋나지 않음", not found, "; ".join(found) if found else "PROCESSING TTL > 에이전트 제한")

        store = rs.ResultStore()

        # 3) PROCESSING
        a = result_id("a")
        store.set_processing(a, worker="w1", partition=0, offset=1)
        ttl = rc.ttl_sec(rs.result_key(a))
        check("3) PROCESSING 은 PROCESSING TTL 로 저장", near(ttl, ttl_policy.RESULT_PROCESSING_TTL_SEC),
              f"남은 {ttl}초, 기대 {ttl_policy.RESULT_PROCESSING_TTL_SEC}초 이하")

        # 4) DONE 으로 바뀌면 TTL 이 새로 시작
        store.set_done(a, "답", 10)
        ttl = rc.ttl_sec(rs.result_key(a))
        check("4) DONE 이 되면 TTL 이 DONE 값으로 새로 시작", near(ttl, ttl_policy.RESULT_DONE_TTL_SEC),
              f"남은 {ttl}초, 기대 {ttl_policy.RESULT_DONE_TTL_SEC}초 이하")

        # 5) FAILED
        f = result_id("f")
        store.set_processing(f, worker="w1", partition=0, offset=2)
        store.set_failed(f, "TimeoutError: 시험")
        ttl = rc.ttl_sec(rs.result_key(f))
        check("5) FAILED 는 FAILED TTL 로 저장", near(ttl, ttl_policy.RESULT_FAILED_TTL_SEC),
              f"남은 {ttl}초, 기대 {ttl_policy.RESULT_FAILED_TTL_SEC}초 이하")

        # 6) 죽은 워커가 남긴 PROCESSING 은 사라진다 (PROCESSING TTL 을 잠깐 1초로 바꿔서 확인)
        d = result_id("dead")
        old = ttl_policy.RESULT_PROCESSING_TTL_SEC
        try:
            ttl_policy.RESULT_PROCESSING_TTL_SEC = 1
            store.set_processing(d, worker="w1", partition=0, offset=3)
        finally:
            ttl_policy.RESULT_PROCESSING_TTL_SEC = old
        before = rc.get_json(rs.result_key(d)) is not None
        time.sleep(1.3)
        after = rc.get_json(rs.result_key(d))
        check("6) PROCESSING 으로 남은 결과는 TTL 이 지나면 사라짐", before and after is None,
              f"만료 전 {'있음' if before else '없음'}, 만료 후 {after}")

        # 7) 대화 기본 TTL
        s1 = session_id("s1")
        cs.append_turn(s1, "질문", "답")
        ttl = cs.ttl_left(s1)
        check("7) 대화는 기본 TTL 로 저장", near(ttl, ttl_policy.CONVERSATION_TTL_SEC),
              f"남은 {ttl}초, 기대 {ttl_policy.CONVERSATION_TTL_SEC}초 이하")

        # 8) 대화 슬라이딩 (대화 TTL 을 잠깐 6초로 바꿔서 확인)
        s2 = session_id("s2")
        old_c = cs.TTL_SEC
        try:
            cs.TTL_SEC = 6
            cs.append_turn(s2, "첫 질문", "첫 답")
            time.sleep(3)
            left_before = cs.ttl_left(s2)           # 3초 지났으니 3~4 쯤
            cs.append_turn(s2, "둘째 질문", "둘째 답")
            left_after = cs.ttl_left(s2)            # 다시 저장했으니 6 근처로 돌아와야 한다
            time.sleep(6.5)
            gone = cs.load_history(s2)
        finally:
            cs.TTL_SEC = old_c
        renewed = left_before is not None and left_after is not None and left_after > left_before and left_after >= 5
        check("8) 대화 TTL 은 저장할 때마다 새로 시작", renewed, f"다시 저장하기 전 {left_before}초 → 후 {left_after}초")
        check("8) 마지막 저장 뒤 TTL 이 지나면 대화가 사라짐", gone == [], f"만료 후 {gone}")

        # 9) 락 TTL
        name = f"day24check-{run_id}-lock"
        keys.append(rl.lock_key(name))
        with rl.lock(name):
            lock_ttl = rc.ttl_sec(rl.lock_key(name))
        released = not rc.exists(rl.lock_key(name))
        check("9) 락은 잡는 동안 TTL 이 걸려 있음", lock_ttl is not None and 0 < lock_ttl <= rl.LOCK_TTL_SEC,
              f"남은 {lock_ttl}초, 기대 {rl.LOCK_TTL_SEC:g}초 이하")
        check("9) 락을 풀면 키가 사라짐", released)

        # 10) TTL 없는 키 점검 (읽기만)
        client = rc.get_client()
        groups: dict[str, list[int]] = {}
        permanent: list[str] = []
        for key in client.scan_iter(match=rc.KEY_PREFIX + "*", count=500):
            if "day24check-" in key:
                continue  # 이 스크립트의 테스트 키는 제외
            t = client.ttl(key)
            if t == -2:
                continue  # 읽는 사이에 만료됨
            kind = key[len(rc.KEY_PREFIX):].split(":", 1)[0]
            groups.setdefault(kind, []).append(t)
            if t == -1:
                permanent.append(key)
        if groups:
            for kind, ttls in sorted(groups.items()):
                finite = [x for x in ttls if x >= 0]
                span = f"TTL {min(finite)}~{max(finite)}초" if finite else "TTL 없음"
                print(f"       agent:{kind}:* {len(ttls)}개, {span}")
        else:
            print("       agent:* 키 없음")
        check("10) TTL 없는(영구) agent:* 키가 없음", not permanent,
              f"영구 키 {len(permanent)}개 {permanent[:3]}" if permanent else f"점검한 키 {sum(len(v) for v in groups.values())}개")
    finally:
        removed = 0
        for k in keys:
            try:
                removed += 1 if rc.delete(k) else 0
            except Exception:
                pass
        print(f"  [INFO] 테스트 키 정리: {removed}개 삭제")

    failed = [r for r in results if not r[1]]
    print()
    print(f"== 결과: {'PASS' if not failed else 'FAIL'} ({len(results) - len(failed)}/{len(results)})")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
