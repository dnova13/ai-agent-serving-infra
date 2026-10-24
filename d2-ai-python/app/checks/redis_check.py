"""
Redis 기본 동작 확인 (Day 19)

redis_client 의 함수가 실제 Redis 에서 기대대로 동작하는지 차례로 확인하고 PASS/FAIL 을 출력한다.
테스트 키는 "agent:day19:check:..." 아래에만 만들고 끝나면 지운다.

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함):
    python -m app.checks.redis_check
"""

import sys
import time
import uuid

from app.services import redis_client as rc
TTL_SEC = 3


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    run_id = uuid.uuid4().hex[:8]
    k_plain = rc.make_key("day19", "check", run_id, "plain")
    k_ttl = rc.make_key("day19", "check", run_id, "ttl")
    k_missing = rc.make_key("day19", "check", run_id, "missing")

    print(f"== Redis {rc.HOST}:{rc.PORT} db={rc.DB}")

    # 1) 연결
    p = rc.ping()
    check("1) 연결(ping)", p["connected"], str(p))
    if not p["connected"]:
        print("RESULT: FAIL (Redis 에 연결하지 못해 나머지는 건너뜀. docker ps 로 redis1 컨테이너 확인)")
        return 1

    try:
        # 2) 저장하고 꺼내기: 한글과 중첩 구조가 그대로 돌아오는가
        value = {"question": "환불은 며칠 안에 되나요?", "turns": [1, 2, 3], "meta": {"ok": True, "n": None}}
        rc.set_json(k_plain, value)
        got = rc.get_json(k_plain)
        check("2) 저장 후 조회(한글, 중첩 dict, list, bool, null)", got == value, f"got={got}")

        # 3) 같은 키 덮어쓰기
        rc.set_json(k_plain, {"v": 2})
        check("3) 같은 키를 다시 쓰면 덮어씀", rc.get_json(k_plain) == {"v": 2})

        # 4) 없는 키
        check("4) 없는 키는 None", rc.get_json(k_missing) is None)
        check("4) exists: 있는 키 True, 없는 키 False", rc.exists(k_plain) and not rc.exists(k_missing))

        # 5) 만료 시간 없는 키
        check("5) TTL 없이 저장하면 영구(-1)", rc.ttl_sec(k_plain) == -1, f"ttl={rc.ttl_sec(k_plain)}")

        # 6) 만료 시간
        rc.set_json(k_ttl, {"v": "곧 사라짐"}, ttl_sec=TTL_SEC)
        t = rc.ttl_sec(k_ttl)
        check(f"6) ttl_sec={TTL_SEC} 로 저장하면 남은 시간이 0~{TTL_SEC}초", t is not None and 0 < t <= TTL_SEC, f"ttl={t}")
        print(f"     ... {TTL_SEC + 1}초 기다리는 중")
        time.sleep(TTL_SEC + 1)
        check("6) 만료 뒤에는 조회가 None, ttl_sec 도 None", rc.get_json(k_ttl) is None and rc.ttl_sec(k_ttl) is None)

        # 7) 삭제
        check("7) delete: 있던 키는 True", rc.delete(k_plain) is True)
        check("7) delete: 이미 없는 키는 False", rc.delete(k_plain) is False)
    finally:
        for k in (k_plain, k_ttl, k_missing):  # 중간에 실패해도 테스트 키는 남기지 않는다
            try:
                rc.delete(k)
            except Exception:
                pass

    failed = [n for n, ok, _ in results if not ok]
    print(f"== 결과: {len(results) - len(failed)}/{len(results)} 통과")
    print("RESULT: PASS" if not failed else "RESULT: FAIL (" + ", ".join(failed) + ")")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
