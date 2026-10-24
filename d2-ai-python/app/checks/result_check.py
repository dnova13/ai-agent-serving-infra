"""
결과 저장소(Redis) 동작 확인 (Day 23)

result_store 가 Day 23 에 메모리에서 Redis 로 바뀐 뒤 기대대로 동작하는지 차례로 확인하고 PASS/FAIL 을 출력한다.
테스트 키는 "agent:result:day23check-..." 아래에만 만들고 끝나면 지운다.

확인 항목
 1) 연결
 2) 받으면 PROCESSING 으로 Redis 에 저장되고 TTL 이 걸린다 (워커, 파티션, offset 포함)
 3) 끝나면 DONE 으로 갱신된다 (답, 처리 시간이 들어가고 워커/파티션/offset 은 그대로 남는다)
 4) 다른 워커처럼 메모리가 빈 저장소(새 ResultStore)에서도 같은 결과가 읽힌다  ← Day 17~22 에는 안 되던 것
 5) 실패하면 FAILED 와 에러 문구가 저장된다
 6) 없는 requestId 는 None
 7) TTL 이 지나면 쓴 쪽에서도 없다고 답한다 (메모리 복사본으로 되살리지 않는다)
 8) Redis 가 안 될 때: 쓰기/읽기가 예외 없이 메모리로 대신하고, 메모리가 빈 저장소는 못 찾는다

실행 (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함):
    python -m app.checks.result_check
"""

import sys
import time
import uuid

import redis

from app.services import redis_client as rc
from app.services import result_store as rs
from app.services import ttl_policy
def main() -> int:
    results: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({detail})" if detail else ""))

    run_id = uuid.uuid4().hex[:8]
    ids: list[str] = []   # 만든 키를 끝에 지우려고 모아 둔다

    def new_id(tag: str) -> str:
        rid = f"day23check-{run_id}-{tag}"
        ids.append(rid)
        return rid

    print(f"== Redis {rc.HOST}:{rc.PORT} db={rc.DB}, 결과 TTL PROCESSING {ttl_policy.RESULT_PROCESSING_TTL_SEC}초 / DONE {ttl_policy.RESULT_DONE_TTL_SEC}초")

    # 1) 연결
    p = rc.ping()
    check("1) 연결", p.get("connected") is True, str(p))
    if not p.get("connected"):
        print("Redis 에 연결할 수 없어 나머지는 건너뜀")
        return 1

    store = rs.ResultStore()

    # 2) PROCESSING 저장
    a = new_id("a")
    store.set_processing(a, worker="w1", partition=1, offset=7)
    raw = rc.get_json(rs.result_key(a))
    ttl = rc.ttl_sec(rs.result_key(a))
    check("2) Redis 에 PROCESSING 으로 저장",
          bool(raw) and raw["status"] == rs.PROCESSING and raw["worker"] == "w1"
          and raw["partition"] == 1 and raw["offset"] == 7,
          f"key={rs.result_key(a)} status={raw and raw.get('status')}")
    check("2) TTL 이 걸림", ttl is not None and 0 < ttl <= ttl_policy.RESULT_PROCESSING_TTL_SEC, f"남은 {ttl}초")

    # 3) DONE 갱신
    store.set_done(a, "답입니다", 1234)
    raw = rc.get_json(rs.result_key(a))
    check("3) DONE 으로 갱신, 답과 처리 시간 저장",
          bool(raw) and raw["status"] == rs.DONE and raw["answer"] == "답입니다" and raw["elapsedMs"] == 1234,
          str(raw and {k: raw[k] for k in ("status", "answer", "elapsedMs")}))
    check("3) 워커/파티션/offset 이 그대로 남음",
          bool(raw) and raw["worker"] == "w1" and raw["partition"] == 1 and raw["offset"] == 7)

    # 4) 다른 워커처럼: 메모리가 빈 새 저장소
    other = rs.ResultStore()
    got = other.get(a)
    check("4) 메모리가 빈 다른 저장소에서도 같은 결과", got is not None and got["status"] == rs.DONE and got["answer"] == "답입니다",
          str(got and got["status"]))

    # 5) FAILED
    f = new_id("f")
    store.set_processing(f, worker="w2", partition=0, offset=3)
    store.set_failed(f, "TimeoutError: 120초 안에 끝나지 않음")
    got = other.get(f)
    check("5) FAILED 와 에러 문구 저장", got is not None and got["status"] == rs.FAILED and "TimeoutError" in (got["error"] or ""),
          str(got and (got["status"], got["error"])))

    # 6) 없는 id
    check("6) 없는 requestId 는 None", store.get(new_id("none")) is None)

    # 7) TTL 만료: 쓴 쪽(메모리 복사본 있음)도 없다고 답해야 한다
    # Day 24: PROCESSING 으로 쓰는 값을 잠깐 1초로 바꾼다
    old_ttl = ttl_policy.RESULT_PROCESSING_TTL_SEC
    t = new_id("ttl")
    try:
        ttl_policy.RESULT_PROCESSING_TTL_SEC = 1
        store.set_processing(t, worker="w1", partition=0, offset=0)
    finally:
        ttl_policy.RESULT_PROCESSING_TTL_SEC = old_ttl
    before = store.get(t) is not None
    time.sleep(1.3)
    after = store.get(t)
    check("7) TTL 이 지나면 쓴 쪽에서도 None (메모리로 되살리지 않음)", before and after is None,
          f"만료 전 {'있음' if before else '없음'}, 만료 후 {after}")

    # 8) Redis 장애: 접속이 안 되는 클라이언트로 바꿔서 확인
    good_client = rc._client
    rc._client = redis.Redis(host=rc.HOST, port=1, socket_connect_timeout=0.3, socket_timeout=0.3, decode_responses=True)
    try:
        d = new_id("down")
        wrote_ok = True
        try:
            store.set_processing(d, worker="w1", partition=0, offset=9)
            store.set_done(d, "메모리 답", 5)
        except Exception as e:
            wrote_ok = False
            print(f"       쓰기 예외: {type(e).__name__}: {e}")
        mem = store.get(d)
        empty = rs.ResultStore().get(d)
    finally:
        rc._client = good_client
    check("8) Redis 가 안 되면 쓰기가 예외 없이 지나감", wrote_ok)
    check("8) 쓴 저장소는 메모리로 읽음", mem is not None and mem["status"] == rs.DONE and mem["answer"] == "메모리 답",
          str(mem and mem["status"]))
    check("8) 메모리가 빈 저장소는 못 찾음 (Day 17 의 한계가 돌아옴)", empty is None)

    # 정리
    removed = 0
    for rid in ids:
        try:
            removed += 1 if rc.delete(rs.result_key(rid)) else 0
        except Exception:
            pass
    print(f"  [INFO] 테스트 키 정리: {removed}개 삭제")

    failed = [r for r in results if not r[1]]
    print()
    print(f"== 결과: {'PASS' if not failed else 'FAIL'} ({len(results) - len(failed)}/{len(results)})")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
