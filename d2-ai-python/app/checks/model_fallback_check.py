"""
모델 폴백 + 서킷 브레이커 점검 (Day 36) — 서버, Kafka, Redis 없이 이 프로세스 안에서 돈다

  python -m app.checks.model_fallback_check            가짜 모델(무료, 몇 초). 폴백과 브레이커 동작을 PASS/FAIL 로 판정
  python -m app.checks.model_fallback_check --real     실제 모델. ① Gemini 만 ② Claude 만 ③ 폴백(gemini>claude)
  python -m app.checks.model_fallback_check --real --n 20  질문 수를 바꾼다(기본 3, 질문 5개를 돌려 쓴다)
  python -m app.checks.model_fallback_check --real --only gemini    ① Gemini 만 (claude 는 ② Claude 만, fallback 은 ③ 폴백만)

--real 은 Gemini 와 Claude 를 실제로 부른다(질문 1건당 모델 약 2번 + 검색 임베딩). GOOGLE_API_KEY, ANTHROPIC_API_KEY,
Qdrant 가 필요하다. 가짜 장애는 쓰지 않는다. Gemini 무료 한도에 걸려 실제로 실패하면 ③ 이 Claude 로 넘어간다.
한도에 걸리지 않으면 폴백이 일어나지 않으니, 걸릴 때까지 --n 을 늘려 다시 돌린다.
코드 흐름은 docs/DAY36_CODE_GUIDE.md, 실험은 docs/DAY36_GUIDE.md.
"""
import argparse
import os
import sys
import time

os.environ.setdefault("FAKE_LLM_ENABLED", "true")
os.environ["FAKE_LLM_DELAY_SEC"] = "0"
os.environ["FAKE_LLM_JITTER"] = "0"

from langchain_core.messages import HumanMessage  # noqa: E402

from app.services import model_fallback as mf  # noqa: E402
from app.services.agent_graph import get_model, run_agent  # noqa: E402

rows: list[tuple[str, bool, str]] = []


def rec(name: str, ok: bool, detail: str) -> None:
    rows.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  ({detail})")


def call(model, n: int) -> float:
    t0 = time.perf_counter()
    for _ in range(n):
        model.invoke([HumanMessage(content="q")])
    return time.perf_counter() - t0


def run_fake() -> None:
    os.environ["MODEL_FAULT_INJECTION"] = "true"
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    os.environ["MODEL_BREAKER_COOLDOWN_SEC"] = "1.0"
    os.environ["MODEL_DOWN_DELAY_SEC"] = "0.3"
    os.environ["MODEL_FORCE_DOWN"] = ""
    print("\n== 가짜 모델: 폴백 fake>fake-b, 브레이커 연속 3번 실패 → 1초 쉼, 장애 때 실패까지 0.3초")

    mf.reset()
    call(get_model("fake>fake-b"), 10)
    s = mf.STATS
    rec("A 정상: 앞 모델만 쓴다", s["served:fake"] == 10 and s["served:fake-b"] == 0 and s["error:fake"] == 0,
        f"fake {s['served:fake']}회, fake-b {s['served:fake-b']}회, 실패 {s['error:fake']}회")

    os.environ["MODEL_FORCE_DOWN"] = "fake"
    mf.reset()
    t_on = call(get_model("fake>fake-b"), 10)
    s = dict(mf.STATS)
    rec("B 앞 모델 장애: 모두 뒤 모델이 답한다", s.get("served:fake-b", 0) == 10, f"fake-b {s.get('served:fake-b', 0)}/10")
    rec("B 브레이커: 3번 실패 뒤에는 앞 모델을 건너뛴다", s.get("error:fake") == 3 and s.get("skipped:fake") == 7 and mf.breaker_state("fake") == "open",
        f"실패 {s.get('error:fake')}회, 건너뜀 {s.get('skipped:fake')}회, 상태 {mf.breaker_state('fake')}")

    os.environ["MODEL_BREAKER_THRESHOLD"] = "1000"
    mf.reset()
    t_off = call(get_model("fake>fake-b"), 10)
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    rec("B 브레이커 효과: 없을 때보다 빠르다", t_on < t_off, f"브레이커 있음 {t_on:.2f}초, 없음 {t_off:.2f}초 (10건)")

    mf.reset()
    call(get_model("fake>fake-b"), 5)
    time.sleep(mf.cooldown() + 0.1)
    os.environ["MODEL_FORCE_DOWN"] = "fake"
    before = mf.STATS["error:fake"]
    call(get_model("fake>fake-b"), 2)
    rec("D 쉬는 시간 뒤 한 번만 다시 시도, 또 실패하면 다시 쉰다",
        mf.STATS["error:fake"] - before == 1 and mf.breaker_state("fake") == "open",
        f"다시 시도 {mf.STATS['error:fake'] - before}번, 상태 {mf.breaker_state('fake')}")

    mf.reset()
    call(get_model("fake>fake-b"), 5)
    time.sleep(mf.cooldown() + 0.1)
    os.environ["MODEL_FORCE_DOWN"] = ""
    call(get_model("fake>fake-b"), 3)
    rec("C 복구: 쉬는 시간 뒤 다시 시도해 성공하면 앞 모델로 돌아간다",
        mf.STATS["served:fake"] == 3 and mf.breaker_state("fake") == "closed",
        f"fake {mf.STATS['served:fake']}회, 상태 {mf.breaker_state('fake')}")

    os.environ["MODEL_FORCE_DOWN"] = "fake,fake-b"
    mf.reset()
    try:
        call(get_model("fake>fake-b"), 1)
        rec("E 모두 장애: 예외를 낸다", False, "예외 없음")
    except mf.ModelDownError as e:
        rec("E 모두 장애: 마지막 모델의 예외를 낸다", True, str(e))

    os.environ["MODEL_FORCE_DOWN"] = ""
    try:
        get_model("fake>없는모델")
        rec("F 모르는 모델 이름은 거절", False, "예외 없음")
    except ValueError as e:
        rec("F 모르는 모델 이름은 거절", True, str(e)[:40])


QUESTIONS = [
    "환불은 며칠 안에 신청해야 하나요?",
    "교환은 어떻게 하나요?",
    "배송은 며칠 걸리나요?",
    "주문 취소는 어떻게 하나요?",
    "환불 수수료가 있나요?",
]


def run_real(n: int, only: str | None = None) -> None:
    if not os.getenv("GOOGLE_API_KEY") or not os.getenv("ANTHROPIC_API_KEY"):
        print("GOOGLE_API_KEY 또는 ANTHROPIC_API_KEY 가 없다(.env 확인). --real 을 건너뛴다")
        return
    os.environ["MODEL_FORCE_DOWN"] = ""  # 실제 모델 단계는 장애 주입을 쓰지 않는다. Gemini 는 실제로 부른다
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    print(f"\n== 실제 모델: 질문 {n}건씩 (이 프로세스에서 직접 실행, 가짜 장애 없음)")
    # ① 은 Gemini 무료 한도에 걸려 실패해도 폴백이 없는 경우를 보여 주는 참고 값이라 PASS/FAIL 에 넣지 않는다
    stages = [("gemini", "① Gemini 만", "gemini", True), ("claude", "② Claude 만", "claude", False),
              ("fallback", "③ 폴백 gemini>claude", "gemini>claude", False)]
    for key, label, provider, informational in stages:
        if only and only != key:  # --only 로 고른 단계만 돈다
            continue
        mf.reset()
        times, ok, errs = [], 0, []
        for i in range(n):
            q = QUESTIONS[i % len(QUESTIONS)]
            t0 = time.perf_counter()
            try:
                ans = run_agent(q, provider)
                if str(ans).strip():
                    ok += 1
                else:
                    errs.append("빈 답")
            except Exception as e:
                errs.append(f"{type(e).__name__}: {str(e)[:60]}")
            times.append(time.perf_counter() - t0)
        detail = f"성공 {ok}/{n}, 질문당 평균 {sum(times) / len(times):.2f}초, 최대 {max(times):.2f}초"
        if ">" in provider:
            detail += f", 모델 호출 기록 {dict(mf.STATS)}"
        if errs:
            detail += f", 실패 {errs[:3]}{' 외 ' + str(len(errs) - 3) + '건' if len(errs) > 3 else ''}"
        rec(label + (" (참고, 판정 안 함)" if informational else ""), True if informational else ok == n, detail)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", action="store_true")
    ap.add_argument("--n", type=int, default=3)
    # --real 에서 한 단계만 돌린다. 없으면 ①②③ 모두
    ap.add_argument("--only", choices=["gemini", "claude", "fallback"], help="--real 에서 이 단계만 실행")
    args = ap.parse_args()
    print("== Day 36 모델 폴백 + 서킷 브레이커 점검")
    if args.real:
        run_real(max(1, args.n), args.only)
    else:
        run_fake()
    fails = [r for r in rows if not r[1]]
    print(f"\n== 요약: PASS {len(rows) - len(fails)}, FAIL {len(fails)}")
    print("== 결과:", "PASS" if not fails else "FAIL")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
