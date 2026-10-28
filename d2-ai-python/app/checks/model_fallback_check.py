"""
모델 폴백 + 서킷 브레이커 점검 (Day 36) — 서버, Kafka, Redis 없이 이 프로세스 안에서 돈다

  python -m app.checks.model_fallback_check            가짜 모델(무료, 몇 초). 폴백과 브레이커 동작을 PASS/FAIL 로 판정
  python -m app.checks.model_fallback_check --real     실제 모델. ① Gemini 만 ② Claude 만 ③ 폴백(gemini>claude)
  python -m app.checks.model_fallback_check --real --n 20  질문 수를 바꾼다(기본 3, 질문 5개를 돌려 쓴다)
  python -m app.checks.model_fallback_check --real --only gemini    ① Gemini 만 (claude 는 ② Claude 만, fallback 은 ③ 폴백만)
  python -m app.checks.model_fallback_check --fake-claude --dry     앞 모델 fake(장애 주입), 뒤 모델 가짜 fake-b. Claude 호출 0번(흐름 확인)
  python -m app.checks.model_fallback_check --fake-claude           앞 모델 fake(장애 주입), 뒤 모델 실제 Claude. 호출 약 32번, 코드 상한 40번

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


# [NEW] 뒤 모델을 실제 Claude 로 둔 폴백 시험(fake>claude). 사용자가 허락한 Claude 호출 상한(40번)을 코드로도 막는다
CLAUDE_CALL_LIMIT = 40


class CountedBack:
    """뒤 모델을 감싸 실제 호출 횟수와 시간을 기록한다. 상한에 닿으면 호출하지 않고 멈춘다."""
    total = 0
    seconds: list[float] = []

    def __init__(self, inner):
        self.inner = inner

    def bind_tools(self, tools):
        return CountedBack(self.inner.bind_tools(tools))

    def invoke(self, messages):
        if CountedBack.total >= CLAUDE_CALL_LIMIT:
            raise RuntimeError(f"뒤 모델 호출 상한({CLAUDE_CALL_LIMIT}번)에 닿아 멈춘다")
        CountedBack.total += 1
        t0 = time.perf_counter()
        try:
            return self.inner.invoke(messages)
        finally:
            CountedBack.seconds.append(time.perf_counter() - t0)


def run_fake_claude(dry: bool, cooldown: float = 1.0) -> None:
    """앞 모델은 가짜(장애 주입), 뒤 모델은 실제 Claude. dry 이면 뒤 모델을 가짜(fake-b)로 바꿔 코드 흐름만 본다(Claude 0번).
    cooldown 은 브레이커 쉬는 시간(초). 뒤 모델 응답(약 1초)보다 짧으면 도중에 다시 시도가 끼어든다(1차 측정에서 확인)."""
    if not dry and not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY 가 없다(.env 확인). fake>claude 시험을 건너뛴다")
        return
    os.environ["MODEL_FAULT_INJECTION"] = "true"
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    # [OLD] os.environ["MODEL_BREAKER_COOLDOWN_SEC"] = "1.0"
    os.environ["MODEL_BREAKER_COOLDOWN_SEC"] = str(cooldown)  # [NEW] --cooldown 으로 바꾼다(기본 1.0)
    os.environ["MODEL_DOWN_DELAY_SEC"] = "0.3"
    os.environ["MODEL_FORCE_DOWN"] = ""
    back = "fake-b" if dry else "claude"
    print(f"\n== fake>claude: 앞 모델 fake(장애 주입), 뒤 모델 {'가짜 fake-b (dry, Claude 호출 0번)' if dry else '실제 Claude'}")
    # [OLD] print("   브레이커 연속 3번 실패 → 1초 쉼, 장애 때 실패까지 0.3초. 호출 상한", CLAUDE_CALL_LIMIT, "번")
    print(f"   브레이커 연속 3번 실패 → {cooldown:g}초 쉼, 장애 때 실패까지 0.3초. 호출 상한 {CLAUDE_CALL_LIMIT}번")  # [NEW]
    CountedBack.total = 0
    CountedBack.seconds = []

    def chain():
        # 이름은 장애 주입과 브레이커가 쓰는 키(fake, claude)다
        return mf.FallbackChatModel(["fake", "claude"], [get_model("fake"), CountedBack(get_model(back))])

    mf.reset()
    call(chain(), 10)
    s = mf.STATS
    rec("A 정상: 앞 모델만 쓴다", s["served:fake"] == 10 and s["served:claude"] == 0 and s["error:fake"] == 0,
        f"fake {s['served:fake']}회, claude {s['served:claude']}회, 실패 {s['error:fake']}회")

    os.environ["MODEL_FORCE_DOWN"] = "fake"
    mf.reset()
    t_on = call(chain(), 10)
    s = dict(mf.STATS)
    rec("B 앞 모델 장애: 모두 뒤 모델이 답한다", s.get("served:claude", 0) == 10, f"claude {s.get('served:claude', 0)}/10")
    rec("B 브레이커: 3번 실패 뒤에는 앞 모델을 건너뛴다",
        s.get("error:fake") == 3 and s.get("skipped:fake") == 7 and mf.breaker_state("fake") == "open",
        f"실패 {s.get('error:fake')}회, 건너뜀 {s.get('skipped:fake')}회, 상태 {mf.breaker_state('fake')}")
    err_on = s.get("error:fake", 0)

    os.environ["MODEL_BREAKER_THRESHOLD"] = "1000"
    mf.reset()
    t_off = call(chain(), 10)
    err_off = mf.STATS["error:fake"]
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    rec("B 브레이커 효과: 없을 때보다 빠르다", t_on < t_off,
        f"브레이커 있음 {t_on:.2f}초, 없음 {t_off:.2f}초 (10건, 뒤 모델 응답 시간 포함). "
        f"앞 모델 실패 {err_on}회 대 {err_off}회(실패 대기 0.3초 x 횟수 = {err_on * 0.3:.1f}초 대 {err_off * 0.3:.1f}초)")

    mf.reset()
    call(chain(), 5)
    time.sleep(mf.cooldown() + 0.1)
    os.environ["MODEL_FORCE_DOWN"] = "fake"
    before = mf.STATS["error:fake"]
    call(chain(), 2)
    rec("D 쉬는 시간 뒤 한 번만 다시 시도, 또 실패하면 다시 쉰다",
        mf.STATS["error:fake"] - before == 1 and mf.breaker_state("fake") == "open",
        f"다시 시도 {mf.STATS['error:fake'] - before}번, 상태 {mf.breaker_state('fake')}")

    mf.reset()
    call(chain(), 5)
    time.sleep(mf.cooldown() + 0.1)
    os.environ["MODEL_FORCE_DOWN"] = ""
    call(chain(), 3)
    rec("C 복구: 쉬는 시간 뒤 다시 시도해 성공하면 앞 모델로 돌아간다",
        mf.STATS["served:fake"] == 3 and mf.breaker_state("fake") == "closed",
        f"fake {mf.STATS['served:fake']}회, 상태 {mf.breaker_state('fake')}")

    # 장애 주입은 모델을 부르기 전에 막으므로 이 단계는 Claude 를 부르지 않는다
    os.environ["MODEL_FORCE_DOWN"] = "fake,claude"
    mf.reset()
    try:
        call(chain(), 1)
        rec("E 모두 장애: 예외를 낸다", False, "예외 없음")
    except mf.ModelDownError as e:
        rec("E 모두 장애: 마지막 모델의 예외를 낸다", True, str(e))
    os.environ["MODEL_FORCE_DOWN"] = ""

    sec = CountedBack.seconds
    if sec:
        print(f"\n== 뒤 모델 {'(가짜)' if dry else '(실제 Claude)'} 호출 {CountedBack.total}번, "
              f"호출당 평균 {sum(sec) / len(sec):.2f}초, 최소 {min(sec):.2f}초, 최대 {max(sec):.2f}초")
    else:
        print("\n== 뒤 모델 호출 0번")


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


# [NEW] 앞 모델 fake(장애 주입) + 뒤 모델 실제 Claude 를 run_agent 전체로 n건 돌려 질문당 시간을 잰다.
# 실제 단계 ①②③ 과 같은 질문, 같은 방식(질문당 run_agent 시간)이라 나란히 비교할 수 있다.
# dry 이면 뒤 모델을 fake-b(가짜)로 바꿔 실제 모델을 부르지 않는다. claude_limit 은 뒤 모델 호출 상한이다.
def run_fake_claude_graph(n: int, dry: bool, claude_limit: int, cooldown: float = 30.0) -> None:
    if not dry and not os.getenv("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY 가 없다(.env 확인). 건너뛴다")
        return
    os.environ["MODEL_FAULT_INJECTION"] = "true"
    os.environ["MODEL_FORCE_DOWN"] = "fake"
    os.environ["MODEL_DOWN_DELAY_SEC"] = "0.3"
    os.environ["MODEL_BREAKER_THRESHOLD"] = "3"
    os.environ["MODEL_BREAKER_COOLDOWN_SEC"] = f"{cooldown:g}"
    back = "fake-b" if dry else "claude"
    provider = f"fake>{back}"
    print(f"\n== fake>claude 질문 {n}건 (뒤 모델 {'가짜(dry)' if dry else '실제 Claude'}, 쉬는 시간 {cooldown:g}초, 뒤 모델 호출 상한 {claude_limit}번)")
    mf.reset()
    times, ok, errs = [], 0, []
    for i in range(n):
        served = sum(v for k, v in mf.STATS.items() if k.startswith("served:") and not k.endswith(":fake"))
        if served >= claude_limit:
            errs.append(f"뒤 모델 호출 상한({claude_limit}번)에 닿아 {i}건에서 멈춤")
            break
        q = QUESTIONS[i % len(QUESTIONS)]
        t0 = time.perf_counter()
        try:
            ans = run_agent(q, provider)
            # [OLD] if str(ans).strip(): ok += 1 / else: errs.append("빈 답")
            # [NEW] 도구가 실패해 그래프가 포기한 답(give_up 문구)은 성공으로 세지 않는다
            if not str(ans).strip():
                errs.append("빈 답")
            elif "요청을 처리하지 못했습니다" in str(ans):
                errs.append(f"{i + 1}번째 질문이 포기 문구로 끝남(도구 실패)")
            else:
                ok += 1
        except Exception as e:
            errs.append(f"{type(e).__name__}: {str(e)[:60]}")
        times.append(time.perf_counter() - t0)
    if times:
        s = sorted(times)
        mid = (s[len(s) // 2] + s[(len(s) - 1) // 2]) / 2
        detail = (f"성공 {ok}/{len(times)}, 질문당 평균 {sum(times) / len(times):.2f}초, 중앙값 {mid:.2f}초, 최대 {max(times):.2f}초, "
                  f"30초 이상 {sum(t >= 30 for t in times)}건, 100초 이상 {sum(t >= 100 for t in times)}건, 모델 호출 기록 {dict(mf.STATS)}")
        if errs:
            detail += f", 메모 {errs[:3]}"
        rec(f"fake>claude {len(times)}건", ok == len(times) and len(times) == n, detail)
        print("   질문별 시간(초):", ", ".join(f"{t:.2f}" for t in times))
    else:
        rec("fake>claude", False, f"실행된 질문 없음 {errs[:3]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", action="store_true")
    ap.add_argument("--n", type=int, default=3)
    # --real 에서 한 단계만 돌린다. 없으면 ①②③ 모두
    ap.add_argument("--only", choices=["gemini", "claude", "fallback"], help="--real 에서 이 단계만 실행")
    # [NEW] 앞 모델은 가짜(장애 주입), 뒤 모델은 실제 Claude. --dry 는 뒤 모델을 가짜로 바꿔 Claude 를 부르지 않고 흐름만 본다
    ap.add_argument("--fake-claude", action="store_true")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--cooldown", type=float, default=1.0, help="--fake-claude 의 브레이커 쉬는 시간(초). 기본 1.0")
    # [NEW] 앞 모델 fake + 뒤 모델 실제 Claude 를 run_agent 전체로 --n 건 돌린다(--claude-limit 은 뒤 모델 호출 상한)
    ap.add_argument("--fake-claude-graph", action="store_true")
    ap.add_argument("--claude-limit", type=int, default=50)
    args = ap.parse_args()
    print("== Day 36 모델 폴백 + 서킷 브레이커 점검")
    if args.fake_claude_graph:
        run_fake_claude_graph(max(1, args.n), args.dry, args.claude_limit, args.cooldown if args.cooldown != 1.0 else 30.0)
        fails = [r for r in rows if not r[1]]
        print(f"\n== 요약: PASS {len(rows) - len(fails)}, FAIL {len(fails)}")
        print("== 결과:", "PASS" if not fails else "FAIL")
        sys.exit(1 if fails else 0)
    # [OLD] if args.real:
    # [OLD]     run_real(max(1, args.n), args.only)
    # [OLD] else:
    # [OLD]     run_fake()
    # [NEW] --fake-claude 를 먼저 본다. 나머지는 이전과 같다
    if args.fake_claude:
        try:
            run_fake_claude(args.dry, args.cooldown)
        except Exception as e:  # 중간에 멈춰도 쓴 호출 횟수는 남긴다
            rec("fake>claude 시험이 중간에 멈춤", False, f"{type(e).__name__}: {str(e)[:80]}, 뒤 모델 호출 {CountedBack.total}번")
    elif args.real:
        run_real(max(1, args.n), args.only)
    else:
        run_fake()
    fails = [r for r in rows if not r[1]]
    print(f"\n== 요약: PASS {len(rows) - len(fails)}, FAIL {len(fails)}")
    print("== 결과:", "PASS" if not fails else "FAIL")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
