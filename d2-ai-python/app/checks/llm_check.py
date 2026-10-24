"""
LLM 연결 확인 + 간단 테스트 (Gemini / GPT / Claude)

각 provider 에 아주 짧은 질문을 한 번 보내서
  - API 키가 설정돼 있는지
  - 실제로 응답이 오는지, 얼마나 걸리는지
  - (옵션) 도구 호출(tool calling)이 되는지
를 한 번에 확인한다. 프로젝트의 에이전트 코드(agent_graph.py)는 건드리지 않는 독립 스크립트.

결과 표시
  OK    : 응답 받음
  SKIP  : API 키가 비어 있거나 패키지가 없어서 시도하지 않음 (에러 아님)
  FAIL  : 호출했는데 실패 (한도 초과, 모델명 오류, 인증 오류 등)

실행 (프로젝트 루트에서)
  python -m app.checks.llm_check                 # 3개 모두
  python -m app.checks.llm_check gemini          # 하나만
  python -m app.checks.llm_check gpt      # 골라서
  python -m app.checks.llm_check gemini gpt      # 골라서
  python -m app.checks.llm_check --tools         # 도구 호출 테스트까지

모델명은 환경변수로 바꿀 수 있다 (.env 에 추가): GEMINI_MODEL / ANTHROPIC_MODEL / OPENAI_MODEL
주의: 호출할 때마다 토큰이 소모되고 무료 한도(429)에 걸릴 수 있다. 질문은 일부러 아주 짧게 했다.
"""

import os
import sys
import time

from dotenv import load_dotenv
from langchain_core.tools import tool

load_dotenv()

PROMPT = "숫자 1 더하기 1은? 숫자만 답해."
TIMEOUT_SEC = 30  # 응답이 안 오면 30초 뒤 FAIL 처리 (무한 대기 방지)


# ── 도구 호출 테스트용 도구 (실제로 실행하진 않고, 모델이 호출을 "요청"하는지만 본다) ──
@tool
def get_current_time() -> str:
    """현재 날짜와 시간을 반환한다. 시간/날짜를 묻는 질문에만 사용."""
    return "2026-01-01 00:00:00"


TOOL_PROMPT = "지금 몇 시야?"


# ── provider 별 모델 생성 ─────────────────────────────
# 키가 없으면 (None, 사유) 를 돌려줘서 SKIP 처리한다. 패키지는 필요할 때만 import 한다.
# max_retries=0 : 실패하면 바로 보여주기 위해 자동 재시도를 끈다 (429 에서 오래 매달리지 않게)
def build_gemini():
    key = os.getenv("GOOGLE_API_KEY")
    if not key:
        return None, "GOOGLE_API_KEY 가 비어 있음"
    from langchain_google_genai import ChatGoogleGenerativeAI

    name = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")
    return ChatGoogleGenerativeAI(
        model=name, google_api_key=key, timeout=TIMEOUT_SEC, max_retries=0
    ), name


def build_claude():
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return None, "ANTHROPIC_API_KEY 가 비어 있음 (API 비용 때문에 일부러 비워 둔 상태)"
    from langchain_anthropic import ChatAnthropic

    name = os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")
    return ChatAnthropic(
        model=name, anthropic_api_key=key, timeout=TIMEOUT_SEC, max_retries=0
    ), name


def build_gpt():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return None, "OPENAI_API_KEY 가 비어 있음"
    try:
        from langchain_openai import ChatOpenAI
    except ModuleNotFoundError:
        return None, "langchain-openai 미설치 (pip install langchain-openai)"

    name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    return ChatOpenAI(
        model=name, api_key=key, timeout=TIMEOUT_SEC, max_retries=0
    ), name


BUILDERS = {"gemini": build_gemini, "claude": build_claude, "gpt": build_gpt}
KEY_ENVS = ["GOOGLE_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"]


def _text(content) -> str:
    """응답 content 를 문자열로. Claude 는 [{"type":"text","text":...}] 블록 리스트로 올 수 있다."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content)


def _safe(msg: str) -> str:
    """에러 메시지를 짧게 줄이고, 혹시 포함된 API 키 값은 가린다."""
    for env in KEY_ENVS:
        val = os.getenv(env)
        if val:
            msg = msg.replace(val, "***")
    msg = " ".join(msg.split())
    return msg[:200] + ("..." if len(msg) > 200 else "")


def _hint(msg: str) -> str:
    """자주 나는 에러에 대한 한 줄 힌트."""
    m = msg.lower()
    if "429" in m or "resource_exhausted" in m or "quota" in m or "rate" in m:
        return "→ 무료 한도 초과(429). 잠시 후 재시도하거나 다른 모델/티어 사용"
    if "404" in m or "not found" in m or "not_found" in m:
        return "→ 모델명이 없거나 종료됨. 환경변수로 다른 모델명 지정"
    if "401" in m or "403" in m or "api key" in m or "authentication" in m or "permission" in m:
        return "→ API 키가 잘못됐거나 권한 없음"
    if "timeout" in m or "timed out" in m:
        return f"→ {TIMEOUT_SEC}초 안에 응답 없음. 네트워크/서버 상태 확인"
    return ""


def check(provider: str, with_tools: bool) -> str:
    """provider 하나를 점검하고 결과 한 줄을 출력. 반환: 'OK' | 'SKIP' | 'FAIL'"""
    try:
        model, info = BUILDERS[provider]()
    except Exception as e:  # import/생성 단계 에러
        print(f"[FAIL] {provider:<6} 모델 생성 실패: {_safe(str(e))}")
        return "FAIL"

    if model is None:
        print(f"[SKIP] {provider:<6} {info}")
        return "SKIP"

    # 1) 기본 응답 테스트
    try:
        t0 = time.perf_counter()
        resp = model.invoke(PROMPT)
        sec = time.perf_counter() - t0
        print(f"[ OK ] {provider:<6} {info} | {sec:.1f}초 | 응답: {_text(resp.content).strip()[:40]!r}")
    except Exception as e:
        msg = str(e)
        print(f"[FAIL] {provider:<6} {info} | {type(e).__name__}: {_safe(msg)}")
        hint = _hint(msg)
        if hint:
            print(f"         {hint}")
        return "FAIL"

    # 2) (옵션) 도구 호출 테스트: 모델이 get_current_time 호출을 요청하면 통과
    if with_tools:
        try:
            t0 = time.perf_counter()
            resp = model.bind_tools([get_current_time]).invoke(TOOL_PROMPT)
            sec = time.perf_counter() - t0
            calls = [c["name"] for c in (resp.tool_calls or [])]
            ok = "get_current_time" in calls
            print(f"         도구 호출 {'OK  ' if ok else 'NO  '}| {sec:.1f}초 | 요청한 도구: {calls or '없음'}")
        except Exception as e:
            print(f"         도구 호출 FAIL | {type(e).__name__}: {_safe(str(e))}")
            return "FAIL"

    return "OK"


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    with_tools = "--tools" in sys.argv
    targets = args or list(BUILDERS)

    unknown = [a for a in targets if a not in BUILDERS]
    if unknown:
        sys.exit(f"알 수 없는 provider: {unknown} (사용 가능: {list(BUILDERS)})")

    print(f"질문: {PROMPT!r}" + (f" / 도구 테스트 질문: {TOOL_PROMPT!r}" if with_tools else ""))
    print("-" * 60)
    results = {p: check(p, with_tools) for p in targets}
    print("-" * 60)
    print("요약: " + ", ".join(f"{p}={r}" for p, r in results.items()))
