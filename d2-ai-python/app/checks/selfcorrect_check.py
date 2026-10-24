"""
Self-Correction 동작 확인 (Day 31)

가짜 모델(provider="fake-agent")과 가짜 검색(Qdrant, 임베딩을 부르지 않음)으로 에이전트 그래프를 돌려,
도구 실패가 일시 장애와 입력 오류로 나뉘어 처리되는지 시나리오별로 확인하고 PASS/FAIL 을 출력한다.
Kafka, Redis, Qdrant, Gemini 가 모두 필요 없고 대기 시간도 없다(지연 0초).

시나리오 (질문에 들어 있는 단어와 가짜 검색의 상태로 실패를 만든다)
  A 정상                         실패 없음
  B 일시 장애 1번 후 성공        가짜 검색이 처음 1번 연결 실패 → 같은 호출 재실행으로 복구
  C 일시 장애가 계속             가짜 검색이 항상 연결 실패 → 재시도 한도에서 포기
  D 입력 오류 후 정정 성공       질문에 "교정테스트" → 모델이 단어를 빼고 다시 부름
  E 입력 오류를 못 고침          질문에 "교정불가" → 모델이 못 고쳐 정정 한도에서 포기
  F 강제실패 (Day 6 회귀)        질문에 "강제실패" → 예전처럼 같은 호출을 재실행하다 포기
  G 정정 뒤 일시 장애            D 에 가짜 검색 연결 실패 1번을 겹침

실행 (d2-ai-python 에서, 파이썬 가상환경 켠 터미널):
    python -m app.checks.selfcorrect_check
"""

import contextlib
import io
import os
import sys

# 가짜 모델은 환경변수로 켠다. 그래프를 불러오기 전에 정해 둔다(지연은 0초).
os.environ["FAKE_LLM_ENABLED"] = "true"
os.environ["FAKE_LLM_DELAY_SEC"] = "0"
os.environ["FAKE_LLM_JITTER"] = "0"

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from prometheus_client import REGISTRY

from app.services import agent_graph as ag
class FakeSearch:
    """Qdrant 검색을 대신한다. 처음 fail_times 번은 연결 실패(일시 장애)를 낸다. 음수면 항상 실패."""

    def __init__(self) -> None:
        self.fail_times = 0
        self.calls = 0

    def search(self, query, limit=3):
        self.calls += 1
        if self.fail_times < 0 or self.calls <= self.fail_times:
            raise ConnectionError("Qdrant 연결 실패(가짜)")
        return [{"text": f"[가짜 문서] {query}"}]

    @staticmethod
    def format_for_llm(hits):
        return "\n".join(h["text"] for h in hits)


# (이름, 질문, 가짜 검색 실패 횟수, 기대: 도구 실행, 모델 호출, retry, correction, 답에 있어야 할 문구)
SCENARIOS = [
    ("A 정상",                    "환불 정책 알려줘",              0, 1, 2, 0, 0, "받아 답했습니다"),
    ("B 일시 장애 1번 후 성공",   "환불 정책 알려줘",              1, 2, 2, 1, 0, "받아 답했습니다"),
    ("C 일시 장애가 계속",        "환불 정책 알려줘",             -1, 2, 1, 2, 0, "2번 시도했지만"),
    ("D 입력 오류 후 정정 성공",  "교정테스트 환불 정책 알려줘",   0, 2, 3, 0, 1, "받아 답했습니다"),
    ("E 입력 오류를 못 고침",     "교정불가 환불 정책 알려줘",     0, 3, 3, 0, 3, "검색 조건을 바꿔 2번"),
    ("F 강제실패 (Day 6 회귀)",   "강제실패 환불 정책 알려줘",     0, 2, 1, 2, 0, "2번 시도했지만"),
    ("G 정정 뒤 일시 장애",       "교정테스트 환불 정책 알려줘",   1, 3, 3, 1, 1, "받아 답했습니다"),
]


def metric(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def run_one(question: str) -> dict:
    state = {
        "messages": [HumanMessage(content=question)],
        "provider": "fake-agent",
        "retry_count": 0,
        "pending_tool_calls": [],
        "correction_count": 0,
        "last_failure_kind": "",
    }
    with contextlib.redirect_stdout(io.StringIO()):  # 그래프의 [DEBUG], [timing] 출력은 숨긴다
        final = ag.AGENT_GRAPH.invoke(state)
    msgs = final["messages"]
    tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
    # give_up 노드가 만든 사과 문구는 모델 호출이 아니다
    ai_msgs = [m for m in msgs if isinstance(m, AIMessage) and not str(m.content).startswith("죄송합니다")]
    return {
        "tool_runs": len(tool_msgs),
        "llm_calls": len(ai_msgs),
        "retry": final["retry_count"],
        "correction": final.get("correction_count", 0),
        "answer": str(msgs[-1].content),
        "tool_msgs": [str(m.content) for m in tool_msgs],
    }


def main() -> int:
    fake = FakeSearch()
    ag.qdrant_search = fake.search            # search_vector_db 가 부르는 이름을 바꿔 끼운다
    ag.format_for_llm = fake.format_for_llm

    fails = 0
    rows = []
    print("== 시나리오별 결과 (도구 실행 / 모델 호출 / retry / correction)")
    for name, question, fail_times, e_tool, e_llm, e_retry, e_corr, e_text in SCENARIOS:
        fake.fail_times, fake.calls = fail_times, 0
        r = run_one(question)
        got = (r["tool_runs"], r["llm_calls"], r["retry"], r["correction"])
        exp = (e_tool, e_llm, e_retry, e_corr)
        ok = got == exp and e_text in r["answer"]
        fails += 0 if ok else 1
        rows.append((name, ok, got, exp, r))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: 실제 {got}, 기대 {exp}")
        if not ok:
            print(f"         답: {r['answer'][:80]}")

    print("== 정정 안내가 모델에게 전달됐는가")
    for name, ok, got, exp, r in rows:
        if name.startswith("D") or name.startswith("G"):
            first = r["tool_msgs"][0]
            has = "[정정 안내]" in first and "교정테스트" in first and "남은 정정 기회: 2번" in first
            fails += 0 if has else 1
            print(f"  [{'PASS' if has else 'FAIL'}] {name}: 첫 실패 결과에 원인, 정정 안내, 남은 기회 2번이 있음")
    e_row = next(x for x in rows if x[0].startswith("E"))
    left = [("남은 정정 기회: 2번" in t, "남은 정정 기회: 1번" in t, "[정정 안내]" in t) for t in e_row[4]["tool_msgs"]]
    ok_e = left[0][0] and left[1][1] and not left[2][2]
    fails += 0 if ok_e else 1
    print(f"  [{'PASS' if ok_e else 'FAIL'}] E: 남은 기회가 2번 → 1번으로 줄고, 마지막(포기) 결과에는 안내가 없음")

    print("== 지표 합계 (7개 시나리오 전체)")
    tool = "search_vector_db"
    expected = {
        "도구 실패 transient": (metric("agent_tool_failures_total", tool=tool, kind="transient"), 6),
        "도구 실패 input":     (metric("agent_tool_failures_total", tool=tool, kind="input"), 5),
        "복구 retry":          (metric("agent_tool_recoveries_total", via="retry"), 2),
        "복구 correction":     (metric("agent_tool_recoveries_total", via="correction"), 1),
        "포기 retry_exhausted":      (metric("agent_give_ups_total", reason="retry_exhausted"), 2),
        "포기 correction_exhausted": (metric("agent_give_ups_total", reason="correction_exhausted"), 1),
    }
    for label, (got, exp) in expected.items():
        ok = got == exp
        fails += 0 if ok else 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}: 실제 {got:g}, 기대 {exp}")

    print("== 요약")
    print(f"  {'모든 항목 PASS' if fails == 0 else f'FAIL {fails}개'}")
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
