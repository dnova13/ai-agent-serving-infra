"""
Day 11 에이전트 통합 테스트 (엔드투엔드)

서버를 띄우지 않고 LangGraph 에이전트를 직접 호출한다.
(HTTP 계층은 Day 10 의 /rag/* 와 /agent/run 으로 이미 확인했으므로 여기서는 에이전트 동작만 본다.)

각 케이스마다 세 가지를 확인한다.
  1) 도구 선택   : 기대한 도구를 호출했는가 (검색이 필요 없는 질문은 호출하지 않았는가)
  2) 문서 반영   : 답변에 "DB 문서에만 있는 값"이 들어 있는가 (mock/일반 상식으로는 나올 수 없는 값)
  3) 시간/호출수 : 질문 1개에 걸린 시간과 모델 호출 수

- 실행: python -m app.checks.agent_e2e            (프로젝트 루트 d2-ai-python 에서)
        python -m app.checks.agent_e2e --provider gemini --sleep 5
- 질문 1개당 임베딩 + 채팅 호출이 여러 번 일어난다. 무료 한도(429)를 피하려고 케이스 사이에 --sleep 초 쉰다.
- 판정: PASS / FAIL / INFO(참고용, 합격 여부 없음)
"""

import argparse
import time

from app.services.agent_graph import AGENT_GRAPH, MAX_RETRIES

from langchain_core.messages import HumanMessage

# (이름, 질문, 기대 도구, 답변에 들어가야 할 값들(하나라도), 비고)
#  기대 도구: "search_vector_db" / "get_current_time" / None(도구 호출 없음)
#  값 목록이 비어 있으면 문서 반영 검사는 하지 않는다.
CASES = [
    ("환불-변심", "단순 변심으로 환불하면 배송비는 누가 내요?", "search_vector_db", ["왕복", "고객"], "refund-001"),
    ("환불-하자", "불량품이 왔어요. 며칠 안에 환불되나요?", "search_vector_db", ["30일"], "refund-002"),
    ("배송-제주", "제주도 배송비는 얼마예요?", "search_vector_db", ["3,000"], "ship-002"),
    ("결제-할부", "무이자 할부는 얼마 이상부터 되나요?", "search_vector_db", ["5만원", "5만 원"], "pay-002"),
    ("회원-VIP", "VIP 등급 조건이 뭐예요?", "search_vector_db", ["100만"], "member-003"),
    ("회원-비번", "비번 까먹었어요", "search_vector_db", ["비밀번호 찾기", "재설정"], "member-001"),
    ("일반-인사", "안녕!", None, [], "도구 호출 없이 답해야 함"),
    # Day 11 에서 발견: 시스템 프롬프트가 없으면 아래 질문에 일반 상식으로 길게 답했다(tools=[]).
    ("범위밖-역사", "세종대왕이 누구야?", None, ["고객센터", "쇼핑몰", "어렵"], "쇼핑몰과 무관 -> 도구 없이 거절 안내"),
    ("범위밖-점심", "점심 메뉴 추천해줘", None, ["고객센터", "쇼핑몰", "어렵"], "쇼핑몰과 무관 -> 도구 없이 거절 안내"),
    ("시간", "지금 몇 시야?", "get_current_time", [], "get_current_time 호출"),
    ("문서없음", "포인트 적립률은 몇 퍼센트예요?", "search_vector_db", [], "INFO: 문서에 없는 질문. 수치를 지어내는지 눈으로 확인"),
    # 주의: 도구는 "모델이 넘긴 검색어"에 강제실패가 들어 있을 때만 실패한다.
#  질문 뒤에 괄호로 붙이면 모델이 검색어에서 빼 버려 실패가 안 일어난다(Day 11 첫 실행에서 FAIL).
#  그래서 검색어 자체가 강제실패가 되도록 질문한다. 그래도 모델 판단에 따라 달라질 수 있다.
    ("강제실패", "강제실패 라는 키워드로 검색해서 알려줘", "search_vector_db", ["죄송"], f"재시도 {MAX_RETRIES}번 후 give_up"),
]


def used_tools(messages) -> list[str]:
    """대화 기록에서 모델이 요청한 도구 이름을 순서대로 모은다."""
    names = []
    for m in messages:
        for call in getattr(m, "tool_calls", None) or []:
            names.append(call.get("name"))
    return names


def final_text(message) -> str:
    content = message.content
    if isinstance(content, list):
        return "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
    return content


def run_case(question: str, provider: str) -> dict:
    state = {
        "messages": [HumanMessage(content=question)],
        "provider": provider,
        "retry_count": 0,
        "pending_tool_calls": [],
    }
    t0 = time.perf_counter()
    try:
        out = AGENT_GRAPH.invoke(state)
    except Exception as e:  # 429, 네트워크 등. 키 값이 섞일 수 있어 메시지는 앞부분만 쓴다.
        return {"error": f"{type(e).__name__}: {str(e)[:150]}", "elapsed_ms": int((time.perf_counter() - t0) * 1000)}
    msgs = out["messages"]
    return {
        "answer": final_text(msgs[-1]),
        "tools": used_tools(msgs),
        "model_calls": sum(1 for m in msgs if m.type == "ai"),
        "retry_count": out.get("retry_count"),
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
    }


def judge(expect_tool, keywords, note, r) -> str:
    if "error" in r:
        return "FAIL"
    if note.startswith("INFO"):
        return "INFO"
    tool_ok = (expect_tool in r["tools"]) if expect_tool else (len(r["tools"]) == 0)
    kw_ok = (not keywords) or any(k in r["answer"] for k in keywords)
    return "PASS" if tool_ok and kw_ok else "FAIL"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", default="gemini")
    ap.add_argument("--sleep", type=float, default=5.0, help="케이스 사이 대기(초). 무료 한도 보호")
    args = ap.parse_args()

    rows = []
    for i, (name, q, expect_tool, keywords, note) in enumerate(CASES):
        r = run_case(q, args.provider)
        verdict = judge(expect_tool, keywords, note, r)
        rows.append((name, verdict, r))
        print(f"\n[{verdict}] {name}  | {q}")
        if "error" in r:
            print(f"  error: {r['error']}")
        else:
            print(f"  tools={r['tools']} model_calls={r['model_calls']} retry={r['retry_count']} {r['elapsed_ms']}ms")
            print(f"  answer: {r['answer'][:200]}")
            print(f"  기대: tool={expect_tool}, 값={keywords or '-'}  ({note})")
        if i < len(CASES) - 1:
            time.sleep(args.sleep)

    print("\n" + "=" * 60)
    print(f"{'케이스':<10} {'판정':<5} {'도구':<28} {'호출':<4} {'ms':>7}")
    for name, verdict, r in rows:
        if "error" in r:
            print(f"{name:<10} {verdict:<5} {'(error)':<28} {'-':<4} {r['elapsed_ms']:>7}")
        else:
            print(f"{name:<10} {verdict:<5} {','.join(r['tools']) or '-':<28} {r['model_calls']:<4} {r['elapsed_ms']:>7}")
    judged = [v for _, v, _ in rows if v != "INFO"]
    print(f"\nPASS {judged.count('PASS')} / {len(judged)}  (INFO 제외)")


if __name__ == "__main__":
    main()
