"""
Qdrant 사용 여부 비교 (Day 10)

같은 질문을 두 방식으로 답하게 해서 Qdrant(RAG)의 효과를 눈으로 비교한다.

  without_rag : 채팅 모델 혼자 답한다 (검색 없음). 모델이 아는 일반 지식으로만 답하므로
                이 쇼핑몰의 실제 규정을 모른 채 그럴듯하게 지어낼 수 있다.
  with_rag    : 질문과 가까운 문서를 Qdrant 에서 찾아 프롬프트에 붙여서 답한다.
                문서에 있는 내용만 근거로 답하게 하고, 없으면 "없다"고 답하게 한다.

왜 에이전트(agent_graph)를 쓰지 않고 따로 만들었나
  - 에이전트는 모델이 도구를 쓸지 스스로 판단해서, 같은 질문에도 검색을 할 때도 있고 안 할 때도 있다.
    "검색 있음/없음"을 정확히 비교하려면 두 경로를 코드로 고정해야 한다.
  - 각 방식이 채팅 모델을 딱 1번만 호출하므로 무료 한도(429)에 덜 걸리고,
    호출 횟수와 시간 차이도 그대로 비교할 수 있다. (에이전트는 도구 판단 + 최종 답변으로 2번)

공정한 비교를 위해 두 방식 모두 같은 모델, 같은 역할 설정(상담원)을 쓰고
RAG 쪽에만 "참고 문서"와 "문서에 근거해서만 답하라"는 규칙을 더했다.
"""

import os
import time

from langchain_core.messages import HumanMessage, SystemMessage

from app.services.agent_graph import get_model
from app.services.qdrant_search import format_for_llm, search

# 두 방식 공통 역할 설정
SYSTEM_BASE = "당신은 쇼핑몰 고객센터 상담원입니다. 한국어로 3문장 이내로 간결하게 답하세요."

# RAG 쪽에만 추가하는 규칙. "문서에 없으면 없다고 답하라"가 있어야 문서 밖 내용을 지어내지 않는다.
RAG_RULE = (
    "아래 [참고 문서]에 있는 내용만 근거로 답하세요. "
    "문서에 답이 없으면 '문서에 해당 내용이 없습니다'라고 답하세요."
)


def _text(content) -> str:
    """응답 content 를 문자열로. Claude 는 [{"type":"text","text":...}] 블록 리스트로 올 수 있다."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content)


def _safe_error(e: Exception) -> str:
    """에러를 짧게 줄이고, 혹시 메시지에 섞인 API 키 값은 가린다."""
    msg = f"{type(e).__name__}: {e}"
    for env in ("GOOGLE_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        val = os.getenv(env)
        if val:
            msg = msg.replace(val, "***")
    msg = " ".join(msg.split())
    return msg[:300] + ("..." if len(msg) > 300 else "")


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


def answer_without_rag(question: str, provider: str) -> dict:
    """검색 없이 채팅 모델 혼자 답한다. (채팅 모델 호출 1번)"""
    t0 = time.perf_counter()
    try:
        resp = get_model(provider).invoke(
            [SystemMessage(content=SYSTEM_BASE), HumanMessage(content=question)]
        )
        return {"answer": _text(resp.content).strip(), "elapsed_ms": _ms(t0), "llm_calls": 1}
    except Exception as e:  # 한쪽이 실패해도 다른 쪽 결과는 보여주려고 에러를 값으로 돌려준다
        return {"error": _safe_error(e), "elapsed_ms": _ms(t0), "llm_calls": 1}


def answer_with_rag(question: str, provider: str, limit: int = 3) -> dict:
    """Qdrant 에서 가까운 문서를 찾아 프롬프트에 붙여 답한다. (임베딩 1번 + 검색 + 채팅 모델 호출 1번)"""
    t0 = time.perf_counter()
    sources = None
    retrieval_ms = None
    try:
        # 1) 검색: 질문 임베딩(Gemini API) + Qdrant 검색. 이 시간이 RAG 가 추가로 드는 비용이다.
        t1 = time.perf_counter()
        hits = search(question, limit=limit)
        retrieval_ms = _ms(t1)
        sources = hits  # [{score, doc_id, category, source, text}, ...]

        # 2) 찾은 문서를 프롬프트에 붙여서 채팅 모델에게 답하게 한다.
        prompt = f"[참고 문서]\n{format_for_llm(hits)}\n\n[질문]\n{question}"
        resp = get_model(provider).invoke(
            [SystemMessage(content=SYSTEM_BASE + "\n" + RAG_RULE), HumanMessage(content=prompt)]
        )
        return {
            "answer": _text(resp.content).strip(),
            "elapsed_ms": _ms(t0),
            "llm_calls": 1,
            "retrieval_ms": retrieval_ms,
            "sources": sources,
        }
    except Exception as e:
        # 검색까지는 성공하고 채팅 모델에서 실패했다면 sources 는 남겨서 보여준다.
        return {
            "error": _safe_error(e),
            "elapsed_ms": _ms(t0),
            "llm_calls": 1,
            "retrieval_ms": retrieval_ms,
            "sources": sources,
        }


def compare(question: str, provider: str = "gemini", limit: int = 3) -> dict:
    """두 방식을 차례로 실행해서 결과를 나란히 돌려준다."""
    return {
        "query": question,
        "provider": provider,
        "without_rag": answer_without_rag(question, provider),
        "with_rag": answer_with_rag(question, provider, limit),
    }


if __name__ == "__main__":
    import json
    import sys

    q = " ".join(sys.argv[1:]) or "환불 정책이 어떻게 되나요?"
    print(json.dumps(compare(q), ensure_ascii=False, indent=2))
