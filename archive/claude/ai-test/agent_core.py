"""
범용(멀티 프로바이더) 에이전트 코어

Gemini Interactions API(구글 전용)에서 LangChain의 bind_tools() 방식으로 전환.
이러면 provider 파라미터 하나로 Gemini <-> Claude 전환 가능.

TODO(2주 차): search_vector_db 내부를 실제 Qdrant 호출로 교체
TODO(4주 차): Redis에 세션별 messages 히스토리 저장해서 대화 이어가기 구현
"""

import os
from dotenv import load_dotenv

from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_anthropic import ChatAnthropic

load_dotenv()


# ── 모델 프로바이더 추상화 ──────────────────────────
def get_model(provider: str = "gemini"):
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model="gemini-3.6-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
        )
    elif provider == "claude":
        return ChatAnthropic(
            model="claude-3-5-haiku-20241022",
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
        )
    else:
        raise ValueError(f"지원하지 않는 provider입니다: {provider}")


# ── 도구 정의 (LangChain @tool 데코레이터 - 프로바이더 무관 공통 정의) ──
@tool
def search_vector_db(query: str) -> str:
    """사내 문서/상품 리뷰 등에서 질문과 의미적으로 유사한 문서를 검색한다."""
    # TODO(2주 차): qdrant_client.search(...) 로 교체
    mock_results = [
        f"[mock 문서 1] '{query}' 관련 사내 규정: 환불은 구매 후 7일 이내 가능합니다.",
        f"[mock 문서 2] '{query}' 관련 리뷰: 배송이 빠르고 품질이 좋다는 평가가 많습니다.",
    ]
    return "\n".join(mock_results)


AVAILABLE_TOOLS = [search_vector_db]
TOOLS_BY_NAME = {t.name: t for t in AVAILABLE_TOOLS}


def run_agent(question: str, provider: str = "gemini") -> str:
    """
    provider: "gemini" | "claude" - 이 값만 바꾸면 모델 교체됨.
    """
    model = get_model(provider)
    model_with_tools = model.bind_tools(AVAILABLE_TOOLS)

    messages = [HumanMessage(content=question)]

    # 1차 호출: 모델이 도구가 필요한지 판단
    ai_response = model_with_tools.invoke(messages)
    messages.append(ai_response)

    # 도구 호출이 없으면 바로 답변 반환
    if not ai_response.tool_calls:
        return ai_response.content

    # 도구 호출 실행 → 결과를 ToolMessage로 이어붙임
    for tool_call in ai_response.tool_calls:
        tool_fn = TOOLS_BY_NAME.get(tool_call["name"])
        if tool_fn is None:
            result = f"알 수 없는 도구: {tool_call['name']}"
        else:
            result = tool_fn.invoke(tool_call["args"])

        messages.append(
            ToolMessage(content=result, tool_call_id=tool_call["id"])
        )

    # 2차 호출: 도구 실행 결과를 반영한 최종 답변
    final_response = model_with_tools.invoke(messages)
    return final_response.content


if __name__ == "__main__":
    # 같은 질문, 같은 도구를 두 프로바이더 모두에게 던져서 비교
    question = "환불 정책이 어떻게 되나요?"

    for provider in ["gemini", "claude"]:
        print(f"\n===== {provider.upper()} =====")
        try:
            answer = run_agent(question, provider=provider)
            print(answer)
        except Exception as e:
            print(f"[{provider}] 실패: {e}")
