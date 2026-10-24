"""
멀티 LLM 프로바이더 추상화 모듈

- provider 문자열 하나만 바꾸면 모델이 교체됨
- 1주 차: Gemini, Claude 두 개만 우선 지원
- 나중에 OpenAI 등 추가하고 싶으면 elif 블록만 추가하면 됨
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LangChain 모델 래퍼 ─────────────────────────
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_anthropic import ChatAnthropic


def get_model(provider: str = "gemini"):
    """
    provider: "gemini" | "claude"
    """
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model="gemini-3.6-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.3,
        )

    elif provider == "claude":
        return ChatAnthropic(
            model="claude-3-5-haiku-20241022",  # 가볍고 저렴한 모델로 테스트용 선택
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
            temperature=0.3,
        )

    else:
        raise ValueError(f"지원하지 않는 provider입니다: {provider}")


if __name__ == "__main__":
    # 간단 테스트: 같은 질문을 두 모델에 각각 던져서 비교
    question = "백엔드 개발자가 Kafka를 배워야 하는 이유를 한 문장으로 설명해줘"

    for provider in ["gemini", "claude"]:
        print(f"\n===== {provider.upper()} =====")
        try:
            model = get_model(provider)
            response = model.invoke(question)
            print(response.content)
        except Exception as e:
            print(f"[{provider}] 호출 실패: {e}")