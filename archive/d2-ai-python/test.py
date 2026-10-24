"""
Gemini Interactions API 응용 예제

원본 예시(날씨 함수)는 '함수 호출 요청이 왔다'까지만 확인하고 끝나는 반쪽짜리 코드였음.
여기서는:
  1) 도구 정의 (Qdrant 벡터 검색 - 실제 프로젝트에서 쓸 RAG Tool)
  2) 모델이 도구 호출을 요청하면
  3) 실제로 그 함수를 실행하고
  4) 결과를 previous_interaction_id로 이어서 모델에게 되돌려줘서
  5) 최종 답변을 받는 전체 루프까지 구현

주의: Interactions API는 Google 전용 인터페이스라 Claude/OpenAI로 그대로
포팅은 안 됨. 그래서 "멀티 모델 라우팅"이 목적이면 이 방식 대신
LangGraph + langchain 래퍼(model_provider.py) 조합을 메인으로 유지하고,
이 스크립트는 "Gemini 네이티브 기능 실험" 용도로 별도 사용 추천.
"""

import os
from dotenv import load_dotenv
from google import genai

load_dotenv()

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# ── 1) 도구 정의: Qdrant 벡터 검색 (실제 프로젝트의 RAG Tool Node에 해당) ──
search_vector_db = {
    "type": "function",
    "name": "search_vector_db",
    "description": "사내 문서/상품 리뷰 등에서 질문과 의미적으로 유사한 문서를 검색한다.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "검색할 질문 또는 키워드",
            },
        },
        "required": ["query"],
    },
}


def execute_search_vector_db(query: str) -> str:
    """
    실제로는 여기서 Qdrant client.search()를 호출해야 함.
    지금은 2주 차 전이라 Qdrant 연동 전이므로 mock 데이터로 대체.
    2주 차 되면 이 함수 내용만 Qdrant 실제 호출로 교체하면 됨.
    """
    # TODO(2주 차): qdrant_client.search(collection_name=..., query_vector=..., limit=3) 로 교체
    mock_results = [
        f"[mock 문서 1] '{query}' 관련 사내 규정: 환불은 구매 후 7일 이내 가능합니다.",
        f"[mock 문서 2] '{query}' 관련 리뷰: 배송이 빠르고 품질이 좋다는 평가가 많습니다.",
    ]
    return "\n".join(mock_results)


def run_agent(question: str):
    # ── 2) 모델에게 질문 + 사용 가능한 도구 목록 전달 ──
    interaction = client.interactions.create(
        model="gemini-3.6-flash",
        input=question,
        tools=[search_vector_db],
    )

    # ── 3) 모델이 도구 호출을 요청했는지 확인 ──
    function_call_step = None
    for step in interaction.steps:
        if step.type == "function_call":
            function_call_step = step
            print(f"[Agent] 도구 호출 요청: {step.name}({step.arguments})")
            break

    # 도구 호출 없이 바로 답변한 경우 (RAG 불필요 판단)
    if function_call_step is None:
        print("[Agent] 도구 호출 없이 바로 답변")
        return interaction.output_text

    # ── 4) 실제로 함수 실행 ──
    if function_call_step.name == "search_vector_db":
        result = execute_search_vector_db(**function_call_step.arguments)
    else:
        result = f"알 수 없는 함수: {function_call_step.name}"

    print(f"[Agent] 검색 결과:\n{result}\n")

    # ── 5) 실행 결과를 이전 interaction에 이어붙여서 최종 답변 요청 ──
    #     (previous_interaction_id로 서버 사이드 상태 유지 - 대화 히스토리 재전송 불필요)
    final_interaction = client.interactions.create(
        model="gemini-3.6-flash",
        previous_interaction_id=interaction.id,
        input=[
            {
                "type": "function_call_output",
                "call_id": function_call_step.call_id,
                "output": result,
            }
        ],
    )

    return final_interaction.output_text


if __name__ == "__main__":
    question = "환불 정책이 어떻게 되나요?"
    answer = run_agent(question)
    print("\n===== 최종 답변 =====")
    print(answer)