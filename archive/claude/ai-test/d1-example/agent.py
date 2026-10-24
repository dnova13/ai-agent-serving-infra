"""
ReAct 패턴을 보여주는 간단한 AI 에이전트 샘플.

Gemini의 함수 호출(function calling) 기능을 이용해
"생각(Thought) -> 행동(Action) -> 관찰(Observation)" 루프를 직접 구현합니다.
개념 설명은 CONCEPTS.md를 참고하세요.
"""

import ast
import json
import operator
import os
from datetime import datetime

from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

# google-genai SDK는 GOOGLE_API_KEY를 GEMINI_API_KEY보다 우선해서 사용합니다.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

MODEL = "gemini-3.6-flash"

# ---------------------------------------------------------------------------
# 1. 에이전트가 사용할 도구(Tool) 정의
# ---------------------------------------------------------------------------

# 안전한 사칙연산 계산기 (eval 대신 AST를 직접 순회해서 임의 코드 실행을 막음)
_ALLOWED_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
}


def _safe_eval(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _ALLOWED_OPERATORS:
        return _ALLOWED_OPERATORS[type(node.op)](
            _safe_eval(node.left), _safe_eval(node.right)
        )
    if isinstance(node, ast.UnaryOp) and type(node.op) in _ALLOWED_OPERATORS:
        return _ALLOWED_OPERATORS[type(node.op)](_safe_eval(node.operand))
    raise ValueError(f"허용되지 않은 수식입니다: {ast.dump(node)}")


def calculator(expression: str) -> str:
    tree = ast.parse(expression, mode="eval")
    result = _safe_eval(tree.body)
    return str(result)


def get_current_time() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 실제 검색 API 대신, 개념 이해를 위한 가짜 지식베이스
_KNOWLEDGE_BASE = {
    "nestjs": "NestJS는 Node.js 기반의 서버 프레임워크로, Angular와 유사한 모듈/DI 구조를 가집니다.",
    "fastapi": "FastAPI는 Python 기반의 고성능 웹 프레임워크로, 타입 힌트 기반 자동 문서화를 지원합니다.",
    "react": "React는 컴포넌트 기반 UI 라이브러리로, Meta(구 Facebook)에서 개발했습니다.",
    "langchain": "LangChain은 LLM 애플리케이션 개발을 돕는 프레임워크로, Chain/Agent/Tool 개념을 제공합니다.",
}


def search_knowledge_base(query: str) -> str:
    query_lower = query.lower()
    for key, value in _KNOWLEDGE_BASE.items():
        if key in query_lower:
            return value
    return "지식베이스에서 관련 정보를 찾지 못했습니다."


TOOL_IMPLEMENTATIONS = {
    "calculator": calculator,
    "get_current_time": get_current_time,
    "search_knowledge_base": search_knowledge_base,
}

# Gemini에게 알려줄 도구 스펙 (JSON Schema)
TOOLS = types.Tool(
    function_declarations=[
        types.FunctionDeclaration(
            name="calculator",
            description="사칙연산(+, -, *, /, **) 수식을 계산합니다. 예: '(12 + 8) * 3'",
            parameters_json_schema={
                "type": "object",
                "properties": {
                    "expression": {"type": "string", "description": "계산할 수식 문자열"}
                },
                "required": ["expression"],
            },
        ),
        types.FunctionDeclaration(
            name="get_current_time",
            description="현재 날짜와 시간을 반환합니다.",
            parameters_json_schema={"type": "object", "properties": {}},
        ),
        types.FunctionDeclaration(
            name="search_knowledge_base",
            description="미리 준비된 지식베이스에서 키워드(nestjs, fastapi, react, langchain 등)로 정보를 검색합니다.",
            parameters_json_schema={
                "type": "object",
                "properties": {"query": {"type": "string", "description": "검색어"}},
                "required": ["query"],
            },
        ),
    ]
)


def execute_tool(name: str, tool_input: dict) -> str:
    """도구 이름에 맞는 함수를 실행하고 결과를 문자열로 반환합니다."""
    fn = TOOL_IMPLEMENTATIONS.get(name)
    if fn is None:
        return f"알 수 없는 도구입니다: {name}"
    try:
        return fn(**tool_input)
    except Exception as exc:  # 도구 실행 실패를 모델에게 그대로 관찰 결과로 알려주기 위함
        return f"도구 실행 중 오류 발생: {exc}"


# ---------------------------------------------------------------------------
# 2. ReAct 루프: 생각(Thought) -> 행동(Action) -> 관찰(Observation) 반복
# ---------------------------------------------------------------------------


def run_agent(user_message: str, max_turns: int = 8) -> str:
    client = genai.Client(api_key=GEMINI_API_KEY)
    contents = [types.Content(role="user", parts=[types.Part(text=user_message)])]

    config = types.GenerateContentConfig(
        system_instruction=(
            "당신은 도구를 활용해 문제를 해결하는 에이전트입니다. "
            "계산이 필요하면 calculator를, 현재 시간이 필요하면 get_current_time을, "
            "특정 기술 용어에 대한 정보가 필요하면 search_knowledge_base를 사용하세요. "
            "도구 없이 답할 수 있으면 바로 답하세요."
        ),
        tools=[TOOLS],
    )

    print(f"\n사용자: {user_message}\n")

    for _ in range(max_turns):
        response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=config,
        )

        candidate = response.candidates[0]
        parts = candidate.content.parts or []

        # 이번 턴에서 모델이 생성한 텍스트(=생각/중간 설명)를 먼저 출력
        for part in parts:
            if part.text and part.text.strip():
                print(f"[생각] {part.text.strip()}")

        function_calls = [part.function_call for part in parts if part.function_call]

        if not function_calls:
            # 더 이상 도구를 쓰지 않으면 최종 답변으로 판단하고 루프 종료
            final_text = next((p.text for p in parts if p.text), "")
            print(f"\n[최종 답변] {final_text}\n")
            return final_text

        # 모델의 응답(도구 호출 포함)을 대화 이력에 추가
        contents.append(candidate.content)

        # 도구 호출 실행 -> 관찰(Observation) 생성
        response_parts = []
        for call in function_calls:
            print(f"[행동] {call.name}({json.dumps(call.args, ensure_ascii=False)})")
            observation = execute_tool(call.name, call.args)
            print(f"[관찰] {observation}")
            response_parts.append(
                types.Part(
                    function_response=types.FunctionResponse(
                        name=call.name,
                        response={"result": observation},
                    )
                )
            )

        # 관찰 결과를 다시 대화 이력에 추가하고 다음 턴으로
        contents.append(types.Content(role="user", parts=response_parts))

    return "최대 턴 수를 초과했습니다. (무한 루프 방지)"


if __name__ == "__main__":
    run_agent(
        "지금 몇 시야? 그리고 (37 + 5) * 2 는 얼마야? 참고로 langchain이 뭔지도 알려줘."
    )
