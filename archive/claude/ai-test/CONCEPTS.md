# AI 에이전트 개념 정리

`agent.py` 코드와 짝지어서 읽으면 이해가 빠릅니다.

## 1. 에이전트란?

목표를 받으면 스스로 **판단 → 도구 실행 → 결과 확인**을 반복하면서 작업을 끝내는 소프트웨어입니다.
한 번의 질문-답변으로 끝나는 일반 챗봇과 달리, 여러 단계를 스스로 계획하고 실행합니다.

## 2. ReAct 패턴 (Reasoning + Acting)

가장 기본이 되는 에이전트 아키텍처입니다.

```
생각(Thought) → 행동(Action) → 관찰(Observation) → 다시 생각 → ... → 최종 답변
```

`agent.py`의 `run_agent()` 함수가 이 루프를 그대로 구현합니다.

| ReAct 개념 | 코드 위치 | 설명 |
|---|---|---|
| 생각 (Thought) | `for part in parts: if part.text` | Gemini가 다음에 뭘 할지 설명하는 텍스트 |
| 행동 (Action) | `part.function_call` → `execute_tool(...)` | Gemini가 선택한 도구를 실제로 실행 |
| 관찰 (Observation) | `types.Part(function_response=...)` | 도구 실행 결과를 다시 대화에 추가 |
| 반복 | `for _ in range(max_turns):` | 모델이 더 이상 `function_call`을 반환하지 않을 때까지 반복 |
| 종료 조건 | `if not function_calls: ...return` | Gemini가 더 이상 도구가 필요없다고 판단하면 종료 |

핵심은 **API 자체는 상태가 없다(stateless)**는 것입니다. 매 요청마다 지금까지의 대화 전체
(`messages` 리스트)를 다시 보내야 하고, 에이전트 코드가 이 리스트를 계속 누적시켜주는 역할을 합니다.

## 3. 도구(Tool) 정의 3요소

`agent.py`의 `TOOLS` 리스트를 보면 도구 하나가 세 부분으로 이루어집니다.

1. **name** — Claude가 호출할 때 쓰는 식별자
2. **description** — Claude가 "이 도구를 언제 써야 하는지" 판단하는 근거 (프롬프트나 마찬가지로 중요)
3. **input_schema** — 도구가 받을 파라미터의 JSON Schema (타입, 필수 여부 등)

Gemini는 이 스펙만 보고 "지금 이 도구를 호출해야겠다"를 판단합니다. 실제 함수 실행은
API가 하는 게 아니라 **내 코드(`execute_tool`)가 직접 수행**하고, 그 결과만 다시 Gemini에게 돌려줍니다.
즉 Gemini는 "무엇을, 어떤 인자로 호출할지"만 결정하고, 실행 권한과 책임은 개발자 쪽에 있습니다.

## 4. 다른 아키텍처 패턴들 (참고)

- **Plan-and-Execute**: 전체 계획을 먼저 다 세운 뒤 하나씩 순서대로 실행. 단계가 명확한 복잡한 작업에 유리.
- **Multi-Agent**: 역할이 다른 여러 에이전트가 서로 결과를 주고받으며 협업 (예: 리서치 에이전트 + 작성 에이전트 + 검토 에이전트).
- **Manual Loop vs. Automatic Function Calling**: `agent.py`는 루프를 직접 짠 "Manual Loop" 방식입니다. Gemini SDK에는
  파이썬 함수를 그대로 전달하면 이 루프를 대신 돌려주는 자동 함수 호출(automatic function calling) 기능도 있어서, 개념을 이해한 뒤엔
  실무에서는 이쪽을 더 많이 씁니다.

## 5. LangChain은 이 코드와 뭐가 다른가?

한 문장으로: **LangChain은 `agent.py`에서 직접 짠 부분들(도구 스펙 변환, 루프, 대화 이력 관리)을
미리 만들어서 제공하는 라이브러리**입니다.

| `agent.py`에서 직접 구현한 것 | LangChain에서는 |
|---|---|
| `TOOLS` 리스트 + `execute_tool()` 분기문 | `@tool` 데코레이터로 함수 하나만 정의하면 자동 등록 |
| `for _ in range(max_turns): ...` 루프 | `AgentExecutor`가 대신 돌려줌 |
| `contents` 리스트를 직접 append | `Memory` 컴포넌트가 자동 관리 |
| Gemini API만 사용 | OpenAI, Claude, Gemini 등 여러 모델을 같은 인터페이스로 교체 가능 |

즉 LangChain을 배우기 전에 `agent.py`처럼 직접 루프를 한 번 짜보면,
LangChain이 내부적으로 뭘 대신 해주는지 훨씬 명확하게 감이 잡힙니다.
이 프로젝트는 그 "감 잡기"를 위한 로우레벨(raw) 버전이라고 보면 됩니다.

## 6. 이 샘플의 한계 (일부러 단순화한 부분)

- 도구가 3개뿐이고 전부 로컬 함수 (실제 웹 검색/DB 조회 없음)
- 에러 처리가 최소화되어 있음 (실무에서는 재시도, 타임아웃 등 추가 필요)
- 병렬 도구 호출은 지원하지만 별도로 시각화하지는 않음 (Claude가 한 번에 여러 도구를 부를 수도 있음)
- 사람 승인(human-in-the-loop) 단계 없음 — 실무 에이전트에서는 위험한 행동 전에 확인을 받는 경우가 많음
