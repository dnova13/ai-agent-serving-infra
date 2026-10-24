# ai-test — ReAct 에이전트 샘플

Gemini API의 함수 호출(function calling) 기능으로 "생각 → 행동 → 관찰"을 반복하는
ReAct 스타일 에이전트를 직접 구현한 샘플입니다. 개념 설명은 [CONCEPTS.md](./CONCEPTS.md)를 참고하세요.

## 실행 방법

```bash
cd ai-test
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# .env 파일을 열어 GEMINI_API_KEY 값을 채워주세요 (https://aistudio.google.com/apikey 에서 발급)

python agent.py
```

## 무슨 일이 일어나는지

`agent.py`를 실행하면 다음 질문을 에이전트에게 던집니다.

> "지금 몇 시야? 그리고 (37 + 5) * 2 는 얼마야? 참고로 langchain이 뭔지도 알려줘."

콘솔에 아래와 같은 로그가 순서대로 찍히는 걸 볼 수 있습니다.

```
사용자: 지금 몇 시야? ...
[생각] ...
[행동] get_current_time({})
[관찰] 2026-08-28 14:03:11
[행동] calculator({"expression": "(37 + 5) * 2"})
[관찰] 84
[행동] search_knowledge_base({"query": "langchain"})
[관찰] LangChain은 LLM 애플리케이션 개발을 돕는 프레임워크로...
[최종 답변] ...
```

`agent.py` 맨 아래 `run_agent(...)` 호출의 문자열을 바꿔서 다른 질문도 시도해보세요.

## 다음 단계

- `TOOLS` 리스트에 새 도구를 추가하고 `TOOL_IMPLEMENTATIONS`에 실제 함수를 연결해보세요.
- 진짜 웹 검색이 필요하면 Gemini의 서버사이드 `google_search` 도구로 교체할 수 있습니다.
- 에이전트 루프를 직접 짜지 않고 Gemini SDK의 자동 함수 호출(automatic function calling) 기능을 써보는 것도 좋은 다음 실험입니다.
