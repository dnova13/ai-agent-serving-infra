"""
가짜 LLM (Day 25) — 부하 테스트용

k6 로 수백 건을 보내면 실제 Gemini/Claude 를 부를 수 없다(무료 한도 소진, 비용, 429). 그래서 provider="fake" 일 때
모델 호출을 "일정 시간 기다린 뒤 고정 문장을 돌려주는 가짜"로 바꾼다. 서버, Kafka, Redis, 워커 구조는 그대로 쓴다.

- 켜는 방법: 환경변수 FAKE_LLM_ENABLED=true 로 서버(워커)를 띄우고, 요청의 provider 를 "fake" 로 보낸다.
  꺼져 있으면 provider="fake" 는 지원하지 않는 provider 로 거절된다(실수로 켜지지 않게).
- 지연: FAKE_LLM_DELAY_SEC(기본 5) 를 기준으로 FAKE_LLM_JITTER(기본 0.2) 비율만큼 위아래로 흔든다. 5초, 0.2 이면 4~6초.
  time.sleep 이라서 실제 모델 호출처럼 그 스레드를 막는다(비동기로 기다리지 않는다).
- 도구는 부르지 않는다(tool_calls 없음). 그래서 임베딩 API 와 Qdrant 는 부하 테스트에 끼지 않고, LLM 호출은 1번이다.
  실제 에이전트는 보통 LLM 2번 + 검색 1번이므로 건당 시간이 이보다 길다.
- 지연 값은 실측이 아니라 설계값이다. 실제 응답 시간은 Day 16 에서 7.6초 한 번, Day 17 에서 22~29초가 나왔다.
"""
import os
import random
import time
import uuid  # Day 31: 가짜 도구 호출의 id

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage  # Day 31: FakeAgentChatModel 이 대화를 읽으려고 추가


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    value = float(raw)
    if value < 0:
        raise ValueError(f"{name} 는 0 이상이어야 한다: {raw}")
    return value


def enabled() -> bool:
    return os.getenv("FAKE_LLM_ENABLED", "false").lower() == "true"


class FakeChatModel:
    """agent_node 가 쓰는 두 메서드(bind_tools, invoke)만 가진 모델 흉내."""

    def __init__(self, label: str = "fake"):  # Day 36: 폴백 시험에서 어느 모델이 답했는지 구분하려고 이름표를 받는다
        self.label = label

    def bind_tools(self, tools):
        return self  # 도구를 쓰지 않으므로 그대로 돌려준다

    def invoke(self, messages):
        base = _float_env("FAKE_LLM_DELAY_SEC", 5.0)
        jitter = _float_env("FAKE_LLM_JITTER", 0.2)
        delay = max(0.0, base * (1 + random.uniform(-jitter, jitter)))
        time.sleep(delay)
        return AIMessage(content=f"[{self.label}] 가짜 응답입니다 (지연 {delay:.1f}초)")


# Day 31: 도구를 부르고, 입력 오류가 나면 고쳐서 다시 부르는 가짜 모델 (provider="fake-agent", Self-Correction 확인용)
# FakeChatModel 은 도구를 안 불러서 재시도와 정정 경로가 한 번도 실행되지 않는다. 이 모델은 대화를 보고 정해진 대로 움직인다.
#   - 질문(사람 메시지)이 마지막이면: 질문 그대로 search_vector_db 를 부른다
#   - 도구 결과가 마지막이면:
#       "[정정 안내]" 가 있으면 검색어에서 "교정테스트" 를 빼고 다시 부른다("교정불가" 는 못 고치는 모델 흉내라 그대로 다시 부른다)
#       안내 없는 실패면 사과로 끝낸다(원래 모델의 사과 종료를 흉내)
#       성공이면 결과 길이를 적은 한 문장으로 답한다
# 지연은 FakeChatModel 과 같은 FAKE_LLM_DELAY_SEC, FAKE_LLM_JITTER 를 쓴다. 호출마다 기다린다.
class FakeAgentChatModel:
    TOOL_NAME = "search_vector_db"

    def bind_tools(self, tools):
        return self

    @staticmethod
    def _tool_call(query: str) -> AIMessage:
        return AIMessage(
            content="",
            tool_calls=[{"name": FakeAgentChatModel.TOOL_NAME, "args": {"query": query}, "id": f"fake-{uuid.uuid4().hex[:8]}"}],
        )

    def invoke(self, messages):
        base = _float_env("FAKE_LLM_DELAY_SEC", 5.0)
        jitter = _float_env("FAKE_LLM_JITTER", 0.2)
        time.sleep(max(0.0, base * (1 + random.uniform(-jitter, jitter))))

        last = messages[-1]
        question = next((str(m.content) for m in reversed(messages) if isinstance(m, HumanMessage)), "")
        if isinstance(last, ToolMessage):
            text = str(last.content)
            if "[도구 실행 실패]" in text:
                if "[정정 안내]" in text:
                    return self._tool_call(" ".join(question.replace("교정테스트", " ").split()))
                return AIMessage(content="[fake-agent] 도구 실패로 답하지 못했습니다")
            return AIMessage(content=f"[fake-agent] 검색 결과 {len(text)}자를 받아 답했습니다")
        return self._tool_call(question)
