"""
LangGraph 기반 에이전트

흐름: agent(모델 호출) → 도구 요청이 있으면 tools(도구 실행) → 결과에 따라 agent, tools(재실행), give_up 중 하나로 간다.

- Day 3: LangGraph 로 전환. Day 6: 도구 실패 시 재시도(retry_count).
  모델에게 재시도를 맡기면 1번 실패 후 스스로 사과 답변을 만들어 버려서(실측 로그로 확인),
  실패하면 모델을 거치지 않고 그래프가 같은 도구 호출을 다시 실행하게 했다.
- Day 10: search_vector_db 가 실제 Qdrant 를 검색한다.
- Day 20, 21: session_id 가 있으면 Redis 의 이전 대화를 이어가고, 같은 세션은 락으로 한 번에 하나씩 처리한다.
- Day 31: 실패를 둘로 나눈다. 일시 장애(연결 끊김 등)는 같은 호출을 재실행하고,
  입력 오류(ToolInputError, 없는 도구, 인자 검증 실패)는 실패 원인을 모델에게 돌려줘 입력을 고쳐 다시 부르게 한다.
- Day 36: provider 를 "gemini>claude" 처럼 ">" 로 이으면 모델 폴백(services/model_fallback.py).
- Day 43: 실행 경로와 시간 로그를 print 에서 logging(로거 agent_graph)으로 바꿨다.
  [timing] 줄은 INFO, 분기 확인용 [DEBUG] 줄은 DEBUG 이다. AGENT_LOG_LEVEL=DEBUG 로 켜면 [DEBUG] 줄도 나온다.
"""

import os
import sys
import time  # Day 18: 단계별 시간 측정용
import logging  # Day 20: Redis 장애 시 경고만 남기고 계속하려고
from dotenv import load_dotenv
from typing import Annotated, TypedDict
from pydantic import ValidationError  # Day 31: 도구 인자 검증 실패를 입력 오류로 분류하려고

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, ToolMessage, AIMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_anthropic import ChatAnthropic

# Day 10: 도구가 실제 Qdrant 를 검색하도록 Day 9 의 검색 함수를 가져온다.
# search 는 이름이 너무 일반적이라 qdrant_search 로 바꿔서 가져온다.
from app.services.qdrant_search import search as qdrant_search, format_for_llm
from app.services import fake_llm
from app.services import metrics
from app.services import model_fallback
load_dotenv()


class _StdoutHandler(logging.StreamHandler):
    """지금의 sys.stdout 에 쓴다. 확인 스크립트가 contextlib.redirect_stdout 으로 출력을 숨길 수 있게 하려는 것."""

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, _value):
        pass


# Day 43: print 대신 쓰는 로거. 형식은 메시지만이라 예전 print 출력과 모양이 같다.
_log = logging.getLogger("agent_graph")
if not _log.handlers:
    _h = _StdoutHandler()
    _h.setFormatter(logging.Formatter("%(message)s"))
    _log.addHandler(_h)
    _log.propagate = False
_log.setLevel(os.getenv("AGENT_LOG_LEVEL", "INFO").upper())

MAX_RETRIES = 2  # 도구 실행 실패 시 최대 재시도 횟수
# Day 31: 입력 오류일 때 모델이 입력을 고쳐 다시 부를 수 있는 최대 횟수(도구 호출은 처음 1번 + 정정 2번 = 최대 3번)
MAX_CORRECTIONS = 2


class ToolInputError(ValueError):
    """Day 31: 도구에 넘긴 입력 자체가 문제인 실패. 같은 입력으로 다시 실행해도 소용없어서,
    실패 원인을 모델에게 알려 입력을 고쳐 다시 부르게 한다(일시 장애와 구분). 메시지는 모델이 읽으므로 원인과 고칠 방향을 적는다."""


# ── 1) State 정의 ──────────────────────────────

# pending_tool_calls 필드 추가.
# 이유: 실패해서 "tools" 노드를 다시 실행할 때, tool_node가
# state["messages"][-1]을 보면 그건 방금 실행한 ToolMessage(실패 결과)지
# 원래의 tool_calls 요청이 아니게 됨. 그래서 원본 요청을 별도로 들고 있어야
# "같은 요청을 그대로 재실행"할 수 있음.
# Day 31: correction_count(입력 오류 횟수), last_failure_kind(직전 실패 종류: "" / "transient" / "input") 추가.
# 노드에서는 state.get(...) 으로 읽어서, 이 필드가 없는 예전 초기 상태(agent_e2e.py)도 동작한다.
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    provider: str
    retry_count: int
    pending_tool_calls: list
    correction_count: int
    last_failure_kind: str


# ── 2) 도구 정의 ──────────────────────────────


# Day 10: 실제 Qdrant 검색 버전
# 바뀐 점
#  1) mock 고정 문구 대신 Day 9 의 search() 로 Qdrant 에서 의미가 가까운 문서 top-3 를 찾는다.
#  2) docstring 을 구체적으로 바꿈. docstring 은 모델이 "이 도구를 언제 쓸지" 판단하는 설명서라서,
#     어떤 종류의 문서가 들어 있는지(환불/배송/교환/결제/회원/후기)를 알려 줘야
#     모델이 관련 질문에서만 이 도구를 호출한다. ("안녕!", "지금 몇 시야?" 에는 호출하지 않아야 함)
#  3) "강제실패" 테스트 분기는 그대로 유지 (Day 6 재시도 로직 회귀 테스트용).
#  4) 예외를 잡지 않고 그대로 던진다. Qdrant 가 꺼져 있거나 임베딩 API 가 429 를 내면
#     tool_node 가 실패로 기록하고 Day 6 의 재시도(MAX_RETRIES)가 동작한다.
#     = 이제 재시도가 가짜 실패가 아니라 실제 일시적 장애에 대한 것이 된다.
@tool
def search_vector_db(query: str) -> str:
    """쇼핑몰 고객센터 문서(환불, 배송, 교환, 결제, 회원, 상품 후기)에서 질문과 의미가 비슷한 문서를 검색한다. 정책, 규정, 이용 방법, 후기에 대한 질문에 사용한다."""
    # 테스트 전용: 질문에 "강제실패"가 포함되면 일부러 예외를 던져서
    # retry_count 로직이 실제로 동작하는지 검증한다.
    if "강제실패" in query:
        raise RuntimeError("의도적으로 발생시킨 테스트용 실패 (retry_count 검증용)")

    # Day 31: 입력 오류(ToolInputError): 같은 입력으로 다시 실행해도 소용없는 실패. 모델이 입력을 고쳐 다시 부르게 한다.
    if not query or not query.strip():
        raise ToolInputError("검색어가 비어 있습니다. 질문의 핵심 키워드를 넣어 검색해야 합니다")
    # 테스트 전용: 이 단어가 검색어에 있으면 입력 오류. "교정테스트" 는 단어를 빼면 성공하고, "교정불가" 는 모델이 못 고치는 경우를 만들려는 것
    for _bad in ("교정테스트", "교정불가"):
        if _bad in query:
            raise ToolInputError(f"검색어에 '{_bad}' 가 들어 있어 검색할 수 없습니다. 이 단어를 뺀 질문의 핵심 키워드로 검색해야 합니다")

    # 질문 임베딩(Gemini API) -> Qdrant 검색 -> LLM 에게 줄 문자열로 변환
    # Day 18: 임베딩+Qdrant 검색 시간 측정
    _t0 = time.perf_counter()
    hits = qdrant_search(query, limit=3)
    _log.info("[timing] 임베딩+Qdrant 검색 %.2f초", time.perf_counter() - _t0)
    return format_for_llm(hits)


@tool
def get_current_time() -> str:
    """현재 날짜와 시간을 반환한다. 시간/날짜를 묻는 질문에만 사용."""
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Day 11: 에이전트의 역할과 범위를 정하는 시스템 프롬프트.
# 이게 없으면 모델이 "검색이 필요 없다"고 판단한 질문(세종대왕, 점심 메뉴 등)에 일반 상식으로 답해 버린다.
# 프롬프트는 강제가 아니라 유도라서 100% 보장은 아니다. state 에는 넣지 않고 모델 호출 직전에만 앞에 붙인다.
SYSTEM_PROMPT = (
    "당신은 쇼핑몰 고객센터 상담원입니다. 한국어로 간결하게 답하세요.\n"
    "- 환불, 배송, 교환, 결제, 회원, 상품 후기에 대한 질문은 반드시 search_vector_db 로 검색하고, "
    "검색된 문서에 있는 내용만 근거로 답하세요. 문서에 없으면 모른다고 답하고 고객센터 문의를 안내하세요.\n"
    "- 현재 날짜나 시간은 get_current_time 으로 확인하세요.\n"
    "- 인사 같은 가벼운 대화에는 짧게 답하세요.\n"
    "- 쇼핑몰 상담과 무관한 질문(역사, 요리, 투자, 코딩 등)에는 답하지 말고 "
    "'고객센터에서는 쇼핑몰 관련 문의만 도와드릴 수 있습니다'라고 안내하세요."
)

AVAILABLE_TOOLS = [search_vector_db, get_current_time]
TOOLS_BY_NAME = {t.name: t for t in AVAILABLE_TOOLS}


def get_model(provider: str):
    # Day 36: "gemini>claude" 처럼 ">" 로 이으면 왼쪽부터 시도하고 실패하면 다음 모델로 넘어간다 (services/model_fallback.py)
    if ">" in provider:
        names = [p.strip() for p in provider.split(">")]
        return model_fallback.FallbackChatModel(names, [get_model(n) for n in names])
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model="gemini-3.1-flash-lite",  # 무료 티어 한도가 더 넉넉해서 개발 중엔 이걸로 사용
            google_api_key=os.getenv("GOOGLE_API_KEY"),
        )
    elif provider == "claude":
        return ChatAnthropic(
            model="claude-haiku-5-5",  # Day 22: 현재 Haiku 모델명(API 문자열)
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
        )
    # Day 25: 부하 테스트용 가짜 모델. FAKE_LLM_ENABLED=true 일 때만 켜진다 (services/fake_llm.py)
    elif provider == "fake" and fake_llm.enabled():
        return fake_llm.FakeChatModel()
    # Day 31: 도구를 부르고 입력 오류가 나면 고쳐서 다시 부르는 가짜 모델(Self-Correction 확인용). 켜는 조건은 위와 같다
    elif provider == "fake-agent" and fake_llm.enabled():
        return fake_llm.FakeAgentChatModel()
    # Day 36: 폴백 시험에서 뒤 모델 역할을 하는 두 번째 가짜 모델. 답에 [fake-b] 가 붙는다
    elif provider == "fake-b" and fake_llm.enabled():
        return fake_llm.FakeChatModel("fake-b")
    else:
        raise ValueError(f"지원하지 않는 provider입니다: {provider}")


# ── 3) Node 정의 ──────────────────────────────

def agent_node(state: AgentState) -> dict:
    """'두뇌' 역할 Node. 도구가 필요한지 모델이 스스로 판단."""
    model = get_model(state["provider"])
    model_with_tools = model.bind_tools(AVAILABLE_TOOLS)

    # Day 11: 시스템 프롬프트를 맨 앞에 붙여서 호출
    # Day 18: LLM 호출 한 번이 몇 초인지 로그로 남긴다 (느린 원인 확인용)
    _t0 = time.perf_counter()
    response = model_with_tools.invoke([SystemMessage(content=SYSTEM_PROMPT)] + state["messages"])
    _log.info("[timing] LLM 호출(agent_node) %.2f초, 도구 요청 %d개", time.perf_counter() - _t0, len(getattr(response, "tool_calls", None) or []))

    # 모델이 도구를 요청했다면, 그 요청 내용(tool_calls)을 별도로 저장.
    # → 나중에 실패해서 "tools"를 재실행할 때 이 값을 그대로 재사용하기 위함.
    pending = response.tool_calls if getattr(response, "tool_calls", None) else []
    return {"messages": [response], "pending_tool_calls": pending}


def tool_node(state: AgentState) -> dict:
    """도구 실행 Node. 실행 중 예외가 발생하면 실패로 기록하고 retry_count를 1 증가시킨다."""

    # Day 31: 실패를 두 종류로 나눈다.
    #   input     : ToolInputError, 없는 도구, 인자 검증 실패 → 모델이 입력을 고쳐 다시 부르게 한다(correction_count 증가)
    #   transient : 그 밖의 예외(연결 끊김, 타임아웃, 429 등) → 지금처럼 같은 호출을 재실행한다(retry_count 증가)
    # 한 번에 여러 도구를 불러 종류가 섞이면 transient 로 본다(재실행은 이번에 부른 도구를 전부 다시 실행하므로).
    done = []  # (tool_call, content, kind) kind 는 성공이면 None
    for tool_call in state["pending_tool_calls"]:
        tool_fn = TOOLS_BY_NAME.get(tool_call["name"])
        kind = None
        try:
            if tool_fn is None:
                raise ToolInputError(f"알 수 없는 도구: {tool_call['name']}")
            _t0 = time.perf_counter()
            content = tool_fn.invoke(tool_call["args"])
            _log.info("[timing] 도구 %s %.2f초", tool_call["name"], time.perf_counter() - _t0)
        except (ToolInputError, ValidationError) as e:
            content = f"[도구 실행 실패] {e}"
            kind = "input"
        except Exception as e:
            content = f"[도구 실행 실패] {e}"
            kind = "transient"
        if kind:
            metrics.TOOL_FAILURES.labels(tool=tool_call["name"], kind=kind).inc()
        done.append((tool_call, content, kind))

    kinds = {k for _, _, k in done if k}
    failure_kind = "transient" if "transient" in kinds else ("input" if "input" in kinds else "")
    new_retry_count = state["retry_count"] + (1 if failure_kind == "transient" else 0)
    new_correction_count = state.get("correction_count", 0) + (1 if failure_kind == "input" else 0)

    # 입력 오류면 실패 원인 뒤에 정정 안내를 붙여 모델이 읽게 한다. 모델이 안 읽고 사과로 끝내면 그대로 종료된다(Day 6 에서 본 동작).
    corrections_left = max(0, MAX_CORRECTIONS - (new_correction_count - 1))
    results = []
    for tool_call, content, kind in done:
        if kind == "input" and failure_kind == "input" and corrections_left > 0:
            content += (
                "\n[정정 안내] 같은 입력으로 다시 호출하지 말고 위 실패 원인을 읽고 입력을 고쳐 다시 호출하세요. "
                f"남은 정정 기회: {corrections_left}번. 고칠 수 없으면 사용자에게 사정을 알리세요."
            )
        results.append(ToolMessage(content=content, tool_call_id=tool_call["id"]))

    # 직전에 실패했다가 이번에 모두 성공했으면 복구로 센다. 직전 실패가 일시 장애면 retry, 입력 오류면 correction
    if not kinds and state.get("last_failure_kind"):
        via = "retry" if state["last_failure_kind"] == "transient" else "correction"
        metrics.TOOL_RECOVERIES.labels(via=via).inc()

    return {
        "messages": results,
        "retry_count": new_retry_count,
        "correction_count": new_correction_count,
        "last_failure_kind": failure_kind,
    }


def give_up_node(state: AgentState) -> dict:
    """MAX_RETRIES를 넘겨도 실패하면 사용자에게 정직하게 알리고 종료."""
    # 최종 답변 문구만으로는 이 노드를 거쳤는지 알 수 없어서 실행 경로를 남긴다(AGENT_LOG_LEVEL=DEBUG 일 때)
    _log.debug(">>> [DEBUG] give_up_node 진입함 (retry_count 한계 도달, 강제 종료 처리)")

    # Day 31: 포기한 이유(일시 장애 재시도 소진 / 입력 정정 소진)에 따라 문구를 나누고 지표를 센다
    if state.get("last_failure_kind") == "input":
        reason = "correction_exhausted"
        text = (
            f"죄송합니다, 검색 조건을 바꿔 {MAX_CORRECTIONS}번 다시 시도했지만 요청을 처리하지 못했습니다. "
            "질문을 조금 바꿔서 다시 물어봐 주세요."
        )
    else:
        reason = "retry_exhausted"
        text = (
            f"죄송합니다, {MAX_RETRIES}번 시도했지만 요청을 처리하지 못했습니다. "
            "잠시 후 다시 시도해주시거나 다른 방식으로 질문해주세요."
        )
    metrics.GIVE_UPS.labels(reason=reason).inc()
    return {"messages": [AIMessage(content=text)]}


# ── 4) Conditional Edge ──────────────────────────
def should_continue(state: AgentState) -> str:
    """agent 실행 후: 도구 호출이 필요한가?"""
    last_message = state["messages"][-1]
    if getattr(last_message, "tool_calls", None):
        return "tools"
    return END


def after_tools_router(state: AgentState) -> str:
    """
    tools 실행 후 분기.

    Day 6 전에는 실패하면 무조건 "agent"로 돌려보내 모델에게 재시도를 맡겼지만, 모델이 1번 실패 후 바로 포기했다(실측 로그).

    실패 시 모델을 거치지 않고 "tools"로 직접 돌려보내 동일 요청을
    그래프가 결정적으로 재실행하게 함. 재시도 여력이 없으면 give_up,
    성공했으면 그제서야 "agent"로 보내 모델이 최종 답변을 작성하게 함.
    """
    last_tool_message = state["messages"][-1]
    failed = isinstance(last_tool_message, ToolMessage) and "[도구 실행 실패]" in str(
        last_tool_message.content
    )

    # 실행 경로(성공, 재시도, 정정, 포기)가 의도대로 갈리는지 확인하는 로그(AGENT_LOG_LEVEL=DEBUG 일 때)
    _log.debug(
        ">>> [DEBUG] after_tools_router 호출됨: failed=%s, retry_count=%s, correction_count=%s, kind=%s",
        failed, state["retry_count"], state.get("correction_count", 0), state.get("last_failure_kind", ""),
    )

    # Day 31: 입력 오류는 모델에게 돌려보내 입력을 고치게 하고(정정 한도를 넘으면 give_up), 일시 장애는 지금처럼 같은 호출을 재실행한다
    if failed:
        if state.get("last_failure_kind") == "input":
            if state.get("correction_count", 0) > MAX_CORRECTIONS:
                return "give_up"
            return "agent"
        if state["retry_count"] >= MAX_RETRIES:
            return "give_up"
        return "tools"  # 모델을 거치지 않고 같은 요청을 그대로 재실행
    return "agent"


# ── 5) 그래프 구성 및 컴파일 ──────────────────────
def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_node("give_up", give_up_node)

    graph.set_entry_point("agent")

    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})

    # tools -> tools(재시도) 자기 자신으로 돌아가는 엣지가 추가됨
    graph.add_conditional_edges(
        "tools", after_tools_router, {"tools": "tools", "agent": "agent", "give_up": "give_up"}
    )
    graph.add_edge("give_up", END)

    return graph.compile()


AGENT_GRAPH = build_graph()


# Day 20: 대화 기록 저장소(Redis)
from app.services import conversation_store
from app.services import redis_lock


def _history_to_messages(history: list[dict]) -> list:
    """Redis 에 저장된 [{"role","content"}] 를 LangChain 메시지로 바꾼다."""
    out = []
    for h in history:
        if h.get("role") == "user":
            out.append(HumanMessage(content=h["content"]))
        elif h.get("role") == "assistant":
            out.append(AIMessage(content=h["content"]))
    return out


# ── Day 20 흐름: ①~⑤ (Day 21 부터는 _run_agent_turn 이 실행하고, 락은 바깥의 run_agent 가 잡는다) ──
#  ① session_id 가 있으면 Redis 에서 이전 대화를 읽는다        (conversation_store.load_history)
#  ② 읽은 대화를 LangChain 메시지로 바꿔 이번 질문 앞에 붙인다  (_history_to_messages)
#  ③ 그래프를 돌린다: agent(LLM) → tools → agent(LLM). 모델은 [이전 대화 + 이번 질문] 을 본다
#  ④ 마지막 메시지에서 최종 답 텍스트를 꺼낸다
#  ⑤ 이번 질문과 답을 Redis 에 저장한다                        (conversation_store.append_turn)
#  Redis 오류는 ① 과 ⑤ 에서 각각 잡아 경고만 남긴다. 답은 항상 돌려준다(대화 이어가기만 포기).
#  session_id 가 없으면 ①②⑤ 를 건너뛰고 예전처럼 매번 새 대화다.
# ──────────────────────────────────────────────────────────────────────────────────

# ── Day 21 흐름: run_agent → (락) → _run_agent_turn ─────────────────────────────────────
#  같은 session_id 의 요청이 동시에 오면 둘 다 같은 이전 대화를 읽고(모델이 서로의 질문을 모름), 저장할 때 서로의 턴을
#  덮어쓸 수 있다. 그래서 session_id 가 있으면 그 세션의 락을 잡은 뒤 ①~⑤(아래 _run_agent_turn)를 통째로 실행한다.
#  - 락 이름: conversation:<sessionId>  (Redis 키 agent:lock:conversation:<sessionId>)
#  - 먼저 온 요청이 끝나(저장까지) 락을 풀어야 다음 요청이 대화를 읽으므로, 다음 요청은 앞 답까지 보고 이어서 답한다.
#  - session_id 가 다르면 락도 달라서 서로 기다리지 않는다. session_id 가 없으면 락 없이 예전처럼 동작한다.
#  - Redis 가 안 되면 락 없이 계속한다(fail_open). 대화 이어가기 자체도 Redis 라서 어차피 같이 안 된다.
#  - 오래 기다려도 못 잡으면 LockTimeout 이 올라온다(REDIS_LOCK_WAIT_SEC, 기본 100초).
#  - 환경변수 SESSION_LOCK_ENABLED=false 이면 락을 쓰지 않는다. Day 22 에서 락 유무를 비교하려는 스위치다.
# ──────────────────────────────────────────────────────────────────────────────────

SESSION_LOCK_ENABLED = os.getenv("SESSION_LOCK_ENABLED", "true").strip().lower() != "false"


# Day 21: run_agent 는 락만 잡고 _run_agent_turn 을 부른다. 호출하는 쪽(agent_handler, routers/agent.py)은 바뀌지 않는다.
def run_agent(question: str, provider: str = "gemini", session_id: str | None = None) -> str:
    if not session_id or not SESSION_LOCK_ENABLED:
        return _run_agent_turn(question, provider, session_id)
    with redis_lock.lock(f"conversation:{session_id}", fail_open=True):
        return _run_agent_turn(question, provider, session_id)


# ── Day 20 흐름: _run_agent_turn(question, provider, session_id) ───────────────────────
# Day 20: session_id 가 있으면 같은 세션의 이전 대화를 Redis 에서 불러와 이어서 답하고, 끝나면 이번 질문과 답을 저장한다.
#             없으면 예전처럼 매번 새 대화다.
# Day 21: 이름을 run_agent 에서 _run_agent_turn 으로 바꿈 (위의 run_agent 가 락을 잡고 이 함수를 부른다)
def _run_agent_turn(question: str, provider: str = "gemini", session_id: str | None = None) -> str:

    # Day 20: 이전 대화 불러오기. Redis 가 안 되면 경고만 남기고 이번 질문만으로 답한다.
    prior: list = []
    if session_id:
        try:
            # ① Redis 에서 읽고 ② 메시지 객체로 바꾼다. 처음 보는 세션이면 load_history 가 빈 리스트를 줘서 prior 도 빈 리스트다
            prior = _history_to_messages(conversation_store.load_history(session_id))
        except Exception as e:
            _log.warning("대화 기록을 못 불러옴 session=%s (%s: %s). 이번 질문만으로 답한다", session_id, type(e).__name__, e)
    _log.info("[timing] 이전 대화 %d개 메시지 session=%s", len(prior), session_id)

    # pending_tool_calls 초기값 추가
    initial_state: AgentState = {
        "messages": prior + [HumanMessage(content=question)],  # ② 이전 대화 뒤에 이번 질문을 붙인다 (모델이 보는 전체 입력)
        "provider": provider,
        "retry_count": 0,
        "pending_tool_calls": [],
    }

    # Day 18: 그래프 전체 시간
    _t0 = time.perf_counter()
    final_state = AGENT_GRAPH.invoke(initial_state)  # ③ agent ↔ tools 를 돌려 최종 답을 만든다 (여기가 20~30초)
    _log.info("[timing] 그래프 전체(run_agent) %.2f초", time.perf_counter() - _t0)
    last_message = final_state["messages"][-1]  # ④ 그래프의 마지막 메시지가 최종 답이다

    content = last_message.content
    if isinstance(content, list):
        texts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
        content = "\n".join(texts)

    # Day 20: 이번 질문과 답을 대화에 저장한다. 실패해도 답은 돌려준다.
    if session_id:
        try:
            # ⑤ 질문+답을 대화 끝에 붙여 저장한다 (최근 N턴만 유지, TTL 새로 건다). 도구 호출/검색 결과는 저장하지 않는다
            conversation_store.append_turn(session_id, question, str(content))
        except Exception as e:
            _log.warning("대화 기록을 못 저장함 session=%s (%s: %s)", session_id, type(e).__name__, e)
    return content


if __name__ == "__main__":
    test_questions = [
        # "환불 정책이 어떻게 되나요?",           # → search_vector_db 정상 호출
        # "지금 몇 시야?",                       # → get_current_time 정상 호출
        # "안녕!",                               # → 도구 없이 바로 답변
        # "환불 정책 강제실패 테스트해줘",         # → 모델이 "강제실패"를 개념 질문으로
        #                                          # 오해해서 도구를 아예 안 부름 (검증 실패)
        "환불 정책이 어떻게 되나요? (검색 키워드: 강제실패)"  # → 자연스러운 문장이라 도구가 호출됨
    ]

    for provider in ["gemini", "claude"]:
        print(f"\n{'#'*50}\nPROVIDER: {provider.upper()}\n{'#'*50}")
        for question in test_questions:
            print(f"\n{'='*50}\n질문: {question}\n{'='*50}")
            try:
                answer = run_agent(question, provider=provider)
                print(answer)
            except Exception as e:
                print(f"실패: {e}")