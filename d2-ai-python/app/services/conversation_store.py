"""
대화 기록을 Redis 에 저장하고 불러오는 함수 (Day 20)

같은 sessionId 로 온 요청끼리 이전 대화를 이어가게 한다.

- 키: agent:conversation:<sessionId>   값: [{"role": "user"|"assistant", "content": "..."}, ...]  (JSON)
- 저장하는 것은 사용자 질문과 최종 답(텍스트)뿐이다. 도구 호출과 검색 결과(ToolMessage)는 저장하지 않는다.
  다음 질문에서 모델이 필요하면 도구를 다시 부르면 되고, 저장 크기와 직렬화가 단순해진다.
- 최근 MAX_TURNS 번의 주고받음(질문+답 한 쌍이 1턴)만 남긴다. 대화가 길어지면 LLM 에 보내는 양과 비용이 늘기 때문이다.
- 저장할 때마다 TTL(CONVERSATION_TTL_SEC, 기본 3600초)을 새로 건다. 마지막 대화 후 그 시간이 지나면 Redis 가 지운다.
  Day 24: 값은 ttl_policy.py 에서 정한다(슬라이딩 만료. 계속 대화하는 세션은 만료되지 않고 크기는 MAX_TURNS 로 제한된다).
- 같은 세션에 요청이 동시에 오면 읽고 쓰는 사이에 서로의 턴을 덮어쓸 수 있다(읽기-수정-쓰기 경합). 막는 것은 Day 21 의 락이다.
  이 파일은 일부러 막지 않는다. Day 22 에서 락이 없을 때 실제로 유실되는지 본다.
- Redis 를 쓸 수 없으면(예외) 호출한 쪽이 대화 이어가기만 포기하고 답은 계속 하도록 여기서는 예외를 그대로 올린다.
  (agent_graph.run_agent 에서 잡아 경고만 남긴다.)
"""

import os
from typing import Optional

from app.services import redis_client as rc
from app.services import ttl_policy
MAX_TURNS = int(os.getenv("CONVERSATION_MAX_TURNS", "10"))
# Day 24: 같은 환경변수(CONVERSATION_TTL_SEC)와 기본값(3600)을 ttl_policy 가 읽는다. 0 이하면 시작할 때 예외
TTL_SEC = ttl_policy.CONVERSATION_TTL_SEC


def conversation_key(session_id: str) -> str:
    return rc.make_key("conversation", session_id)


def load_history(session_id: str) -> list[dict]:
    """저장된 대화(오래된 것부터). 없으면 빈 리스트."""
    # get_json 은 키가 없거나 TTL 로 만료됐으면 None 을 주므로 `or []` 로 "처음 보는 세션"과 같이 취급한다
    return rc.get_json(conversation_key(session_id)) or []


def append_turn(session_id: str, question: str, answer: str) -> list[dict]:
    """질문과 답 한 쌍을 대화 끝에 붙여 저장하고, 저장된 전체 대화를 돌려준다. 최근 MAX_TURNS 턴만 남긴다."""
    # 흐름: (1) 지금 저장된 대화를 읽는다 → (2) 이번 질문과 답을 붙인다 → (3) 오래된 턴을 자른다 → (4) 통째로 다시 쓴다
    # 읽기(1)와 쓰기(4) 사이에 다른 요청이 끼면 그 요청의 턴이 덮어써진다(락이 없는 지금 구조의 한계, Day 21~22)
    history = load_history(session_id)                                  # (1)
    history.append({"role": "user", "content": question})               # (2)
    history.append({"role": "assistant", "content": answer})
    history = history[-MAX_TURNS * 2:]                                  # (3) 질문+답이 한 턴이라 *2. 뒤에서부터 자른다
    rc.set_json(conversation_key(session_id), history, ttl_sec=TTL_SEC)  # (4) 쓸 때마다 TTL 이 새로 시작된다
    return history


def clear(session_id: str) -> bool:
    """대화를 지운다(새로 시작). 지운 것이 있으면 True."""
    return rc.delete(conversation_key(session_id))


def ttl_left(session_id: str) -> Optional[int]:
    """남은 만료 시간(초). 대화가 없으면 None."""
    return rc.ttl_sec(conversation_key(session_id))
