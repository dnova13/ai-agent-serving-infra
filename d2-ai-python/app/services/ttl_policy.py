"""
Redis 키별 만료(TTL) 정책 (Day 24)

Day 20~23 에서 키마다 흩어져 있던 만료 시간을 한곳에 모았다. 값을 바꿀 때는 이 파일(또는 환경변수)만 본다.

| 키                             | 만료                                  | 이유                                                                                   |
| ------------------------------ | ------------------------------------- | -------------------------------------------------------------------------------------- |
| agent:result:<id> PROCESSING   | RESULT_PROCESSING_TTL_SEC (기본 600)  | 워커가 죽으면 PROCESSING 이 영원히 남는다. 한 건은 최대 120초(AGENT_TIMEOUT_SEC)라 그보다 길게 |
| agent:result:<id> DONE         | RESULT_DONE_TTL_SEC (기본 86400)      | 클라이언트가 나중에 조회할 시간을 준다. Day 23 까지의 값을 그대로 유지                 |
| agent:result:<id> FAILED       | RESULT_FAILED_TTL_SEC (기본 86400)    | 실패 원인을 나중에 볼 수 있게 DONE 과 같게 둔다(따로 줄일 수 있게 변수는 분리)         |
| agent:conversation:<sessionId> | CONVERSATION_TTL_SEC (기본 3600)      | 마지막 대화 후 이 시간이 지나면 새 대화. 저장할 때마다 새로 계산한다(슬라이딩)         |
| agent:lock:<name>              | REDIS_LOCK_TTL_SEC (기본 150, redis_lock.py) | 락을 잡은 워커가 죽어도 풀리게. 한 건 최대 120초보다 길게. 이 파일에서 바꾸지 않는다 |

원칙
- Redis 에 쓰는 모든 `agent:*` 키는 TTL 이 있어야 한다. TTL 없는 키는 쌓이기만 한다(ttl_check.py 가 점검한다).
- 결과 키는 상태가 바뀔 때마다(PROCESSING → DONE) TTL 을 그 상태의 값으로 새로 건다. PROCESSING 의 짧은 TTL 이 DONE 으로 갱신되면서 길어진다.
- 대화는 슬라이딩이다. 계속 대화하는 세션은 만료되지 않고, 크기는 최근 MAX_TURNS 턴으로 제한된다(conversation_store.py).

이 값들은 설계값이다. 실제 사용 패턴이나 메모리 사용량을 재서 정한 것이 아니다.
"""

import os


def _positive_int(name: str, default: int) -> int:
    """환경변수를 양의 정수(초)로 읽는다. 0 이하이거나 숫자가 아니면 바로 알 수 있게 시작할 때 예외를 낸다."""
    raw = os.getenv(name)
    value = default if raw is None else int(raw)
    if value <= 0:
        raise ValueError(f"{name} 는 1 이상의 초 단위 정수여야 한다 (값: {value})")
    return value


RESULT_PROCESSING_TTL_SEC = _positive_int("RESULT_PROCESSING_TTL_SEC", 600)
# 예전 변수(RESULT_TTL_SEC, Day 23)를 쓰던 환경이 그대로 동작하도록 DONE 의 기본값으로 읽는다.
RESULT_DONE_TTL_SEC = _positive_int("RESULT_DONE_TTL_SEC", _positive_int("RESULT_TTL_SEC", 86400))
RESULT_FAILED_TTL_SEC = _positive_int("RESULT_FAILED_TTL_SEC", RESULT_DONE_TTL_SEC)
CONVERSATION_TTL_SEC = _positive_int("CONVERSATION_TTL_SEC", 3600)


def result_ttl(status: str) -> int:
    """결과 상태별 TTL(초). result_store 의 상태 문자열("PROCESSING", "DONE", "FAILED")과 같다."""
    # 모듈 변수를 호출할 때 읽으므로, 확인 스크립트가 값을 잠깐 바꿔서 만료를 시험할 수 있다.
    if status == "PROCESSING":
        return RESULT_PROCESSING_TTL_SEC
    if status == "FAILED":
        return RESULT_FAILED_TTL_SEC
    return RESULT_DONE_TTL_SEC


def problems() -> list[str]:
    """값끼리 어긋나는 곳을 문장으로 돌려준다. 비어 있으면 문제 없음."""
    found: list[str] = []
    agent_timeout = float(os.getenv("AGENT_TIMEOUT_SEC", "120"))
    if RESULT_PROCESSING_TTL_SEC <= agent_timeout:
        found.append(f"PROCESSING TTL({RESULT_PROCESSING_TTL_SEC}초)이 에이전트 제한({agent_timeout:g}초) 이하다. 처리 중에 상태가 사라질 수 있다")
    return found
