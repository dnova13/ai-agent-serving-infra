"""
Redis 분산 락 (Day 21)

같은 sessionId 로 요청이 동시에 오면 워커 둘이 같은 대화를 읽고, 각자 쓰면서 서로의 턴을 덮어쓸 수 있다
(Day 20, DAY20_RESULT 3-6). 한 번에 한 워커만 그 세션의 "읽기 → 모델 호출 → 저장" 을 하도록 Redis 에 락을 건다.

방식 (SET NX + 소유 토큰 + 안전한 해제)
- 잡기:  SET <락 키> <토큰> NX PX <만료ms>
    NX = 키가 없을 때만 저장된다. 저장에 성공한 쪽이 락을 잡은 것이다. 토큰은 락마다 새로 만드는 무작위 값이다.
    PX = 만료. 락을 잡은 워커가 죽어도 이 시간이 지나면 Redis 가 키를 지워서 락이 영원히 남지 않는다.
- 못 잡으면: 짧게 쉬고 다시 시도하다가 wait_sec 이 지나면 LockTimeout.
- 풀기: 키의 값이 내 토큰일 때만 지운다(Lua 스크립트로 "읽기 + 지우기" 를 한 번에).
    내 락이 만료된 뒤 다른 워커가 새로 잡았는데, 늦게 끝난 내가 그냥 DELETE 하면 남의 락을 지우게 된다. 토큰 비교가 이를 막는다.

한계 (알고 쓰는 것)
- 락 만료 시간(ttl_sec)보다 일이 오래 걸리면 락이 풀려서 다른 워커가 들어올 수 있다. 연장(갱신) 기능은 만들지 않았다.
  그래서 ttl_sec 은 가장 오래 걸릴 일보다 길게 잡는다(에이전트 한 건 최대 120초, AGENT_TIMEOUT_SEC).
- Redis 서버 한 대 기준이다. Redis 가 복제/장애조치되는 환경에서의 안전성(Redlock 논쟁)은 다루지 않았다.
- redis-py 에도 같은 일을 하는 내장 락(client.lock())이 있다. 여기서는 동작 원리를 보이려고 직접 만들었다.

실행(확인 스크립트): python -m app.checks.lock_check   (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함)
"""

import logging
import os
import random
import time
import uuid
from contextlib import contextmanager
from typing import Iterator, Optional

import redis

from app.services import redis_client as rc
_log = logging.getLogger("redis_lock")

# 락 만료(초). 락을 잡은 일이 이 시간 안에 끝나야 한다. 에이전트 한 건 최대 120초(AGENT_TIMEOUT_SEC)보다 길게.
LOCK_TTL_SEC = float(os.getenv("REDIS_LOCK_TTL_SEC", "150"))
# 락을 못 잡고 기다리는 최대 시간(초). agent_handler 의 전체 제한(120초) 안에서 끝나도록 그보다 짧게.
LOCK_WAIT_SEC = float(os.getenv("REDIS_LOCK_WAIT_SEC", "100"))
# 못 잡았을 때 다시 시도하기까지 쉬는 시간(초). 여러 워커가 같은 순간에 몰리지 않게 조금씩 흔든다.
RETRY_INTERVAL_SEC = float(os.getenv("REDIS_LOCK_RETRY_SEC", "0.1"))

# 키 값이 내 토큰일 때만 지운다. 읽기와 지우기가 Redis 안에서 한 번에 실행되어 그 사이에 끼어들 수 없다.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


class LockTimeout(Exception):
    """wait_sec 안에 락을 잡지 못했다. 다른 쪽이 오래 쥐고 있다."""


def lock_key(name: str) -> str:
    """lock_key("conversation:abc") -> "agent:lock:conversation:abc" """
    return rc.make_key("lock", name)


def try_acquire(name: str, ttl_sec: float = LOCK_TTL_SEC) -> Optional[str]:
    """한 번만 시도한다. 잡았으면 토큰, 이미 누가 쥐고 있으면 None."""
    token = uuid.uuid4().hex
    # nx=True: 키가 없을 때만 저장. px: 만료를 밀리초로 (정수여야 하므로 변환)
    ok = rc.get_client().set(lock_key(name), token, nx=True, px=max(1, int(ttl_sec * 1000)))
    return token if ok else None


def release(name: str, token: str) -> bool:
    """내 토큰일 때만 락을 푼다. 지웠으면 True. 이미 만료됐거나 남이 새로 잡았으면 False(건드리지 않음)."""
    return rc.get_client().eval(_RELEASE_SCRIPT, 1, lock_key(name), token) == 1


def _acquire(name: str, ttl_sec: float, wait_sec: float, fail_open: bool) -> Optional[str]:
    deadline = time.monotonic() + wait_sec
    while True:
        try:
            token = try_acquire(name, ttl_sec)
        except redis.RedisError as e:
            if not fail_open:
                raise
            # Redis 를 쓸 수 없으면 락 없이 계속한다(대화 이어가기도 Redis 라서 어차피 같이 안 된다).
            _log.warning("락을 못 잡음, Redis 오류라 락 없이 계속 name=%s (%s: %s)", name, type(e).__name__, e)
            return None
        if token is not None:
            return token
        if time.monotonic() >= deadline:
            raise LockTimeout(f"{wait_sec:g}초 안에 락을 잡지 못함 name={name}")
        time.sleep(RETRY_INTERVAL_SEC * (0.5 + random.random()))  # 0.5~1.5 배로 흔들기


@contextmanager
def lock(name: str, ttl_sec: float = LOCK_TTL_SEC, wait_sec: float = LOCK_WAIT_SEC,
         fail_open: bool = False) -> Iterator[Optional[str]]:
    """with lock("conversation:abc"): ... 블록 안은 같은 name 으로 한 번에 한 곳만 실행된다.

    - 못 잡으면 wait_sec 까지 기다리고, 그래도 못 잡으면 LockTimeout.
    - fail_open=True 이면 Redis 오류로 락을 못 잡을 때 예외 대신 락 없이 블록을 실행한다(토큰은 None).
    - 블록이 예외로 끝나도 락은 푼다. 푸는 중 Redis 오류는 경고만 남긴다(만료로 어차피 풀린다).
    """
    token = _acquire(name, ttl_sec, wait_sec, fail_open)
    try:
        yield token
    finally:
        if token is not None:
            try:
                if not release(name, token):
                    # 블록이 ttl_sec 보다 오래 걸려 락이 먼저 만료됐다. 그 사이 다른 쪽이 들어왔을 수 있다.
                    _log.warning("락이 풀기 전에 만료됨 name=%s (ttl_sec=%s 보다 오래 걸림)", name, ttl_sec)
            except redis.RedisError as e:
                _log.warning("락을 푸는 중 Redis 오류 name=%s (%s: %s). 만료로 풀린다", name, type(e).__name__, e)
