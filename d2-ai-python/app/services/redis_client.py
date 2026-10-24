"""
Redis 기본 연결과 읽기/쓰기 함수 (Day 19)

Day 20 에서 에이전트 상태를, Day 21 에서 분산 락을 Redis 에 올리기 전에
연결과 기본 동작(저장, 조회, 삭제, 만료 시간)만 따로 확인한다.

- 접속 주소는 환경변수 REDIS_HOST(기본 localhost), REDIS_PORT(기본 6379), REDIS_DB(기본 0).
  docker-compose 의 redis1 컨테이너가 6379 를 호스트에 열어 둔다.
- 값은 JSON 문자열로 저장한다. 파이썬 dict/list 를 그대로 넣고 꺼낸다.
- 키 이름은 항상 "agent:" 접두어를 붙인다(다른 용도의 키와 섞이지 않게).
- 동기(sync) 클라이언트다. 에이전트가 asyncio.to_thread 로 도는 스레드에서도 그대로 쓸 수 있다.
  FastAPI 의 async 함수 안에서 직접 부르면 그동안 이벤트 루프가 멈추므로, 부를 곳이 async 면 to_thread 로 감싼다.
- 실행(연결 확인): python -m app.services.redis_client   (d2-ai-python 에서, Redis 컨테이너가 떠 있어야 함)
"""

import json
import os
import time
from typing import Any, Optional

import redis

KEY_PREFIX = "agent:"

HOST = os.getenv("REDIS_HOST", "localhost")
PORT = int(os.getenv("REDIS_PORT", "6379"))
DB = int(os.getenv("REDIS_DB", "0"))

# Redis 가 꺼져 있어도 서버가 오래 멈추지 않게 짧게 끊는다(초).
CONNECT_TIMEOUT_SEC = float(os.getenv("REDIS_CONNECT_TIMEOUT_SEC", "1"))
SOCKET_TIMEOUT_SEC = float(os.getenv("REDIS_SOCKET_TIMEOUT_SEC", "2"))

_client: Optional["redis.Redis"] = None


def get_client() -> "redis.Redis":
    """프로세스에서 하나만 만들어 재사용한다. 내부에 연결 풀이 있어서 스레드에서 같이 써도 된다."""
    global _client
    if _client is None:
        _client = redis.Redis(
            host=HOST,
            port=PORT,
            db=DB,
            decode_responses=True,  # bytes 가 아니라 str 로 받는다
            socket_connect_timeout=CONNECT_TIMEOUT_SEC,
            socket_timeout=SOCKET_TIMEOUT_SEC,
        )
    return _client


def make_key(*parts: str) -> str:
    """make_key("state", "abc") -> "agent:state:abc" """
    return KEY_PREFIX + ":".join(parts)


def ping() -> dict:
    """연결 상태를 예외 없이 돌려준다. /health 에서 쓴다.

    성공: {"connected": True, "latencyMs": 0.8}
    실패: {"connected": False, "error": "ConnectionError: ..."}
    """
    t0 = time.perf_counter()
    try:
        get_client().ping()
    except Exception as e:  # Redis 가 꺼져 있어도 /health 는 응답해야 한다
        return {"connected": False, "error": f"{type(e).__name__}: {e}"}
    return {"connected": True, "latencyMs": round((time.perf_counter() - t0) * 1000, 2)}


def set_json(key: str, value: Any, ttl_sec: Optional[int] = None) -> None:
    """값을 JSON 으로 저장한다. ttl_sec 이 있으면 그 시간(초) 뒤에 Redis 가 알아서 지운다. 같은 키는 덮어쓴다."""
    # ensure_ascii=False: 한글을 \\uXXXX 로 바꾸지 않고 그대로 저장(redis-cli 에서 읽기 쉽다)
    get_client().set(key, json.dumps(value, ensure_ascii=False), ex=ttl_sec)


def get_json(key: str) -> Optional[Any]:
    """저장한 값을 꺼낸다. 키가 없거나 만료됐으면 None."""
    raw = get_client().get(key)
    if raw is None:
        return None
    return json.loads(raw)


def delete(key: str) -> bool:
    """키를 지운다. 실제로 지웠으면 True, 원래 없었으면 False."""
    return get_client().delete(key) > 0


def exists(key: str) -> bool:
    return get_client().exists(key) > 0


def ttl_sec(key: str) -> Optional[int]:
    """남은 만료 시간(초). 키가 없으면 None, 만료 시간이 없는 키(영구)는 -1.

    Redis TTL 명령은 키 없음=-2, 영구=-1 을 주는데, 호출하는 쪽이 헷갈리지 않게 -2 만 None 으로 바꾼다.
    """
    t = get_client().ttl(key)
    return None if t == -2 else t


if __name__ == "__main__":
    # 간단한 연결 확인: python -m app.services.redis_client
    print(f"접속 {HOST}:{PORT} db={DB}")
    print(ping())
