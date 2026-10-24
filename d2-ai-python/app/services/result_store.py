"""Day 17 - 에이전트 처리 결과를 보관한다. (Day 23 부터 Redis 에 둔다)

Day 17 에는 프로세스 메모리에만 두었다. 한계: 서버를 껐다 켜면 사라지고, 워커가 여러 개(Day 18)면 서로의 결과를 보지 못했다.
Day 23: requestId 별 상태와 답을 Redis 에 둔다.
  - 키: agent:result:<requestId>   값: {requestId, status, answer, error, elapsedMs, worker, partition, offset, updatedAt} (JSON)
  - 쓸 때마다 TTL 을 그 상태의 값으로 새로 건다. 상태별 값은 ttl_policy.py 에 있다(Day 24: PROCESSING 600초, DONE/FAILED 86400초).
  - 워커 1(8000)이 쓴 결과를 워커 2(8001)의 조회 주소에서도 볼 수 있고, 서버를 재시작해도 남는다(TTL 안에서).
  - Redis 를 쓸 수 없으면 예외를 올리지 않고 경고만 남긴 뒤 예전처럼 이 프로세스의 메모리를 쓴다. 답을 만드는 일이 결과 저장 때문에 실패하면 안 되기 때문이다.
    이때는 Day 17 의 한계가 그대로 돌아온다(그 워커의 조회 주소에서만 보임).
  - 읽을 때 Redis 가 되면 Redis 가 정답이다. 키가 없으면(없는 id, 아직 안 받음, TTL 만료) 메모리를 보지 않고 없다고 답한다.
  - 메모리에는 오래된 것부터 지우며(기본 1000건) 복사본을 계속 둔다. Redis 가 안 될 때의 대체용이다.
"""
import logging
import os
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional

from app.services import redis_client as rc
from app.services import ttl_policy
_log = logging.getLogger("kafka_worker.result_store")

# Day 24: 상태별 TTL 은 ttl_policy.result_ttl(status) 가 정한다(PROCESSING 은 짧게, DONE/FAILED 는 길게).

MAX_ENTRIES = int(os.getenv("RESULT_STORE_MAX", "1000"))

PROCESSING = "PROCESSING"  # 워커가 받아서 에이전트를 부르는 중
DONE = "DONE"              # 답이 나옴
FAILED = "FAILED"          # 에이전트 호출이 실패(타임아웃 포함)


def result_key(request_id: str) -> str:
    """result_key("abc") -> "agent:result:abc" """
    return rc.make_key("result", request_id)


class ResultStore:
    def __init__(self, max_entries: int = MAX_ENTRIES):
        self._max = max_entries
        self._items: "OrderedDict[str, dict]" = OrderedDict()

    # Day 23: 지금 저장된 값(Redis, 안 되면 메모리)을 읽어 새 필드를 합친 뒤, 메모리 복사본과 Redis 에 함께 쓴다.
    #             합칠 때 Redis 를 먼저 읽는 이유: 이전 상태(PROCESSING)를 다른 프로세스가 썼을 수도 있어서다(같은 requestId 는 한 워커가 끝까지 처리하므로 보통은 같은 워커다).
    def _put(self, request_id: str, **fields) -> None:
        item = self.get(request_id) or {}
        item.update(fields)
        item["requestId"] = request_id
        item["updatedAt"] = datetime.now(timezone.utc).isoformat()
        self._items.pop(request_id, None)              # 갱신되면 가장 최근 것으로 옮긴다
        self._items[request_id] = item
        while len(self._items) > self._max:
            self._items.popitem(last=False)            # 가장 오래된 것을 지운다
        try:
            # Day 24: 합친 뒤의 status 로 TTL 을 고른다. PROCESSING → DONE 으로 바뀌면 TTL 이 DONE 의 값으로 새로 시작된다
            rc.set_json(result_key(request_id), item, ttl_sec=ttl_policy.result_ttl(item.get("status")))
        except Exception as e:  # Redis 장애: 메모리 복사본만 남기고 계속한다
            _log.warning("결과를 Redis 에 못 저장함 requestId=%s (%s: %s). 이 프로세스 메모리에만 둔다", request_id, type(e).__name__, e)

    # Day 18: 어느 워커가 어느 파티션에서 받아 처리했는지도 남긴다(워커를 여러 개 띄울 때 분배를 보려는 것)
    def set_processing(self, request_id: str, worker: Optional[str] = None,
                       partition: Optional[int] = None, offset: Optional[int] = None) -> None:
        self._put(request_id, status=PROCESSING, answer=None, error=None, elapsedMs=None,
                  worker=worker, partition=partition, offset=offset)

    def set_done(self, request_id: str, answer: str, elapsed_ms: int) -> None:
        self._put(request_id, status=DONE, answer=answer, error=None, elapsedMs=elapsed_ms)

    def set_failed(self, request_id: str, error: str) -> None:
        self._put(request_id, status=FAILED, answer=None, error=error, elapsedMs=None)

    # Day 23: Redis 가 되면 Redis 가 정답이다(없으면 None). 예외가 나면 이 프로세스의 메모리 복사본으로 대신한다.
    def get(self, request_id: str) -> Optional[dict]:
        try:
            return rc.get_json(result_key(request_id))
        except Exception as e:
            _log.warning("결과를 Redis 에서 못 읽음 requestId=%s (%s: %s). 이 프로세스 메모리를 본다", request_id, type(e).__name__, e)
            item = self._items.get(request_id)
            return dict(item) if item is not None else None

    def __len__(self) -> int:
        return len(self._items)


# 앱 전체에서 하나를 같이 쓴다 (agent_handler 가 쓰고, routers/results.py 가 읽는다)
store = ResultStore()
