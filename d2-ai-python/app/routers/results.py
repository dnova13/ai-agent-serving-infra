"""Day 17 - 결과 조회 API (FastAPI 8000, 8001).

Spring(8080)은 202 와 requestId 만 돌려주고, 답은 FastAPI 워커가 만든다. 그 답을 requestId 로 찾아 보는 주소다.
Day 17 에는 메모리 저장소를 읽는 임시 구현이었다(요청을 처리한 워커의 주소에서만 보였다).
Day 23: Redis 저장소(services/result_store.py)를 읽는다. 어느 워커의 주소로 물어도 같은 결과가 나온다.
Redis 호출은 동기 함수라 def 로 두면 FastAPI 가 스레드풀에서 실행한다(sessions.py 와 같은 방식).
"""
from fastapi import APIRouter, HTTPException

from app.services.result_store import store

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.get("/agent/results/{request_id}")
# Day 23: store.get 이 Redis 를 부르므로(동기) def 로 바꿔 이벤트 루프를 막지 않게 한다
def get_result(request_id: str):
    item = store.get(request_id)
    if item is None:
        # 없는 id 이거나, Kafka 에서 아직 워커가 받지 못했거나, 서버를 재시작해서 지워진 경우다. 셋을 구분하지 못한다.
        raise HTTPException(
            status_code=404,
            detail="결과가 없다. 없는 requestId 이거나 아직 워커가 받지 않았거나 서버 재시작으로 지워졌을 수 있다",
        )
    return item
