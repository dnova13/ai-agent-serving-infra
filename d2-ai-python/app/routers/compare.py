"""
Qdrant 사용 여부 비교 엔드포인트 (Day 10)

POST /api/v1/rag/compare    같은 질문을 두 방식으로 답하게 해서 결과를 나란히 돌려준다.
POST /api/v1/rag/ask        Qdrant 검색 후 답변 (RAG) 하나만 실행한다.
POST /api/v1/rag/ask-plain  검색 없이 채팅 모델 혼자 답한다. (비교 기준용)
기존 /api/v1/agent/run (에이전트)과는 별개이며, 그 동작은 바꾸지 않는다.
(파일 이름은 처음 만든 compare 그대로이고, ask 계열도 같은 /rag 라우터에 둔다.)
"""

from fastapi import APIRouter, HTTPException
from app.schemas.models import AskRequest, AskResponse, CompareRequest, CompareResponse
from app.services.rag_compare import answer_with_rag, answer_without_rag, compare

router = APIRouter(prefix="/api/v1/rag", tags=["rag"])

SUPPORTED_PROVIDERS = ("gemini", "claude")


# async def 가 아니라 def 로 만든 이유:
# 이 함수 안에서 모델/Qdrant 호출이 끝날 때까지 기다리는(blocking) 코드를 실행한다.
# async def 안에서 blocking 호출을 하면 그동안 서버 전체(다른 요청 포함)가 멈추지만,
# def 로 만들면 FastAPI 가 별도 스레드에서 실행해서 다른 요청을 막지 않는다.
@router.post("/compare", response_model=CompareResponse)
def compare_endpoint(req: CompareRequest):
    if req.provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=f"지원하지 않는 provider입니다: {req.provider} (사용 가능: {list(SUPPORTED_PROVIDERS)})",
        )
    if not 1 <= req.limit <= 10:
        raise HTTPException(status_code=400, detail="limit 은 1~10 사이여야 합니다.")
    return compare(req.query, req.provider, req.limit)


# ── 방식별 단독 API ──────────────────────────────
# /compare 와 달리 방식 하나만 실행한다. 모델 호출이 실패하면 결과 안에 error 를 담는 대신
# 502(상위 서비스 실패)로 응답한다. 단독 API 는 호출한 쪽이 상태 코드로 성공/실패를 구분할 수 있어야 하기 때문.
def _validate(req: AskRequest) -> None:
    if req.provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=f"지원하지 않는 provider입니다: {req.provider} (사용 가능: {list(SUPPORTED_PROVIDERS)})",
        )
    if not 1 <= req.limit <= 10:
        raise HTTPException(status_code=400, detail="limit 은 1~10 사이여야 합니다.")


def _to_response(req: AskRequest, mode: str, result: dict) -> dict:
    if result.get("error"):
        raise HTTPException(status_code=502, detail=result["error"])
    return {"query": req.query, "provider": req.provider, "mode": mode, **result}


@router.post("/ask", response_model=AskResponse)
def ask_with_rag(req: AskRequest):
    """Qdrant 에서 가까운 문서를 찾아 근거로 답한다. (임베딩 1번 + Qdrant 검색 + 채팅 모델 1번)"""
    _validate(req)
    return _to_response(req, "rag", answer_with_rag(req.query, req.provider, req.limit))


@router.post("/ask-plain", response_model=AskResponse)
def ask_without_rag(req: AskRequest):
    """검색 없이 채팅 모델 혼자 답한다. (채팅 모델 1번). limit 은 무시된다."""
    _validate(req)
    return _to_response(req, "plain", answer_without_rag(req.query, req.provider))
