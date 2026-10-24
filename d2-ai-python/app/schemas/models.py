from datetime import datetime  # Day 15: Kafka 메시지의 createdAt

from pydantic import BaseModel
from typing import Optional


class AgentRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"  # "gemini" 또는 "claude"
    session_id: Optional[str] = None  # Day 20: 같은 값이면 이전 대화를 이어간다(Redis). 없으면 매번 새 대화


class AgentResponse(BaseModel):
    answer: str
    provider: str


# ── Day 10: Qdrant 사용 여부 비교 API (POST /api/v1/rag/compare) ──
# 같은 질문을 "Qdrant 검색 없이" / "Qdrant 검색 후" 두 번 답하게 해서 차이를 한 번에 보여준다.
class CompareRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"  # "gemini" 또는 "claude"
    limit: Optional[int] = 3  # RAG 쪽에서 검색해서 근거로 쓸 문서 수


class SourceDoc(BaseModel):
    doc_id: Optional[str] = None
    category: Optional[str] = None
    source: Optional[str] = None
    score: float
    text: str


class CompareSide(BaseModel):
    answer: Optional[str] = None  # 실패하면 None
    error: Optional[str] = None  # 실패 사유 (한쪽이 실패해도 다른 쪽 결과는 보여주기 위해 따로 둠)
    elapsed_ms: int  # 이 방식 전체에 걸린 시간
    llm_calls: int  # 채팅 모델 호출 횟수
    retrieval_ms: Optional[int] = None  # RAG 만: 질문 임베딩 + Qdrant 검색 시간
    sources: Optional[list[SourceDoc]] = None  # RAG 만: 근거로 쓴 문서와 유사도 점수


class CompareResponse(BaseModel):
    query: str
    provider: str
    without_rag: CompareSide
    with_rag: CompareSide


# ── Day 10: 방식별 단독 API (POST /api/v1/rag/ask, /api/v1/rag/ask-plain) ──
# /compare 는 두 방식을 한 번에 돌려주지만, 이쪽은 방식 하나만 실행해서 결과 하나만 돌려준다.
class AskRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"  # "gemini" 또는 "claude"
    limit: Optional[int] = 3  # /ask (RAG) 에서만 사용. 검색해서 근거로 쓸 문서 수


class AskResponse(CompareSide):
    # CompareSide 의 필드(answer, elapsed_ms, llm_calls, retrieval_ms, sources)를 그대로 쓰고
    # 어떤 질문/모델/방식이었는지만 더한다.
    query: str
    provider: str
    mode: str  # "rag" (Qdrant 검색 후 답변) | "plain" (검색 없이 모델 단독)


# ── Day 15: Kafka agent.requests 토픽 메시지 (Spring WAS 가 보내는 JSON) ──
# 스키마는 docs/DAY13_GUIDE.md 3장, Spring 쪽은 AgentRequestMessage.java. 필드를 바꾸면 양쪽을 같이 고친다.
# createdAt 은 "2026-10-10T14:41:16.949855Z" 처럼 끝이 Z 이고 소수점 6자리다. pydantic v2 가 이 형식을 datetime 으로
# 바로 파싱한다(표준 라이브러리 datetime.fromisoformat 은 파이썬 3.10 이하에서 Z 를 못 읽는다).
class AgentRequestMessage(BaseModel):
    requestId: str
    question: str
    provider: str = "gemini"
    createdAt: datetime
    # Day 20: 같은 sessionId 면 이전 대화를 이어간다. 없거나 null 이면 새 대화. Spring 의 AgentRequestMessage.java 와 같이 고친다.
    sessionId: Optional[str] = None
