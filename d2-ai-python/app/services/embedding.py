"""
임베딩 모듈 (Day 8)

텍스트를 숫자 벡터(768차원)로 바꾼다.

핵심 포인트
1) 차원 768은 qdrant_setup.VECTOR_SIZE 와 반드시 같아야 한다.
   (gemini-embedding-001 기본은 3072차원이라 output_dimensionality 로 줄여서 쓴다)
2) task_type 을 문서/질문에 따라 다르게 준다.
   - 저장할 문서   : RETRIEVAL_DOCUMENT
   - 사용자의 질문 : RETRIEVAL_QUERY   (Day 9 검색에서 사용)
   같은 모델이라도 용도에 맞춰 벡터를 만들면 검색 정확도가 올라간다.
3) 무료 한도(429)에 걸릴 수 있어서 배치로 묶어서 호출한다.
"""

import os
from dotenv import load_dotenv
from langchain_google_genai import GoogleGenerativeAIEmbeddings

load_dotenv()

EMBEDDING_MODEL = "models/gemini-embedding-001"
EMBEDDING_DIM = 768


def _embeddings() -> GoogleGenerativeAIEmbeddings:
    return GoogleGenerativeAIEmbeddings(
        model=EMBEDDING_MODEL,
        google_api_key=os.getenv("GOOGLE_API_KEY"),
    )


def embed_documents(texts: list[str]) -> list[list[float]]:
    """저장용 문서들을 한 번에 임베딩."""
    return _embeddings().embed_documents(
        texts,
        task_type="RETRIEVAL_DOCUMENT",
        output_dimensionality=EMBEDDING_DIM,
    )


def embed_query(text: str) -> list[float]:
    """검색용 질문 1개를 임베딩. (Day 9에서 사용)"""
    return _embeddings().embed_query(
        text,
        task_type="RETRIEVAL_QUERY",
        output_dimensionality=EMBEDDING_DIM,
    )
