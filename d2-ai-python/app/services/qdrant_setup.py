"""
Qdrant Collection 생성 (Day 7)

- 이미 존재하면 건드리지 않음 (idempotent)
- 벡터 차원은 Day 8에서 쓸 임베딩 모델과 반드시 일치해야 함
  (gemini-embedding-001 은 output_dimensionality 로 768 지정 예정)
- 실행: python -m app.services.qdrant_setup
"""

import os
from dotenv import load_dotenv

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, HnswConfigDiff, VectorParams

load_dotenv()

COLLECTION_NAME = "documents-12"
VECTOR_SIZE = 768
DISTANCE = Distance.COSINE
HNSW_M = 16             # 노드마다 이웃 연결 수 (Day 29 결과로 정함, Qdrant 기본값과 같다)
HNSW_EF_CONSTRUCT = 100  # 인덱스를 만들 때 살펴보는 후보 수 (Qdrant 기본값과 같다)


def get_client() -> QdrantClient:
    return QdrantClient(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
    )


## 멱등성 보장: 이미 존재하면 건드리지 않음
def ensure_collection(client: QdrantClient, name: str = COLLECTION_NAME) -> bool:
    """컬렉션이 없으면 생성. 새로 만들었으면 True, 이미 있으면 False."""
    if client.collection_exists(name):
        return False
    client.create_collection(
        collection_name=name,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=DISTANCE),
        hnsw_config=HnswConfigDiff(m=HNSW_M, ef_construct=HNSW_EF_CONSTRUCT),
    )
    return True


if __name__ == "__main__":
    client = get_client()
    created = ensure_collection(client)
    print(f"[{COLLECTION_NAME}] {'생성됨' if created else '이미 존재함'}")

    info = client.get_collection(COLLECTION_NAME)
    print(f"  status : {info.status}")
    print(f"  vectors: {info.config.params.vectors}")
    print(f"  points : {info.points_count}")
    print(f"  전체 컬렉션: {[c.name for c in client.get_collections().collections]}")
