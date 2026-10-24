"""
가상 데이터 임베딩 + Qdrant 적재 (Day 8)

흐름: SAMPLE_DOCS -> 임베딩(768차원) -> Qdrant upsert

- point ID는 문서 id 로 만든 uuid5 -> 다시 실행해도 중복 생성 없이 덮어씀 (idempotent)
- payload 에 원문(text)과 category/source 를 같이 저장
  -> 검색 결과로 원문을 바로 돌려줄 수 있고, Day 9에서 category 필터 검색 가능
- 실행: python -m app.services.qdrant_ingest   (프로젝트 루트에서)
"""

import uuid

from qdrant_client.models import PointStruct

from app.services.qdrant_setup import COLLECTION_NAME, ensure_collection, get_client
from app.services.embedding import embed_documents
from app.data.sample_docs import SAMPLE_DOCS
def to_point_id(doc_id: str) -> str:
    """문서 id(문자열) -> Qdrant가 받는 UUID. 같은 id 는 항상 같은 UUID."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))


def ingest(docs=SAMPLE_DOCS) -> int:
    client = get_client()
    ensure_collection(client)

    vectors = embed_documents([d["text"] for d in docs])

    points = [
        PointStruct(
            id=to_point_id(d["id"]),
            vector=vec,
            payload={
                "doc_id": d["id"],
                "text": d["text"],
                "category": d["category"],
                "source": d["source"],
            },
        )
        for d, vec in zip(docs, vectors)
    ]
    client.upsert(collection_name=COLLECTION_NAME, points=points)
    return len(points)


if __name__ == "__main__":
    n = ingest()
    client = get_client()
    total = client.count(COLLECTION_NAME, exact=True).count
    print(f"upsert 요청: {n}건 / 컬렉션 저장 건수: {total}건")

    # 적재 확인: 첫 문서 1건을 벡터 길이와 함께 출력
    rec = client.retrieve(
        COLLECTION_NAME, ids=[to_point_id("refund-001")], with_vectors=True
    )[0]
    print(f"샘플: {rec.payload['doc_id']} | 벡터 길이 {len(rec.vector)} | {rec.payload['text'][:30]}...")
