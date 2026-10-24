"""
Qdrant 검색 함수 (Day 9)

질문 -> 임베딩(RETRIEVAL_QUERY, 768차원) -> Qdrant 에서 가장 가까운 문서 top-k

- Day 10 에서 LangGraph 도구(search_vector_db)가 이 함수를 그대로 호출한다.
- 컬렉션 이름은 qdrant_setup.COLLECTION_NAME 한 곳에서만 가져온다.
  (Day 8 에서 이름이 어긋나 0건으로 보였던 문제 재발 방지)
- qdrant-client 1.19 에는 client.search() 가 없다. query_points() 를 쓴다.
- 실행: python -m app.services.qdrant_search "돈 돌려받고 싶어요"
"""

import sys
from typing import Optional

from qdrant_client.models import FieldCondition, Filter, MatchValue, SearchParams

from app.services.qdrant_setup import COLLECTION_NAME, get_client
from app.services.embedding import embed_query
HNSW_EF = 64  # 검색 때 살펴보는 후보 수. recall@10 약 0.99, 더 키우면 정확해지지만 느려진다 (Day 29)


def search(
    query: str,
    limit: int = 3,
    category: Optional[str] = None,
    score_threshold: Optional[float] = None,
) -> list[dict]:
    """
    질문과 의미가 가까운 문서를 찾아 [{score, doc_id, category, source, text}, ...] 로 반환.

    category        : 지정하면 해당 카테고리 문서 안에서만 검색 (payload 필터)
    score_threshold : 지정하면 유사도(cosine)가 이 값 미만인 결과는 버림
    """
    # 1) 질문 -> 벡터. Gemini API 를 호출하는 단계라서 검색 시간의 대부분이 여기서 쓰인다.
    #    (Qdrant 검색 자체는 수 ms. 나중에 k6 부하 테스트에서 병목이 어디인지 볼 때 중요한 포인트)
    #    문서를 저장할 때(RETRIEVAL_DOCUMENT)와 달리 질문용(RETRIEVAL_QUERY) 임베딩을 쓴다.
    vector = embed_query(query)

    # 2) 카테고리 필터 (선택). payload 의 category 값이 정확히 일치하는 문서만 검색 대상으로 삼는다.
    #    category 가 None 이면 필터 없이 전체 문서에서 검색.
    #    Qdrant 는 "필터로 후보를 먼저 거른 뒤 그 안에서 가까운 것"을 찾으므로
    #    필터를 걸어도 limit 개수를 채워서 돌려준다. (가까운 걸 뽑고 나중에 거르는 방식이 아님)
    query_filter = None
    if category:
        query_filter = Filter(
            must=[FieldCondition(key="category", match=MatchValue(value=category))]
        )

    # 3) Qdrant 검색.
    #    - query_points() : qdrant-client 1.19 의 검색 API (옛 client.search() 는 없어짐)
    #    - limit          : 가까운 순으로 몇 건 가져올지 (top-k)
    #    - score_threshold: 코사인 유사도가 이 값보다 낮은 건 버림. None 이면 끄기.
    #                       실측해 보니 정답 점수(0.71~0.82)와 오답 점수가 겹쳐서
    #                       무관 질문의 점수를 확인하기 전까지 기본값은 None 으로 둔다.
    #    - search_params  : HNSW 검색 때 살펴보는 후보 수 hnsw_ef (Day 29 에서 64 로 정함).
    #                       값을 안 적으면 Qdrant 가 인덱스를 만들 때 값(ef_construct=100)을 쓰므로 코드에 적어 둔다.
    #    - with_payload   : 원문/카테고리를 같이 받아와야 결과를 바로 쓸 수 있다.
    result = get_client().query_points(
        collection_name=COLLECTION_NAME,
        query=vector,
        query_filter=query_filter,
        limit=limit,
        score_threshold=score_threshold,
        search_params=SearchParams(hnsw_ef=HNSW_EF),
        with_payload=True,
    )

    # 4) Qdrant 의 응답 객체(ScoredPoint)를 다루기 쉬운 dict 리스트로 변환한다.
    #    score 는 코사인 유사도. 높을수록 비슷하다 (1.0 에 가까울수록 같은 의미).
    #    결과는 이미 점수 내림차순이라 [0] 이 가장 비슷한 문서다.
    #    result.points 가 비어 있으면(필터/임계값에 걸림) 빈 리스트가 반환된다.
    return [
        {
            "score": round(p.score, 4),
            "doc_id": p.payload.get("doc_id"),
            "category": p.payload.get("category"),
            "source": p.payload.get("source"),
            "text": p.payload.get("text"),
        }
        for p in result.points
    ]


def format_for_llm(hits: list[dict]) -> str:
    """검색 결과를 LLM 에게 줄 문자열로 변환. (Day 10 도구에서 사용)"""
    # 빈 결과를 빈 문자열로 주면 LLM 이 근거가 없는데도 지어낼 수 있어서,
    # "찾지 못했다"고 명시적으로 알려 준다.
    if not hits:
        return "관련 문서를 찾지 못했습니다."
    # 번호를 붙여서 LLM 이 "[문서 1] 에 따르면…" 처럼 근거를 구분해 인용할 수 있게 한다.
    return "\n".join(f"[문서 {i}] {h['text']}" for i, h in enumerate(hits, 1))


if __name__ == "__main__":
    # 명령행 인자를 이어 붙여 질문으로 쓴다. (따옴표 없이 띄어 써도 동작)
    # 인자가 없으면 기본 질문으로 실행.
    q = " ".join(sys.argv[1:]) or "돈 돌려받고 싶어요"
    print(f"질문: {q}\n")
    for h in search(q, limit=3):
        print(f"  {h['score']:.4f} | {h['category']:<8} | {h['doc_id']:<11} | {h['text'][:40]}...")
