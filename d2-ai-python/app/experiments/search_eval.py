"""
검색 정확도 확인 (Day 9)

문서에 없는 표현으로 물어도 의미가 같은 문서가 나오는지 본다.
각 질문에 "정답 카테고리"를 정해 두고 top-1 / top-3 안에 정답이 있는지 센다.

- 키워드 검색(LIKE '%환불%')이라면 못 찾을 질문을 일부러 많이 넣었다.
- 정답이 애매한 질문(두 카테고리에 걸침)은 accept 에 둘 다 허용했다.
- 실행: python -m app.experiments.search_eval
- 질문 1개당 임베딩 API 1번 호출 (무료 한도 주의, 질문 수만큼 호출됨)
"""

import time

from app.services.qdrant_search import search

# (질문, 정답으로 인정하는 카테고리들)
# 질문 작성 원칙: 문서에 있는 단어를 그대로 쓰지 않았다.
#   예) "돈 돌려받고" (문서: 환불), "비번" (문서: 비밀번호), "택배" (문서: 배송)
#   키워드 검색이라면 못 찾았을 표현으로 물어야 임베딩 검색의 가치가 확인된다.
EVAL_SET = [
    ("돈 돌려받고 싶어요", {"refund"}),
    ("산 거 취소하고 환급받으려면 며칠 안에 해야 해요?", {"refund"}),
    ("불량품이 왔는데 배송비는 누가 내요?", {"refund"}),
    ("택배 언제 와요?", {"shipping"}),
    ("제주도는 배송비가 더 붙나요?", {"shipping"}),
    ("주소를 잘못 적었어요 바꿀 수 있나요", {"shipping"}),
    ("다른 사이즈로 바꾸고 싶어요", {"exchange", "review"}),
    ("할부 되나요?", {"payment"}),
    ("카카오페이로 계산할 수 있어요?", {"payment"}),
    ("비번을 까먹었어요", {"member"}),
    ("탈퇴하면 적립금은 어떻게 되나요", {"member"}),
    ("물건 받아본 사람들 후기가 어때요", {"review"}),
]


def run() -> None:
    top1 = top3 = 0
    for q, accept in EVAL_SET:
        hits = search(q, limit=3)
        cats = [h["category"] for h in hits]
        # top-1: 가장 비슷하다고 고른 문서의 카테고리가 정답인가 (RAG 에서 가장 중요한 지표)
        ok1 = bool(cats) and cats[0] in accept
        # top-3: 3건 안에만 정답이 있어도 LLM 이 읽고 걸러낼 수 있으므로 별도로 센다.
        ok3 = any(c in accept for c in cats)
        top1 += ok1
        top3 += ok3
        # O: 1위 정답 / △: 1위는 틀렸지만 3위 안에 정답 / X: 3위 안에도 없음
        mark = "O" if ok1 else ("△" if ok3 else "X")
        best = hits[0] if hits else {"score": 0, "category": "-", "doc_id": "-"}
        print(f"[{mark}] {q}\n     정답 {sorted(accept)} / 1위 {best['category']} {best['doc_id']} ({best['score']})")
        # 질문 1개당 임베딩 API 를 1번 호출한다. 짧게 쉬어서 무료 한도(분당 요청 수)에 덜 걸리게 한다.
        time.sleep(0.3)  # 무료 한도 완화

    n = len(EVAL_SET)
    print(f"\ntop-1 정확도: {top1}/{n} ({top1 / n:.0%})")
    print(f"top-3 정확도: {top3}/{n} ({top3 / n:.0%})")


if __name__ == "__main__":
    run()
