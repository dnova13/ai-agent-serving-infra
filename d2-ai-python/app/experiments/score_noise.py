"""
Day 11 검색 점수 바닥값(noise floor) 측정

문서와 상관없는 질문을 넣었을 때 top-1 점수가 얼마나 나오는지 본다.
관련 질문의 점수(약 0.71~0.82)와 겹치지 않는 구간을 찾으면 score_threshold 를 정할 수 있다.

- 실행: python -m app.experiments.score_noise   (프로젝트 루트 d2-ai-python 에서)
- 질문 1개당 임베딩 API 1번 호출
"""

import time

from app.services.qdrant_search import search

# 문서와 상관없는 질문 (쇼핑몰 CS 와 무관)
OFF_TOPIC = [
    "오늘 서울 날씨 어때?",
    "파이썬 리스트 정렬하는 법 알려줘",
    "점심 메뉴 추천해줘",
    "세종대왕이 누구야?",
    "축구 월드컵 우승국이 어디야?",
    "비트코인 지금 살까요?",
]
# 쇼핑몰 질문이지만 문서에 없는 것 (애매한 구간)
IN_DOMAIN_NO_DOC = [
    "포인트 적립률은 몇 퍼센트예요?",
    "매장에 직접 방문해서 살 수 있나요?",
    "해외 배송도 되나요?",
    "선물 포장 해주나요?",
]
# 관련 있는 질문 (비교 기준)
RELEVANT = [
    "제주도 배송비는 얼마예요?",
    "비번을 까먹었어요",
    "할부 되나요?",
    "돈 돌려받고 싶어요",
]


def top1(q: str):
    hits = search(q, limit=1)
    return hits[0] if hits else None


def section(title: str, questions: list[str]) -> list[float]:
    print(f"\n[{title}]")
    scores = []
    for q in questions:
        h = top1(q)
        s = h["score"] if h else 0.0
        scores.append(s)
        print(f"  {s:.4f}  {q}  -> {h['doc_id'] if h else '-'}")
        time.sleep(0.5)
    print(f"  min={min(scores):.4f}  max={max(scores):.4f}")
    return scores


def main() -> None:
    rel = section("관련 질문", RELEVANT)
    dom = section("쇼핑몰 질문, 문서에 없음", IN_DOMAIN_NO_DOC)
    off = section("완전 무관", OFF_TOPIC)
    print("\n" + "=" * 50)
    print(f"관련 min       : {min(rel):.4f}")
    print(f"문서없음 max   : {max(dom):.4f}")
    print(f"무관 max       : {max(off):.4f}")
    gap = min(rel) - max(off)
    print(f"관련 min - 무관 max = {gap:+.4f}  ({'threshold 로 구분 가능' if gap > 0.03 else '구간이 겹쳐 threshold 만으로는 구분 어려움'})")


if __name__ == "__main__":
    main()
