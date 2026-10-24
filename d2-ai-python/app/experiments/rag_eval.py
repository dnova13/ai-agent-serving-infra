"""
RAG 유무 정량 비교 (Day 41 이후 추가 실험)

같은 질문을 세 가지 방식으로 답하게 하고, 답에 정답 사실이 들어 있는지 자동으로 센다.

  plain : 검색 없이 채팅 모델 혼자 답한다.
  rag   : 질문과 가까운 문단 k개(기본 3)를 Qdrant 에서 찾아 프롬프트에 붙여 답한다.
  full  : 검색 없이 문서 4개 전체를 프롬프트에 붙여 답한다. (RAG 를 쓰는 이유를 가르는 비교 기준)

질문 두 묶음
  문서 안 질문 12개 : data/long_docs.py 의 EVAL_QUESTIONS. 문서에만 있는 값(예: 교환 배송비 5,000원)을
                      묻는다. 답에 정답 사실(FACTS)이 모두 있으면 정답으로 센다.
  문서 밖 질문 4개  : 문서에 없는 내용. "모른다/없다"고 답하면 올바른 것으로 센다(지어내지 않음).

채점은 글자 규칙이다. 의미를 이해하지 못하므로 틀리게 세는 경우가 있어서, 모든 답을 --out 파일에 남긴다.
사람이 눈으로 확인할 수 있게 하려는 것이다.

실행 (프로젝트 루트 d2-ai-python 에서)
  python -m app.experiments.rag_eval --selftest   # 채점 규칙만 확인. API 호출 없음
  python -m app.experiments.rag_eval              # Claude 로 실행. 질문당 방식 3개 x 반복 3번

호출량 (기본값): 채팅 모델 (12+4) x 3방식 x 3번 = 144번, 임베딩은 청크 15개와 질문 16개뿐이다.
"""

import argparse
import json
import re
import statistics
import time

# ── 정답 사실 ─────────────────────────────────────────
# EVAL_QUESTIONS 와 같은 순서다. 각 질문마다 정규식 목록이고, 답에 '모두' 맞아야 정답이다.
# 답은 먼저 공백과 쉼표를 지운 뒤 검사한다(norm). 그래서 "5,000 원" 도 "5000원" 으로 맞는다.
FACTS = [
    [r"5000원"],                                        # 단순 변심 왕복 배송비 5,000원
    [r"30일"],                                          # 하자는 받은 날부터 30일
    [r"불가|할수없|안됩|안돼|어렵"],                    # 뜯은 화장품은 환불 불가
    [r"3(일)?(~|-|에서|부터|∼)5일"],                    # 카드 환불 영업일 3~5일
    [r"3000원"],                                        # 제주 추가 배송비 3,000원
    [r"3000원", r"면제되지않|부과|별도|내야|그대로"],     # 5만 원 이상 무료여도 도서산간 추가는 그대로
    [r"2시"],                                           # 오후 2시 이전 결제만 당일 출고
    [r"재발송"],                                        # 분실이면 재발송 또는 환불
    [r"3개월"],                                         # 무이자 할부 최대 3개월
    [r"1%", r"1년"],                                    # 적립 1%, 사용 기한 1년
    [r"30분"],                                          # 재설정 링크 30분
    [r"100만원", r"6개월"],                             # 최근 6개월 100만 원
]

# 문서에 없는 질문. 모른다고 하는 것이 맞다.
OUT_OF_DOC = [
    "고객센터 전화번호가 뭐예요?",
    "반품할 때 어느 택배사가 오나요?",
    "해외로도 배송해 주나요?",
    "매장에 직접 가서 받아갈 수 있어요?",
]

# 모른다고 답했는지 보는 표현. 이 중 하나라도 있으면 "모른다고 답함"으로 센다.
ABSTAIN = r"해당내용이없|문서에없|정보가없|알수없|알지못|확인되지않|확인할수없|안내되어있지않|안내해드리기어|말씀드리기어|모르"


def norm(text: str) -> str:
    """공백과 쉼표를 지워 표기 차이를 줄인다. (5,000원 = 5000원)"""
    return re.sub(r"[\s,]", "", text or "")


def is_correct(answer: str, facts: list[str]) -> bool:
    a = norm(answer)
    return all(re.search(f, a) for f in facts)


def abstained(answer: str) -> bool:
    return bool(re.search(ABSTAIN, norm(answer)))


# ── 채점 확인 (API 호출 없음) ─────────────────────────
def selftest() -> None:
    cases = [
        (0, "왕복 배송비 5,000원은 고객이 부담합니다.", True),
        (0, "고객이 부담해야 합니다.", False),
        (3, "영업일 기준 3~5일 안에 취소됩니다.", True),
        (3, "카드사에 따라 다릅니다.", False),
        (5, "5만 원 이상이면 기본 배송비는 면제되지만 제주도 추가 배송비 3,000원은 면제되지 않습니다.", True),
        (5, "5만 원 이상 구매하면 무료입니다.", False),
        (9, "구매 금액의 1%가 적립되고 1년 동안 쓸 수 있어요.", True),
        (9, "1% 적립됩니다.", False),
        (11, "최근 6개월 구매 금액 100만 원 이상이면 됩니다.", True),
    ]
    bad = 0
    for i, ans, want in cases:
        got = is_correct(ans, FACTS[i])
        if got != want:
            bad += 1
            print(f"[틀림] 질문 {i + 1} 기대 {want} 결과 {got}: {ans}")
    for ans, want in [("문서에 해당 내용이 없습니다.", True), ("전화번호는 1588-0000 입니다.", False),
                      ("정확한 정보를 알 수 없습니다.", True)]:
        got = abstained(ans)
        if got != want:
            bad += 1
            print(f"[틀림] 거절 기대 {want} 결과 {got}: {ans}")
    # 정답 사실이 문서에 실제로 있는지도 확인한다. (FACTS 오타 방지)
    from app.data.long_docs import LONG_DOCS, EVAL_QUESTIONS
    assert len(FACTS) == len(EVAL_QUESTIONS), "FACTS 와 질문 수가 다름"
    docs = norm(" ".join(d["text"] for d in LONG_DOCS))
    for i, facts in enumerate(FACTS):
        for f in facts:
            if not re.search(f, docs):
                bad += 1
                print(f"[문서에 없음] 질문 {i + 1} 정규식 {f}")
    print("채점 확인:", "이상 없음" if bad == 0 else f"{bad}건 이상")


# ── 실험 ──────────────────────────────────────────────
SYSTEM_BASE = "당신은 쇼핑몰 고객센터 상담원입니다. 한국어로 3문장 이내로 간결하게 답하세요."
RAG_RULE = (
    "아래 [참고 문서]에 있는 내용만 근거로 답하세요. "
    "문서에 답이 없으면 '문서에 해당 내용이 없습니다'라고 답하세요."
)
COLL = "rageval"  # 실제 컬렉션 이름은 chunk-exp-rageval. documents-12 는 건드리지 않는다.


def run(args) -> None:
    from langchain_core.messages import HumanMessage, SystemMessage

    from app.data.long_docs import LONG_DOCS, EVAL_QUESTIONS
    from app.experiments import chunk_experiment as ce
    from app.services.agent_graph import get_model
    from app.services.rag_compare import _safe_error, _text
    from app.services.qdrant_search import format_for_llm
    questions = [(q, FACTS[i], must) for i, (q, must) in enumerate(EVAL_QUESTIONS)]
    questions += [(q, None, None) for q in OUT_OF_DOC]

    # 1) 문서를 문단 단위로 잘라 실험용 컬렉션에 넣는다. (Day 12 에서 정확도가 가장 높았던 방식)
    client = ce.get_client()
    chunks = [(d["id"], i, c) for d in LONG_DOCS for i, c in enumerate(ce.chunk_paragraph(d["text"]))]
    ce.ingest(client, COLL, chunks)
    print(f"문서 {len(LONG_DOCS)}개 -> 문단 {len(chunks)}개를 컬렉션 chunk-exp-{COLL} 에 저장")

    # 2) 질문마다 검색은 한 번만 한다. 반복해도 검색 결과는 같기 때문이다.
    retr = []
    for q, facts, must in questions:
        t0 = time.perf_counter()
        hits = ce.search_chunks(client, COLL, ce.embed_q(q), limit=args.k)
        ms = int((time.perf_counter() - t0) * 1000)
        found = ce.hit_rank(hits, must) is not None if must else None  # 정답 근거 문단이 k개 안에 있는가
        retr.append({"hits": hits, "ms": ms, "found": found})
    full_text = "\n\n".join(f"[{d['title']}]\n{d['text']}" for d in LONG_DOCS)

    model = get_model(args.provider)

    def ask(system: str, user: str) -> dict:
        t0 = time.perf_counter()
        try:
            r = model.invoke([SystemMessage(content=system), HumanMessage(content=user)])
            return {"answer": _text(r.content).strip(), "ms": int((time.perf_counter() - t0) * 1000)}
        except Exception as e:
            return {"error": _safe_error(e), "ms": int((time.perf_counter() - t0) * 1000)}

    rows = []  # 호출 1번당 1행
    total = len(questions) * 3 * args.reps
    done = 0
    for qi, (q, facts, must) in enumerate(questions):
        for rep in range(args.reps):
            for arm in ("plain", "rag", "full"):
                if arm == "plain":
                    r = ask(SYSTEM_BASE, q)
                elif arm == "rag":
                    ctx = format_for_llm(retr[qi]["hits"])
                    r = ask(SYSTEM_BASE + "\n" + RAG_RULE, f"[참고 문서]\n{ctx}\n\n[질문]\n{q}")
                else:
                    r = ask(SYSTEM_BASE + "\n" + RAG_RULE, f"[참고 문서]\n{full_text}\n\n[질문]\n{q}")
                row = {"qi": qi, "in_doc": facts is not None, "arm": arm, "rep": rep, "question": q, **r}
                if "answer" in r:
                    row["correct"] = is_correct(r["answer"], facts) if facts else None
                    row["abstain"] = abstained(r["answer"])
                rows.append(row)
                done += 1
            print(f"\r진행 {done}/{total}", end="", flush=True)
    print()

    summarize(rows, retr, questions, args)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"provider": args.provider, "reps": args.reps, "k": args.k,
                       "retrieval": [{"ms": r["ms"], "found": r["found"],
                                      "docs": [h["doc_id"] for h in r["hits"]]} for r in retr],
                       "rows": rows}, f, ensure_ascii=False, indent=1)
        print(f"전체 답변을 {args.out} 에 저장했다.")


def summarize(rows, retr, questions, args) -> None:
    n_in = sum(1 for q in questions if q[1] is not None)
    found = sum(1 for r, q in zip(retr, questions) if r["found"])
    print(f"\n== 모델 {args.provider}, 반복 {args.reps}번, 검색 k={args.k}")
    print(f"검색: 정답 근거 문단이 상위 {args.k}개 안에 있었다 {found}/{n_in}질문, "
          f"검색 시간 평균 {statistics.mean(r['ms'] for r in retr)/1000:.2f}초(질문 임베딩 포함)\n")

    print("문서 안 질문: 정답 사실이 답에 모두 있는 비율")
    print(f"{'방식':<6}{'정답':>10}{'오류':>6}{'평균 시간':>12}{'중앙값':>10}{'최대':>10}")
    for arm in ("plain", "rag", "full"):
        mine = [r for r in rows if r["arm"] == arm and r["in_doc"]]
        ok = [r for r in mine if "answer" in r]
        right = sum(1 for r in ok if r["correct"])
        ts = [r["ms"] / 1000 for r in ok] or [0]
        print(f"{arm:<6}{f'{right}/{len(ok)}':>10}{len(mine) - len(ok):>6}"
              f"{statistics.mean(ts):>11.2f}초{statistics.median(ts):>9.2f}초{max(ts):>9.2f}초")

    print("\n질문별 정답 횟수 (반복 중 몇 번 맞았는가)")
    print(f"{'질문':<46}{'plain':>7}{'rag':>7}{'full':>7}{'검색':>6}")
    for qi, (q, facts, _) in enumerate(questions):
        if facts is None:
            continue
        cells = []
        for arm in ("plain", "rag", "full"):
            mine = [r for r in rows if r["qi"] == qi and r["arm"] == arm and "answer" in r]
            cells.append(f"{sum(1 for r in mine if r['correct'])}/{len(mine)}")
        print(f"{q[:22]:<46}{cells[0]:>7}{cells[1]:>7}{cells[2]:>7}{'O' if retr[qi]['found'] else 'X':>6}")

    print("\n문서 밖 질문: 모른다고 답한 비율 (높을수록 지어내지 않음)")
    for arm in ("plain", "rag", "full"):
        mine = [r for r in rows if r["arm"] == arm and not r["in_doc"] and "answer" in r]
        ab = sum(1 for r in mine if r["abstain"])
        print(f"{arm:<6}{ab}/{len(mine)}")
    print("\n※ 채점은 글자 규칙이다. 틀리게 센 경우가 있을 수 있으니 --out 파일의 답을 몇 개 직접 읽어 확인한다.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", default="claude", help="채팅 모델 (기본 claude)")
    ap.add_argument("--reps", type=int, default=3, help="같은 질문을 반복하는 횟수")
    ap.add_argument("--k", type=int, default=3, help="rag 가 가져올 문단 수")
    ap.add_argument("--out", default="rag_eval_result.json", help="전체 답변을 저장할 파일 (빈 문자열이면 저장 안 함)")
    ap.add_argument("--selftest", action="store_true", help="채점 규칙만 확인하고 끝낸다 (API 호출 없음)")
    args = ap.parse_args()
    if args.selftest:
        selftest()
    else:
        run(args)


if __name__ == "__main__":
    main()
