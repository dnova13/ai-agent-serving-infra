# Day 9 코드 가이드 — 질문으로 문서 찾기와 정확도 확인

> 실행하는 방법과 확인 항목은 [DAY9_GUIDE.md](DAY9_GUIDE.md), 검색 개념은 [CONCEPTS_RAG.md](CONCEPTS_RAG.md). 이 문서는 검색 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **6333 = Qdrant**. 질문마다 Gemini 임베딩 API 를 호출하므로 `.env` 의 `GOOGLE_API_KEY` 가 필요하다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 점수와 정확도(top-1 11/12)는 DAY9_GUIDE 의 "실행 결과"에 당시 기록으로 있는 값을 옮겼다.

**전체 목적:** 질문 한 줄이 벡터로 바뀌어 Qdrant 에서 가까운 문서로 돌아오기까지의 코드 순서와, 정확도를 재는 스크립트가 무엇을 세는지 따라간다.


## 1. 전체 흐름

```
search(query, limit=3, category=None, score_threshold=None)
  ① embed_query(query)               질문 → 768개 숫자 (Gemini API 호출, RETRIEVAL_QUERY)
  ② 카테고리 필터 만들기 (선택)        category 가 있으면 payload 의 category 가 같은 문서만 후보
  ③ query_points(...)                 Qdrant 가 가장 가까운 top-k 를 점수 순으로 반환
  ④ [{score, doc_id, category, source, text}, ...] 로 변환
  ▼
format_for_llm(hits)                  Day 10 의 도구가 LLM 에게 줄 문자열로 바꾼다
```

| 단계 | 하는 일                          | 파일                                              | 함수                         |
| ---- | -------------------------------- | ------------------------------------------------- | ---------------------------- |
| A    | 질문 임베딩                      | `d2-ai-python/app/services/embedding.py`          | `embed_query` (Day 8 에서 만듦) |
| B    | 검색                             | `d2-ai-python/app/services/qdrant_search.py`      | `search`                     |
| C    | LLM 에게 줄 문자열               | `d2-ai-python/app/services/qdrant_search.py`      | `format_for_llm`             |
| D    | 정확도 측정                      | `d2-ai-python/app/experiments/search_eval.py`        | `run`, `EVAL_SET`            |


## 2. 주요 코드

### 2-1. 검색 (`search`)

```python
vector = embed_query(query)                                       # ①
query_filter = Filter(must=[FieldCondition(key="category", match=MatchValue(value=category))]) if category else None   # ②
result = get_client().query_points(collection_name=COLLECTION_NAME, query=vector, query_filter=query_filter,
                                   limit=limit, score_threshold=score_threshold, with_payload=True)   # ③
return [{"score": round(p.score, 4), "doc_id": p.payload.get("doc_id"), ...} for p in result.points]   # ④
```

| 부분                     | 설명                                                                                                                              |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `embed_query` 가 시간의 대부분 | 코드 주석: Qdrant 검색 자체는 수 ms 이고, 외부 API 를 부르는 임베딩이 검색 시간 대부분이다. 나중에 부하 테스트에서 병목을 볼 때 중요한 지점이라고 적혀 있다 |
| `query_points`           | `qdrant-client` 1.19 의 검색 API 다. 옛 `client.search()` 는 없어서 `AttributeError` 가 난다(DAY9_GUIDE 7번)                        |
| 필터                     | 후보를 먼저 거른 뒤 그 안에서 가까운 것을 찾는다. 필터를 걸어도 `limit` 개수를 채워 돌려준다는 것이 코드 주석의 설명이다            |
| `score_threshold`        | 기본값 `None`(끔). 정답 점수(0.71~0.82)와 오답 점수가 겹쳐서, 무관한 질문의 점수를 보기 전까지는 정하지 않기로 했다고 코드에 적혀 있다 |
| `score`                  | 코사인 유사도. 높을수록 가깝다. 결과는 점수 내림차순이라 `[0]` 이 가장 가까운 문서다                                                |
| 빈 결과                  | 필터나 임계값에 걸리면 빈 리스트가 돌아온다                                                                                         |

### 2-2. LLM 에게 줄 형태 (`format_for_llm`)

```python
if not hits:
    return "관련 문서를 찾지 못했습니다."
return "\n".join(f"[문서 {i}] {h['text']}" for i, h in enumerate(hits, 1))
```

빈 결과를 빈 문자열로 주면 모델이 근거 없이 지어낼 수 있어서 "찾지 못했다"고 명시한다. 번호를 붙이면 모델이 "[문서 1] 에 따르면" 처럼 근거를 구분해 인용할 수 있다.

### 2-3. 정확도 측정 (`search_eval.py`)

```python
EVAL_SET = [("돈 돌려받고 싶어요", {"refund"}), ("택배 언제 와요?", {"shipping"}), ... ]   # 질문 12개와 정답 카테고리

for q, accept in EVAL_SET:
    hits = search(q, limit=3)
    cats = [h["category"] for h in hits]
    ok1 = bool(cats) and cats[0] in accept       # top-1: 1위가 정답
    ok3 = any(c in accept for c in cats)         # top-3: 3건 안에 정답이 있음
    mark = "O" if ok1 else ("△" if ok3 else "X")
    time.sleep(0.3)
```

| 부분                         | 설명                                                                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| 질문 작성 원칙               | 문서에 있는 단어를 그대로 쓰지 않았다("돈 돌려받고"↔환불, "비번"↔비밀번호, "택배"↔배송). 키워드 검색이면 못 찾았을 표현이어야 의미 검색의 가치가 보인다 |
| 정답이 둘인 질문             | `accept` 에 카테고리를 둘 넣었다("다른 사이즈로 바꾸고 싶어요"는 `exchange`, `review`)                                            |
| O / △ / X                    | 1위 정답 / 1위는 틀렸지만 3위 안에 정답 / 3위 안에도 없음                                                                          |
| `time.sleep(0.3)`            | 질문마다 임베딩 API 를 한 번씩 부르므로 무료 한도(429)에 덜 걸리게 쉬는 것                                                         |

DAY9_GUIDE 의 당시 기록: top-1 11/12 (92%), △ 1건("택배 언제 와요?"의 1위가 배송 규정이 아니라 배송 후기 `review-001`). 점수는 정답 1위가 0.71~0.82, 틀린 1위가 0.7359 로 같은 구간이라, 점수만으로 관련 여부를 가르기 어렵다고 분석했다. 문서 24건, 질문 12개라서 방향 확인용 수치이고 성능 지표로 쓸 수 없다고도 적었다.


## 3. 결과를 읽는 순서

1. `qdrant_search "돈 돌려받고 싶어요"` 의 1~3위가 환불 문서인가.
2. `search_eval` 의 `[O]`, `[△]`, `[X]` 표시와 끝의 `top-1 정확도`.
3. 틀린 질문은 `정답` 과 `1위` 줄을 비교해 어떤 카테고리가 섞였는지 본다.
4. 결과가 계속 빈 리스트이면 컬렉션 이름과 적재 건수부터 본다(DAY9_GUIDE 7번).


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                      |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                          | 이 문서를 쓰면서 검색이나 평가를 실행하지 않았다. 숫자는 DAY9_GUIDE 의 당시 기록이다                                                          |
| top-3 정확도                  | DAY9_GUIDE 에 top-3 줄이 출력에서 빠져 기록되지 않았다고 적혀 있다(△ 1건이라 12/12 로 추정한 것도 추정이다)                                   |
| `score_threshold`             | 정하지 않았다. 무관한 질문의 점수는 이 Day 에는 보지 않았다(Day 11 에서 다룬다)                                                             |
| 같은 Day 의 `llm_check.py`    | 이 Day 의 커밋에 함께 들어 있지만 DAY9_GUIDE 가 다루지 않아 이 문서 범위 밖이다([LLM_CHECK.md](LLM_CHECK.md) 참고)                           |
