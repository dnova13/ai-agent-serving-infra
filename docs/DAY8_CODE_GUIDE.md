# Day 8 코드 가이드 — 문서를 벡터로 바꿔 Qdrant 에 넣기

> 실행하는 방법과 확인 항목은 [DAY8_GUIDE.md](DAY8_GUIDE.md). 이 문서는 적재 코드가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **6333 = Qdrant**. Gemini 임베딩 API 를 호출하므로 `.env` 의 `GOOGLE_API_KEY` 가 필요하다.
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 당시 실행 결과(24건 적재, 대시보드 점 24개)는 DAY8_GUIDE 의 "실행 결과"에 있고, 같은 가이드에 멱등성(2회 실행)과 벡터 길이 출력은 "아직 확인하지 않은 것"으로 적혀 있다.

**전체 목적:** 가상 문서 24건이 임베딩(768차원)을 거쳐 Qdrant 에 저장되기까지의 코드 순서와, 다시 실행해도 중복되지 않는 이유를 따라간다.


## 1. 전체 흐름

```
python -m app.services.qdrant_ingest            (프로젝트 루트에서)
  ▼
ingest(docs=SAMPLE_DOCS)
  ① get_client(), ensure_collection(client)       컬렉션이 없으면 만든다 (Day 7)
  ② embed_documents([각 문서의 text])              24개 문장을 한 번에 임베딩 → 768개 숫자 24묶음
  ③ 문서마다 PointStruct 만들기
        id      = uuid5(문서 id)                    같은 문서 id 면 늘 같은 값
        vector  = 위 임베딩
        payload = {doc_id, text, category, source}
  ④ client.upsert(collection, points)               있으면 덮어쓰고 없으면 넣는다
  ▼
__main__ 이 저장 건수와 샘플 한 건(벡터 길이)을 출력
```

| 단계 | 하는 일                          | 파일                                              | 함수                                  |
| ---- | -------------------------------- | ------------------------------------------------- | ------------------------------------- |
| A    | 가상 문서 24건                   | `d2-ai-python/app/data/sample_docs.py`            | `SAMPLE_DOCS`                         |
| B    | 텍스트 → 벡터                    | `d2-ai-python/app/services/embedding.py`          | `embed_documents`, `_embeddings`      |
| C    | 문서 id → Qdrant 점 id           | `d2-ai-python/app/services/qdrant_ingest.py`      | `to_point_id`                         |
| D    | 임베딩과 저장                    | `d2-ai-python/app/services/qdrant_ingest.py`      | `ingest`                              |


## 2. 주요 코드

### 2-1. 문서 데이터 (`sample_docs.py`)

```python
SAMPLE_DOCS = [
    {"id": "refund-001", "category": "refund", "source": "policy", "text": "환불은 상품 수령 후 7일 이내에 신청할 수 있습니다. ..."},
    ...
]
```

| 필드       | 설명                                                                                                  |
| ---------- | ----------------------------------------------------------------------------------------------------- |
| `id`       | 고정 문자열. Qdrant 의 점 id 를 여기서 만든다. 같은 `id` 로 다시 넣으면 덮어쓴다                       |
| `category` | `refund`, `shipping`, `exchange`, `payment`, `member`, `review`. Day 9 의 카테고리 필터 검색에 쓴다    |
| `source`   | 규정(`policy`), 자주 묻는 질문(`faq`), 리뷰 구분                                                       |
| `text`     | 벡터로 바뀌는 본문. "돈 돌려받기" 처럼 문서에 없는 표현으로 물어도 찾을 수 있게 문장을 다양하게 썼다    |

DAY8_GUIDE 는 24건의 구성을 환불 5, 배송 5, 교환 2, 결제 3, 회원 3, 리뷰 6 으로 적고 있다.

### 2-2. 임베딩 (`embedding.py`)

```python
EMBEDDING_MODEL = "models/gemini-embedding-001"
EMBEDDING_DIM = 768

def embed_documents(texts):
    return _embeddings().embed_documents(texts, task_type="RETRIEVAL_DOCUMENT", output_dimensionality=EMBEDDING_DIM)

def embed_query(text):      # Day 9 의 검색에서 사용
    return _embeddings().embed_query(text, task_type="RETRIEVAL_QUERY", output_dimensionality=EMBEDDING_DIM)
```

| 부분                          | 설명                                                                                                                  |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `output_dimensionality=768`   | 이 모델의 기본 출력은 3072차원이라 줄여서 쓴다. Day 7 의 `VECTOR_SIZE` 와 같아야 한다. 적용이 안 되면 `expected dim: 768, got 3072` 오류가 난다(DAY8_GUIDE 6번) |
| `task_type` 을 둘로 나눔      | 저장하는 문서는 `RETRIEVAL_DOCUMENT`, 검색하는 질문은 `RETRIEVAL_QUERY`. 같은 모델이라도 용도에 맞춰 만들면 검색이 더 잘 맞는다는 것이 코드 주석의 설명이다 |
| 배치로 한 번에                | 24건을 한 번의 호출로 보낸다. 건별 호출보다 빠르고 무료 한도(429)에 덜 걸린다는 것이 코드 주석의 설명이다            |

### 2-3. 점 id 와 저장 (`qdrant_ingest.py`)

```python
def to_point_id(doc_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))      # 같은 doc_id → 항상 같은 UUID

points = [PointStruct(id=to_point_id(d["id"]), vector=vec,
                      payload={"doc_id": d["id"], "text": d["text"], "category": d["category"], "source": d["source"]})
          for d, vec in zip(docs, vectors)]
client.upsert(collection_name=COLLECTION_NAME, points=points)
```

| 부분                       | 설명                                                                                                                         |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `uuid5`                    | 입력이 같으면 결과가 같은 UUID. 그래서 같은 문서를 다시 적재해도 새 점이 생기지 않고 덮어쓴다(멱등). Spring 에서 같은 PK 로 `save()` 하는 것과 같다고 DAY8_GUIDE 가 설명한다 |
| payload 에 `text` 저장     | 검색 결과에서 원문을 바로 돌려줄 수 있다. Qdrant 에는 벡터와 함께 원문이 들어 있다                                              |
| `upsert`                   | 점 id 가 있으면 갱신, 없으면 추가                                                                                              |
| import 의 `try/except`     | 서버(`app` 폴더에서 실행)와 `-m` 실행(프로젝트 루트) 둘 다 되도록 `services...` 와 `app.services...` 를 둘 다 시도한다        |


## 3. 결과를 읽는 순서

1. 출력의 `upsert 요청: 24건 / 컬렉션 저장 건수: 24건` 이 같은 값인가.
2. 샘플 줄의 벡터 길이가 768 인가.
3. 한 번 더 실행해도 저장 건수가 24 로 그대로인가(멱등).
4. 건수가 0 으로 보이면 조회한 컬렉션 이름이 `qdrant_setup.COLLECTION_NAME`(`documents-12`)과 같은지 본다. DAY8_GUIDE 에서 `documents` 를 조회해 0건으로 보인 적이 있다.


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                         |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 실행                          | 이 문서를 쓰면서 스크립트를 실행하지 않았다. Gemini 와 Qdrant 에 접속하지 않았다                                                              |
| 멱등성과 벡터 길이 출력       | DAY8_GUIDE 의 "아직 확인하지 않은 것"에 당시에도 미확인으로 적혀 있다. 이 문서도 코드에서 읽은 설명이다                                         |
| 문서 24건의 내용              | 데이터 파일의 `id` 개수가 24 개인 것만 세었다. 카테고리별 건수(2-1 의 구성)는 DAY8_GUIDE 의 기록을 옮겼다                                    |
