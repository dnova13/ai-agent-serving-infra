# Day 8 가이드 — 가상 데이터 임베딩 + Qdrant 적재

> 상태: 완료. `documents-12` 컬렉션에 24건 적재 확인 (대시보드 Visualize 에서 점 24개).

## 1. 오늘 목표

Day 7에서 만든 빈 컬렉션(`documents-12`, 아래 "컬렉션 이름" 참고)에 **문서를 벡터로 바꿔서 채운다.**

```
SAMPLE_DOCS(24건) → 임베딩(768차원) → Qdrant upsert → 대시보드에서 24건 확인
```

검색(Day 9)과 에이전트 연결(Day 10)은 아직 하지 않는다. 오늘은 "데이터가 제대로 들어갔는가"까지.

## 2. 임베딩이 뭔가

문장을 의미가 담긴 숫자 배열로 바꾸는 것. 의미가 비슷한 문장은 숫자 배열(벡터)도 가까워진다.

| 문장 | 벡터 (예시, 실제는 768개) |
|---|---|
| "환불은 7일 이내 가능" | [0.12, -0.40, 0.88, …] |
| "돈 돌려받고 싶어요" | [0.10, -0.38, 0.85, …] ← 위와 가까움 |
| "배송이 빠릅니다" | [-0.55, 0.21, 0.03, …] ← 멀음 |

그래서 `LIKE '%환불%'` 처럼 글자가 같아야 찾는 방식과 달리, 단어가 달라도 의미로 찾는다 (Day 9에서 확인).

## 3. 만든 파일

| 파일 | 역할 |
|---|---|
| `app/data/sample_docs.py` | 쇼핑몰 CS 가상 문서 24건 (환불 5, 배송 5, 교환 2, 결제 3, 회원 3, 리뷰 6) |
| `app/services/embedding.py` | Gemini `gemini-embedding-001` 로 텍스트 → 768차원 벡터 |
| `app/services/qdrant_ingest.py` | 임베딩 + Qdrant upsert + 적재 확인 출력 |

## 4. 설계 결정과 이유

### 4-1. 벡터 차원 768
- `gemini-embedding-001` 기본 출력은 3072차원. `output_dimensionality=768` 로 줄여서 쓴다.
- 이유: 차원이 클수록 저장 공간과 검색 비용이 늘어난다. 이 규모의 CS 문서에는 768이면 충분하다.
- **컬렉션 차원(`qdrant_setup.VECTOR_SIZE`)과 임베딩 차원이 다르면 upsert가 에러**가 난다. 컬렉션은 한 번 만들면 차원을 못 바꾸므로, 바꾸려면 컬렉션을 지우고 다시 만들어야 한다.

### 4-2. task_type 을 문서/질문으로 구분
- 저장할 문서는 `RETRIEVAL_DOCUMENT`, 사용자 질문은 `RETRIEVAL_QUERY` (Day 9).
- 같은 모델이라도 "찾을 대상"과 "찾는 질문"을 다르게 임베딩하면 검색 정확도가 올라간다.

### 4-3. point ID = 문서 id 로 만든 uuid5
- Qdrant point ID는 정수 또는 UUID만 가능하다. 문자열 `"refund-001"` 은 안 된다.
- `uuid5("refund-001")` 는 항상 같은 UUID → **같은 문서를 다시 적재하면 중복 생성이 아니라 덮어쓰기.**
- 그래서 스크립트를 몇 번 다시 돌려도 컬렉션은 계속 24건이다. (Spring의 `save()` 로 같은 PK를 저장하는 것과 같다.)

### 4-4. payload 에 원문과 메타데이터 저장
- `text`(원문), `category`, `source`, `doc_id` 를 같이 저장.
- 벡터만 있으면 검색해도 "어떤 문서인지" 알 수 없다. payload가 있어야 결과로 원문을 바로 돌려준다.
- `category` 는 Day 9에서 "환불 문서만 검색" 같은 필터에 쓴다.

### 4-5. 임베딩은 배치로 한 번에
- 24건을 한 번의 호출로 보낸다. 건별 호출(24번)보다 빠르고 무료 한도(429)에 덜 걸린다.
- 문서가 수백 건 이상이 되면 배치 크기를 나눠야 한다 (Day 12 Chunking 실험에서 다룸).

## 5. 실행

```bash
cd d2-ai-python
docker compose up -d                       # Qdrant 기동 확인
source venv/bin/activate
python -m app.services.qdrant_setup        # (Day 7) 컬렉션 생성
python -m app.services.qdrant_ingest       # (Day 8) 임베딩 + 적재
```

### 정상 출력
```
upsert 요청: 24건 / 컬렉션 저장 건수: 24건
샘플: refund-001 | 벡터 길이 768 | 환불은 상품 수령 후 7일 이내에 신청할 수 있...
```

### 대시보드 확인
`http://localhost:6333/dashboard` → Collections → `documents-12`
- Points: 24
- 포인트 하나를 열면 payload(text/category/source)와 벡터가 보인다.
- Visualize 탭에서 벡터를 2D로 펼치면 카테고리별로 뭉치는지 볼 수 있다.

## 컬렉션 이름 (주의)

현재 코드의 컬렉션 이름은 **`documents-12`** 이다. (`app/services/qdrant_setup.py` 의 `COLLECTION_NAME`)

- 처음 계획은 `documents` 였는데, 수정한 내용을 이전 데이터와 섞이지 않게 **새 컬렉션에서 따로 보려고** 이름을 바꿨다.
- `qdrant_ingest.py` 는 `qdrant_setup.COLLECTION_NAME` 을 가져다 쓰므로, **적재된 24건은 `documents-12` 에 있고 `documents` 는 비어 있다.**
- 현재 Qdrant 에 컬렉션이 3개 있다: `documents`(비어 있음), `documents-12`(Day 8 적재), `test_collection`(이전 실험).

### 겪은 문제: 24건이 안 보임
- 증상: `curl .../collections/documents/points/count` 가 0, `scroll` 도 `points: []`.
- 원인: **조회한 컬렉션 이름이 코드의 이름과 달랐다.** 데이터가 사라진 게 아니라 다른 컬렉션을 보고 있었다.
- 확인 방법: 먼저 컬렉션 목록과 코드의 이름을 맞춰본다.
  ```bash
  curl -s http://localhost:6333/collections
  grep -n "COLLECTION_NAME =" app/services/qdrant_setup.py
  ```
- 교훈: 이름은 **한 곳(`qdrant_setup.COLLECTION_NAME`)에서만 정의**하고, 검색(Day 9)과 에이전트 도구(Day 10)도 모두 이걸 import 해서 쓴다. 문서나 curl 에 이름을 직접 쓰면 이렇게 어긋난다.
- 볼륨 설정(`qdrant_data:/qdrant/storage`) 은 정상이라 컨테이너를 재시작해도 데이터는 남는다. 데이터가 사라지는 경우는 `docker compose down -v` 나 컬렉션 삭제뿐이다.

### 확인 명령 (컬렉션 이름 포함)
```bash
# 전체 건수 (기대값 24)
curl -s -X POST http://localhost:6333/collections/documents-12/points/count \
  -H "Content-Type: application/json" -d '{"exact": true}'

# 카테고리 필터 (refund 기대값 5)
curl -s -X POST http://localhost:6333/collections/documents-12/points/count \
  -H "Content-Type: application/json" \
  -d '{"filter": {"must": [{"key": "category", "match": {"value": "refund"}}]}, "exact": true}'
```

## 6. 자주 나는 에러

| 증상 | 원인 / 해결 |
|---|---|
| `ModuleNotFoundError: dotenv` | venv가 활성화되지 않았거나 깨짐. venv를 다시 만든다 |
| `Connection refused` (6333) | Qdrant 컨테이너가 안 떠 있음. `docker compose up -d` |
| `Vector dimension error: expected dim: 768, got 3072` | `output_dimensionality` 가 적용되지 않음. `embedding.py` 의 인자 확인 |
| `429 RESOURCE_EXHAUSTED` | Gemini 무료 한도. 잠시 후 재시도 |
| 적재 건수가 24가 아님 | id 중복 여부 확인 (`SAMPLE_DOCS` 의 id 는 모두 고유해야 함) |

## 7. 오늘 확인할 것 (체크리스트)

- [ ] 적재 후 컬렉션 건수가 24건
- [ ] 스크립트를 두 번 실행해도 24건 유지 (idempotent)
- [ ] 벡터 길이가 768
- [ ] 대시보드에서 payload 원문이 보임

## 8. 다음 (Day 9)

질문을 `RETRIEVAL_QUERY` 로 임베딩해서 가장 가까운 문서를 찾는 `search()` 함수를 만든다.
"돈 돌려받고 싶어요" 처럼 **문서에 없는 표현으로 물어도** 환불 규정이 나오는지 확인하는 것이 핵심.

## 실행 결과

- 실행: `python -m app.services.qdrant_ingest`
- 대시보드 확인: Collections → `documents-12` → Visualize → RUN 시 **점 24개** (문서 1건 = 점 1개).
  - Visualize 는 768차원 벡터를 UMAP 으로 2차원에 펼친 것. 점이 가까울수록 의미가 비슷하다.
  - RUN 은 버튼이 아니라 코드 5번 줄 `{` 바로 위의 작은 회색 글씨 (또는 JSON 안쪽 클릭 후 `Cmd + Enter`).
  - 카테고리별 색: 요청에 `"color_by": {"payload": "category"}` 추가.
- 확인 시 겪은 일: 조회한 컬렉션 이름이 `documents` 로 코드(`documents-12`)와 달라 0건으로 보였음 (위 "컬렉션 이름" 참고).

### 아직 확인하지 않은 것
- 스크립트 2회 실행 시 24건 유지 (멱등성)
- `refund` 카테고리 count 5 (curl)
- 벡터 길이 768 출력

→ Day 9 검색 작업 전에 한 번에 확인한다.
