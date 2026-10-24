# Day 7 실행 가이드 — Qdrant Collection 생성

## 목표

2주 차(Qdrant RAG 연동)의 첫날. Qdrant를 기동하고 벡터를 담을 Collection
`documents`를 만든 뒤, 대시보드(`http://localhost:6333/dashboard`)에서 확인한다.

---

## Qdrant란?

**벡터 DB**. 텍스트를 임베딩 모델로 숫자 배열(벡터)로 바꿔 저장해 두고,
"키워드 일치"가 아니라 "의미 유사도"로 검색한다.

- 문서: "환불은 7일 이내 가능합니다" / 질문: "물건 돌려보내고 싶어요"
- 키워드 검색은 겹치는 단어가 없어 실패하지만, 벡터 검색은 의미가 가까워 찾아낸다.

RAG 흐름:

1. 임베딩 — 문서를 벡터로 변환 (Day 8)
2. 저장(upsert) — 벡터 + 원문을 Qdrant에 적재 (Day 8)
3. 검색 — 질문도 벡터로 바꿔 가장 가까운 문서 N개 조회 (Day 9)
4. 답변 — 찾은 문서를 LLM에 전달해 근거 기반으로 답변 (Day 10~11)

현재 에이전트의 `search_vector_db` 도구는 mock이고, Day 10에 실제 Qdrant 호출로 교체한다.

---

## 결정 사항

| 항목            | 값          | 이유                                                                     |
| --------------- | ----------- | ------------------------------------------------------------------------ |
| Collection 이름 | `documents` | 범용 문서/리뷰 적재용                                                    |
| 벡터 크기       | `768`       | Day 8에서 `gemini-embedding-001`을 `output_dimensionality=768`로 쓸 예정 |
| 거리 함수       | `Cosine`    | 텍스트 임베딩의 일반적 선택                                              |

> ⚠️ **벡터 크기는 임베딩 모델의 출력 차원과 반드시 같아야 한다.**
> 생성 후 변경 불가이며, 다른 모델로 바꾸면 Collection을 지우고 다시 만들어야 한다.

---

## 작업 순서

### 1) Qdrant 기동

```bash
cd archive/d1-ai-agent
docker compose up -d qdrant      # 새로 만들 때
docker start qdrant              # 이미 컨테이너가 있을 때
curl localhost:6333/collections  # JSON이 나오면 정상
```

- compose 전체(`up -d`)를 올리면 kafka-ui의 8080 포트 등이 다른 컨테이너와
  충돌할 수 있으니 `qdrant`만 지정해서 올린다.
- 데이터는 Docker 볼륨 `d1-ai-agent_qdrant_data`에 보존된다.

### 2) 클라이언트 설치

```bash
cd d2-ai-python
venv/bin/python -m pip install qdrant-client    # requirements.txt에 반영됨
```

### 3) Collection 생성

**방법 A. 코드 (권장)** — `app/services/qdrant_setup.py`

```bash
## 가상환경 킨후
python -m app.services.qdrant_setup
```

핵심 코드:

```python
client.create_collection(
    collection_name="documents",
    vectors_config=VectorParams(size=768, distance=Distance.COSINE),
)
```

`collection_exists()`로 먼저 확인하므로 여러 번 실행해도 안전하다(멱등).

**방법 B. 대시보드** — Collections → Create Collection → 이름 `documents`,
크기 `768`, 거리 `Cosine`.

**방법 C. 대시보드 Console(REST)**

```
PUT collections/practice
{ "vectors": { "size": 4, "distance": "Dot" } }
GET collections
DELETE collections/practice
```

### 4) 확인

- 대시보드 → `documents` → `Vectors: 768, Cosine`, `Points: 0`
- `curl localhost:6333/collections/documents`

---

## 트러블슈팅

### `Connection refused` (Python에서만 실패, curl은 성공)

`.env`의 `QDRANT_PORT`가 `63331`로 오타였다. Qdrant REST 포트는 `6333`이다.

```
QDRANT_HOST=localhost
QDRANT_PORT=6333
```

curl은 포트를 직접 쳤기 때문에 성공하고, Python은 `.env`를 읽기 때문에
실패했다. 이런 증상이면 서버보다 `.env`부터 의심할 것.

### Pylance `Import "qdrant_client" could not be resolved`

코드 문제가 아니라 VS Code가 venv 인터프리터를 못 잡은 것.
인터프리터를 `d2-ai-python/venv/bin/python`으로 선택하면 사라진다.

### 예전 `test_collection`(4차원)이 남아 있음

Day 1 무렵의 실험용. 현재 작업과 무관하며 필요 없으면 삭제해도 된다.

---

## 완료 기준

- [x] Qdrant 기동 (v1.19.1)
- [x] `documents` Collection 생성 (768, Cosine)
- [x] 스크립트 재실행 시 "이미 존재함" 확인 (멱등)
- [ ] 대시보드에서 육안 확인 ← 직접 확인 필요

## 다음 (Day 8)

가상 데이터셋 준비 → `gemini-embedding-001`(768차원)으로 임베딩 → `documents`에 upsert.
