# Day 7 코드 가이드 — Qdrant 컬렉션 만들기

> 실행하는 방법과 확인 항목은 [DAY7_GUIDE.md](DAY7_GUIDE.md). 이 문서는 컬렉션 생성 코드(`qdrant_setup.py`)가 어떤 순서로 도는지 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 적지 않았다. 함수 이름으로 찾는다.
> 포트: **6333 = Qdrant(REST)**. 이 Day 에는 FastAPI 서버가 필요 없다(스크립트만 실행한다).
> 상태: 나중에 코드를 읽고 정리한 문서다. 이 문서를 쓰면서 코드를 실행해 확인한 것은 없다. 당시 실행 결과는 DAY7_GUIDE 의 완료 기준과 트러블슈팅을 따른다.

**전체 목적:** 문서를 벡터로 저장할 그릇인 Qdrant 컬렉션을, 이미 있으면 건드리지 않고 없으면 만드는 코드의 흐름과, 그 설정값(이름, 차원, 거리)이 이후 Day 에 어떻게 이어지는지 따라간다.


## 1. 전체 흐름

```
python -m app.services.qdrant_setup        (프로젝트 루트에서)
  ▼
get_client()          환경변수 QDRANT_HOST(기본 localhost), QDRANT_PORT(기본 6333) 로 접속 객체 만들기
  ▼
ensure_collection(client)
  ├─ 이미 있다 ──▶ 아무것도 안 하고 False
  └─ 없다 ───────▶ create_collection(size=768, distance=COSINE) 후 True
  ▼
상태, 벡터 설정, 점 개수, 전체 컬렉션 이름을 출력
```

| 단계 | 하는 일                          | 파일                                          | 함수                 |
| ---- | -------------------------------- | --------------------------------------------- | -------------------- |
| A    | 접속 객체 만들기                 | `d2-ai-python/app/services/qdrant_setup.py`   | `get_client`         |
| B    | 컬렉션이 없을 때만 만들기        | `d2-ai-python/app/services/qdrant_setup.py`   | `ensure_collection`  |
| C    | 만든 결과 확인 출력              | `d2-ai-python/app/services/qdrant_setup.py`   | `__main__` 블록       |


## 2. 주요 코드

### 2-1. 설정값 세 개

```python
COLLECTION_NAME = "documents-12"
VECTOR_SIZE = 768
DISTANCE = Distance.COSINE
```

| 값                | 설명                                                                                                                                  |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `COLLECTION_NAME` | 코드의 값은 `documents-12` 다. DAY7_GUIDE 의 결정 사항에는 `documents` 로 적혀 있는데, DAY8_GUIDE 의 "컬렉션 이름" 절이 이름을 바꾼 이유(수정한 내용을 이전 데이터와 섞이지 않게 새 컬렉션에서 보려고)를 설명한다 |
| `VECTOR_SIZE`     | 768. Day 8 에서 쓰는 임베딩 모델의 출력 차원과 **반드시 같아야 한다**. 다르면 적재할 때 차원 오류가 난다(DAY8_GUIDE 6번의 에러 표)  |
| `DISTANCE`        | 코사인 유사도. 벡터의 방향이 비슷할수록 가깝다고 본다. 이후 검색 결과의 `score` 가 이 값이다                                            |

### 2-2. 접속 (`get_client`)

```python
def get_client():
    return QdrantClient(host=os.getenv("QDRANT_HOST", "localhost"),
                        port=int(os.getenv("QDRANT_PORT", "6333")))
```

`.env` 의 `QDRANT_PORT` 가 `63331` 로 오타였을 때 Python 에서만 `Connection refused` 가 났던 것이 이 함수가 읽는 값 때문이다(DAY7_GUIDE 트러블슈팅). `curl` 은 포트를 직접 쓰므로 성공했다.

### 2-3. 멱등 생성 (`ensure_collection`)

```python
def ensure_collection(client, name=COLLECTION_NAME) -> bool:
    if client.collection_exists(name):
        return False
    client.create_collection(collection_name=name,
                             vectors_config=VectorParams(size=VECTOR_SIZE, distance=DISTANCE))
    return True
```

이미 있으면 만들지 않고 `False` 를 돌려준다. 몇 번 실행해도 같은 결과가 되는 것(멱등)이 목적이고, 이미 있는 컬렉션의 설정(차원, 거리)을 바꾸거나 검사하지는 않는다. 기존 컬렉션의 차원이 다르면 이 함수는 그것을 알려 주지 않는다.

### 2-4. 이 컬렉션을 쓰는 곳

| Day    | 쓰는 코드                                                            |
| ------ | -------------------------------------------------------------------- |
| Day 8  | `qdrant_ingest.ingest` 가 `ensure_collection` 을 먼저 부르고 문서를 넣는다 |
| Day 9  | `qdrant_search.search` 가 `COLLECTION_NAME` 으로 검색한다             |


## 3. 결과를 읽는 순서

1. 실행하면 `[documents-12] 생성됨` 또는 `이미 존재함` 이 나오는가.
2. `vectors` 출력이 `size=768`, `distance=Cosine` 인가.
3. `points` 가 0 인가(Day 8 에서 채우기 전).


## 4. 확인하지 않은 것

| 항목                          | 내용                                                                                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| 실행                          | 이 문서를 쓰면서 스크립트를 실행하지 않았다. Qdrant 에 접속하지 않았다                                                                       |
| 컬렉션 이름이 바뀐 시점       | 이 코드가 가장 오래된 저장소 기록에 이미 `documents-12` 로 들어 있어서, `documents` 에서 바뀐 때를 코드 기록으로는 확인하지 못했다(DAY8_GUIDE 설명을 따랐다) |
| 기존 컬렉션과 설정이 다를 때  | `ensure_collection` 이 기존 컬렉션의 차원을 검사하지 않는다는 것은 코드에서 읽은 것이고 시험하지 않았다                                       |
