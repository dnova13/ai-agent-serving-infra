# Day 29 코드 가이드 — hnsw_experiment.py (Qdrant HNSW 파라미터 실험)

> 실행 방법과 확인 항목은 [DAY29_GUIDE.md](DAY29_GUIDE.md), 결과는 [DAY29_RESULT.md](DAY29_RESULT.md). 이 문서는 **스크립트가 어떤 순서로 무엇을 하는지**를 함수 단위로 따라간다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름으로 찾는다.
> 포트: **6333 = Qdrant(REST)**, 6334 = Qdrant(gRPC).

**목적:** HNSW 인덱스 설정(`m`, `ef_construct`)과 검색 설정(`hnsw_ef`)을 바꿔 가며 검색 정확도(recall), 검색 속도(p50, p95, QPS), 인덱스 만드는 시간이 어떻게 달라지는지 잰다.

**Day 29 의 코드 변경:** 새 파일 `d2-ai-python/app/experiments/hnsw_experiment.py` 하나(커밋 `add2c1e`). 서버 코드와 기존 `qdrant_setup.py`, `rag` 코드는 바꾸지 않았다. 실험용 컬렉션은 접두사 `hnsw-exp-` 를 붙여 기존 `documents` 컬렉션과 섞이지 않는다.


## 1. 전체 흐름

```
python -m app.experiments.hnsw_experiment sweep --m 8,16,32,64 --ef-construct 100 --ef 64
        │
        ├─ (m, ef_construct) 조합마다
        │    ├─ build   : 컬렉션 만들기 → 합성 벡터 올리기(인덱스 끔) → 인덱스 켜기 → 완성 대기, 시간 재기
        │    ├─ search  : exact 검색으로 정답 만들기 → hnsw_ef 별로 recall, 지연 시간, QPS 재기
        │    └─ 컬렉션 삭제 (--keep 을 주면 남김)
        └─ 인덱스 표와 검색 표를 모아 출력

명령: build / search / sweep / drop
```

| 단계 | 하는 일                                             | 함수 (파일: `d2-ai-python/app/experiments/hnsw_experiment.py`) |
| ---- | --------------------------------------------------- | ----------------------------------------------------------- |
| A    | Qdrant 접속                                         | `get_client`                                                |
| B    | 합성 벡터 만들기 (클러스터 중심, 점, 질의)          | `_unit`, `make_centers`, `make_points`, `make_queries`      |
| C    | 컬렉션 만들기 → 올리기 → 인덱스 만들기 시간 재기    | `build`, `wait_until_indexed`                               |
| D    | 정답(exact) 만들고 `hnsw_ef` 별 recall, 지연 재기   | `search`, `percentile`                                      |
| E    | 표 출력, 인자 해석                                  | `print_table`, `parse_ints`                                 |
| F    | 명령 분기(build, search, sweep, drop)               | `main`                                                      |


## 2. 주요 코드

### 2-1. 합성 벡터 (B)

**목적:** 같은 시드면 항상 같은 데이터가 나오게 하고, 실제 임베딩처럼 "비슷한 것끼리 모인" 모양으로 만든다.

| 함수            | 내용                                                                                                                                                       |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_unit`         | 벡터를 길이 1 로 맞춘다 (코사인 거리를 쓰므로)                                                                                                             |
| `make_centers`  | 표준정규분포 난수로 클러스터 중심 `clusters` 개(기본 100)를 만들어 길이를 1로 맞춘다                                                                         |
| `make_points`   | 점마다 중심 하나를 무작위로 고르고, `noise`(기본 3.0)를 차원 수의 제곱근으로 나눈 크기의 정규분포 잡음을 더한 뒤 길이를 1로 맞춘다. `noise` 가 클수록 중심에서 덜 모인다. 올릴 때는 배치마다 시드를 달리(`seed * 1_000_003 + start`) 해서 배치끼리 같은 점이 나오지 않게 한다 |
| `make_queries`  | 데이터와 같은 중심(같은 시드)에서 다른 시드(`seed + 7919`)로 질의 벡터를 뽑는다. 질의가 데이터와 같은 분포에 있어야 recall 이 의미가 있다                       |

**왜 합성 벡터인가:** 현재 문서 조각은 수십 개뿐이라 HNSW 설정을 바꿔도 차이가 보이지 않는다. 큰 데이터를 Gemini 로 임베딩하면 API 한도(429)에 걸린다. 합성 벡터는 API 호출 없이 몇십만 개를 만들 수 있다. 대신 "실제 문서의 검색 품질"은 이 실험으로 알 수 없다(스크립트 맨 위 docstring 에도 적었다).

### 2-2. build — 인덱스 만드는 시간만 따로 재기 (C)

**목적:** 올리는 시간과 인덱스 만드는 시간이 섞이지 않게 한다.

```python
client.create_collection(
    collection_name=coll,
    vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
    hnsw_config=HnswConfigDiff(m=m, ef_construct=ef_construct),
    optimizers_config=OptimizersConfigDiff(indexing_threshold=0),     # (a) 올리는 동안 인덱스 끔
)
...  # upsert 를 UPLOAD_BATCH 개씩, wait=True 로
client.update_collection(collection_name=coll, optimizers_config=OptimizersConfigDiff(indexing_threshold=20000))   # (b)
wait_until_indexed(client, coll, n)                                                                                 # (c)
```

| 표시 | 하는 일                                                                                                                                                                        |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| (a)  | `indexing_threshold=0` 이면 옵티마이저가 인덱스를 만들지 않는다. 올리는 동안 HNSW 가 조금씩 만들어져 시간이 섞이는 것을 막는다                                                  |
| (b)  | 다 올린 뒤 기준을 기본값(20000KB)으로 되돌린다. 이 순간 옵티마이저가 인덱스를 만들기 시작하므로, 여기서부터가 "인덱스 만드는 시간"이다                                           |
| (c)  | `wait_until_indexed` 가 0.5초마다 컬렉션 정보를 읽어 상태가 green 이고 `indexed_vectors_count >= n` 이 될 때까지 기다린다. 5초마다 진행 상황을 찍고, 제한 시간을 넘으면 `TimeoutError` |

반환하는 `row` 에는 `upload_sec`, `index_sec`, `points`, `indexed`, `segments`, `status` 가 들어 있다. `segments` 가 여러 개면 인덱스가 조각마다 만들어진 것이라, 결과를 읽을 때 같이 본다.

### 2-3. search — recall 과 지연 시간 (D)

**목적:** 근사 검색(HNSW)이 정확한 검색의 상위 K 개를 얼마나 찾는지(recall)와 그때 걸리는 시간을 `hnsw_ef` 별로 잰다.

```python
# 정답: exact=True 는 HNSW 를 쓰지 않고 전부 비교한다
res = client.query_points(coll, query=q.tolist(), limit=limit, search_params=SearchParams(exact=True))
truth.append({p.id for p in res.points})
...
for ef in efs:
    ... 준비 운동 10건 버림 ...
    res = client.query_points(coll, query=..., limit=limit, search_params=SearchParams(hnsw_ef=ef))
    recalls.append(len({p.id for p in res.points} & truth[qi]) / limit)
```

| 지표                     | 계산                                                                               |
| ------------------------ | ---------------------------------------------------------------------------------- |
| `recall` (recall@limit)  | 질의마다 (근사 결과 ∩ 정답) / limit 을 구해 평균. `min_recall` 은 가장 나쁜 질의    |
| `p50_ms`, `p95_ms`       | 질의 한 건씩 `time.perf_counter()` 로 잰 시간의 중앙값과 95번째 백분위수            |
| `qps`                    | 전체 질의 수 / 전체 걸린 시간. 클라이언트 한 개가 차례로 보낸 값이라 서버의 최대 처리량이 아니다 |
| `repeat`                 | 같은 질의 묶음을 여러 번 보내 표본을 늘린다                                         |

정답을 `hnsw_ef` 반복 밖에서 한 번만 만든다. `exact=True` 검색은 느리지만 질의 수만큼만 하면 되고, 모든 `ef` 값이 같은 정답과 비교된다.

### 2-4. 명령 (F)

| 명령     | 하는 일                                                                                                  |
| -------- | -------------------------------------------------------------------------------------------------------- |
| `build`  | `--name`, `--m`, `--ef-construct`, `--n` 으로 컬렉션 하나를 만든다                                        |
| `search` | 이미 만든 컬렉션에서 `--ef` 목록(`16,32,64,...`)으로 recall 과 지연을 잰다                                |
| `sweep`  | `--m` 목록 × `--ef-construct` 목록의 모든 조합에 대해 `build` 후 `search`, 컬렉션 삭제(`--keep` 이면 생략), 표 2개(인덱스, 검색) 출력 |
| `drop`   | 접두사 `hnsw-exp-` 컬렉션을 지운다 (`--all` 이면 전부, 아니면 `--name`)                                   |

`parse_ints` 가 `"8,16,32"` 같은 쉼표 목록을 정수 리스트로 바꾼다. 공통 옵션은 `--dim`(768), `--clusters`(100), `--noise`(3.0), `--seed`(42), `--grpc`(REST 대신 6334 gRPC 사용)이고, `build` 는 `--n`(100000), `--m`(16), `--ef-construct`(100), `search` 는 `--queries`(200), `--limit`(10), `--ef`, `--repeat`(1)이 기본이다. 환경변수 `QDRANT_HOST`(기본 localhost), `QDRANT_PORT`(기본 6333) 로 접속 대상을 바꾼다.


## 3. 시간 순서 (build 한 번)

```
t=0     컬렉션 생성 (indexing_threshold=0, m, ef_construct 지정)
t=0~    upsert 배치 반복 (wait=True)                           → upload_sec
t=U     update_collection(indexing_threshold=20000)            ← 여기서부터 index_sec
t=U~    status 를 0.5초마다 확인, green + indexed >= n 이면 종료
t=U+I   row 반환 (upload_sec=U, index_sec=I)
```


## 4. 실패 지점별 동작

| 상황                                          | 동작                                                                                   |
| --------------------------------------------- | -------------------------------------------------------------------------------------- |
| 같은 이름의 컬렉션이 이미 있음                | `build` 가 지우고 다시 만든다 (`collection_exists` → `delete_collection`)               |
| 인덱스가 제한 시간(1800초) 안에 끝나지 않음   | `wait_until_indexed` 가 `TimeoutError` 를 던진다. 그 컬렉션은 `drop` 으로 지운다          |
| 컬렉션이 없는데 `search`                      | Qdrant 클라이언트 예외가 그대로 올라온다. 먼저 `build` 한다                              |
| 실험 컬렉션이 남음                            | `sweep` 은 조합마다 컬렉션(`hnsw-exp-m16-efc100` 꼴)을 지우지만 `--keep` 을 주면 남는다. `build` 로 만든 것도 남는다. 디스크와 메모리를 차지하므로 `drop --all` 로 지운다 |


## 5. 설계 선택과 이유

| 선택                                          | 이유                                                                                                  |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| 합성 클러스터 벡터                            | 순수 무작위 벡터는 점 사이 거리가 거의 같아 HNSW 가 실제보다 훨씬 어렵게 나온다(코드 주석). 중심점 주변에 점을 흩뿌려 실제 임베딩처럼 모인 분포를 흉내 낸다 |
| 시드 고정 (기본 42)                           | 같은 설정으로 다시 실험하면 같은 데이터가 나온다                                                       |
| 정답을 `exact=True` 로 만듦                   | 별도 라이브러리 없이 Qdrant 가 직접 전수 검색한 결과를 정답으로 쓴다                                    |
| 올리는 동안 인덱스 끄기                       | 인덱스 시간을 올리는 시간과 분리하려고                                                                 |
| 준비 운동 질의 버리기                         | 첫 몇 건은 캐시와 연결 때문에 느려 지연 시간 통계를 왜곡한다                                            |
| 접두사 `hnsw-exp-`                            | 실수로 `documents` 같은 실제 컬렉션을 지우지 않게 한다                                                 |


## 6. 코드를 따라 읽는 순서

1. 맨 위 docstring (용어와 명령)
2. `make_centers` → `make_points` → `make_queries`
3. `build` → `wait_until_indexed`
4. `search`
5. `main` 의 명령 분기
6. [DAY29_RESULT.md](DAY29_RESULT.md) 에서 위 함수가 낸 표와 해석 확인
