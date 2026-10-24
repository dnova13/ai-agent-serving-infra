"""
HNSW 파라미터 실험 (Day 29)

[이 스크립트가 하는 일]
  Qdrant 의 HNSW 인덱스 설정(m, ef_construct)과 검색 설정(hnsw_ef)을 바꿔 가며
  "검색 정확도(recall)"와 "검색 속도(지연 시간, QPS)", "인덱스 만드는 시간"이 어떻게 달라지는지 잰다.
  실제 임베딩(Gemini)을 쓰지 않고 합성 벡터를 쓴다. 이유: 현재 문서는 조각이 수십 개뿐이라 HNSW 설정을 바꿔도 차이가
  보이지 않고, 큰 데이터를 임베딩하려면 API 한도(429)에 걸린다. 합성 벡터는 시드가 같으면 항상 같은 값이 나온다.
  다음은 하지 않는다: 실제 문서 의미 검색 품질 평가, 양자화(quantization), 필터 검색.

[용어]
  m             한 점이 이웃으로 연결하는 간선 수. 클수록 정확하고 메모리와 인덱스 크기가 커진다 (Qdrant 기본 16)
  ef_construct  인덱스를 만들 때 후보를 살펴보는 폭. 클수록 정확하고 만드는 시간이 길다 (Qdrant 기본 100)
  hnsw_ef       검색할 때 후보를 살펴보는 폭. 클수록 정확하고 느리다. 검색 요청마다 바꿀 수 있다
  recall@K      근사 검색(HNSW)이 찾은 K 개 중, 정확한 검색(exact)이 찾은 상위 K 개와 겹치는 비율의 평균

[명령]  (d2-ai-python 폴더에서 실행)
  build   컬렉션을 만들고 합성 벡터를 넣은 뒤 인덱스가 완성될 때까지 기다리며 시간을 잰다
  search  이미 만든 컬렉션에서 hnsw_ef 를 바꿔 가며 recall 과 지연 시간을 잰다
  sweep   (m, ef_construct) 조합마다 build 후 search 를 이어서 한다. 결과를 표로 모아 보여 준다
  drop    실험용 컬렉션(접두사 hnsw-exp-)을 지운다

[실행 예]
  python -m app.experiments.hnsw_experiment build --name base --m 16 --ef-construct 100 --n 100000
  python -m app.experiments.hnsw_experiment search --name base --ef 16,32,64,128,256,512
  python -m app.experiments.hnsw_experiment sweep --m 8,16,32,64 --ef-construct 100 --ef 64
  python -m app.experiments.hnsw_experiment drop --all

환경변수: QDRANT_HOST(기본 localhost), QDRANT_PORT(기본 6333)
"""

import argparse
import json
import os
import statistics
import time

import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    HnswConfigDiff,
    OptimizersConfigDiff,
    PointStruct,
    SearchParams,
    VectorParams,
)

PREFIX = "hnsw-exp-"  # 실험용 컬렉션 이름 앞에 붙이는 접두사. 기존 컬렉션(documents-12 등)과 섞이지 않게 한다
DEFAULT_DIM = 768  # 프로젝트 임베딩 차원(embedding.EMBEDDING_DIM)과 같게 둔다
UPLOAD_BATCH = 1000
INDEX_WAIT_TIMEOUT_SEC = 1800


def get_client(grpc: bool = False) -> QdrantClient:
    return QdrantClient(
        host=os.getenv("QDRANT_HOST", "localhost"),
        port=int(os.getenv("QDRANT_PORT", "6333")),
        grpc_port=6334,
        prefer_grpc=grpc,
        timeout=120,
    )


# ── 합성 벡터 ──────────────────────────────────────────────
# 실제 임베딩은 의미가 비슷한 글끼리 모여 있다. 그 모습을 흉내 내려고 "중심점 여러 개 주변에 점을 흩뿌린" 데이터를 만든다.
# 순수 무작위 벡터는 점 사이 거리가 거의 같아 HNSW 가 실제보다 훨씬 어렵게 나오므로 쓰지 않는다.
def _unit(x: np.ndarray) -> np.ndarray:
    return x / np.linalg.norm(x, axis=1, keepdims=True)


def make_centers(dim: int, clusters: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return _unit(rng.standard_normal((clusters, dim)).astype(np.float32))


def make_points(centers: np.ndarray, count: int, noise: float, seed: int) -> np.ndarray:
    """중심점 하나를 고르고 noise 만큼 흐트러뜨린 뒤 길이를 1로 맞춘 벡터 count 개."""
    rng = np.random.default_rng(seed)
    dim = centers.shape[1]
    pick = rng.integers(0, centers.shape[0], size=count)
    pts = centers[pick] + noise * rng.standard_normal((count, dim)).astype(np.float32) / np.sqrt(dim)
    return _unit(pts.astype(np.float32))


# ── build ─────────────────────────────────────────────────
def wait_until_indexed(client: QdrantClient, coll: str, expect_points: int) -> float:
    """인덱스가 완성될 때까지(상태 green 이고 모든 벡터가 인덱스에 들어갈 때까지) 기다리고 걸린 초를 돌려준다."""
    t0 = time.time()
    last_print = 0.0
    while True:
        info = client.get_collection(coll)
        indexed = info.indexed_vectors_count or 0
        done = str(info.status).lower().endswith("green") and indexed >= expect_points
        now = time.time()
        if now - last_print > 5:
            print(f"  ... 인덱스 대기 {now - t0:5.1f}s  status={info.status} indexed={indexed}/{expect_points}")
            last_print = now
        if done:
            return now - t0
        if now - t0 > INDEX_WAIT_TIMEOUT_SEC:
            raise TimeoutError(f"인덱스가 {INDEX_WAIT_TIMEOUT_SEC}초 안에 끝나지 않았다 (indexed={indexed})")
        time.sleep(0.5)


def build(client: QdrantClient, name: str, n: int, dim: int, m: int, ef_construct: int,
          clusters: int, noise: float, seed: int) -> dict:
    coll = PREFIX + name
    if client.collection_exists(coll):
        client.delete_collection(coll)

    # 올리는 동안에는 인덱스를 만들지 않게(indexing_threshold=0) 하고, 다 올린 뒤에 켜서 "인덱스 만드는 시간"만 따로 잰다
    client.create_collection(
        collection_name=coll,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        hnsw_config=HnswConfigDiff(m=m, ef_construct=ef_construct),
        optimizers_config=OptimizersConfigDiff(indexing_threshold=0),
    )

    centers = make_centers(dim, clusters, seed)
    t0 = time.time()
    for start in range(0, n, UPLOAD_BATCH):
        cnt = min(UPLOAD_BATCH, n - start)
        vecs = make_points(centers, cnt, noise, seed=seed * 1_000_003 + start)  # 배치마다 시드가 달라 겹치지 않는다
        client.upsert(
            collection_name=coll,
            points=[PointStruct(id=start + i, vector=vecs[i].tolist()) for i in range(cnt)],
            wait=True,
        )
    upload_sec = time.time() - t0
    print(f"  업로드 {n}개 {upload_sec:.1f}s")

    # 인덱스 만들기 시작: 기준을 기본값(20000KB)으로 되돌리면 옵티마이저가 HNSW 를 만든다
    t1 = time.time()
    client.update_collection(collection_name=coll, optimizers_config=OptimizersConfigDiff(indexing_threshold=20000))
    wait_until_indexed(client, coll, n)
    index_sec = time.time() - t1

    info = client.get_collection(coll)
    row = {
        "name": name, "m": m, "ef_construct": ef_construct, "n": n, "dim": dim,
        "upload_sec": round(upload_sec, 1), "index_sec": round(index_sec, 1),
        "points": info.points_count, "indexed": info.indexed_vectors_count,
        "segments": info.segments_count, "status": str(info.status),
    }
    print(f"  인덱스 완성 {index_sec:.1f}s  points={row['points']} indexed={row['indexed']} segments={row['segments']}")
    return row


# ── search ────────────────────────────────────────────────
def make_queries(dim: int, clusters: int, noise: float, seed: int, count: int) -> np.ndarray:
    centers = make_centers(dim, clusters, seed)  # 데이터와 같은 중심점에서, 다른 시드로 뽑는다
    return make_points(centers, count, noise, seed=seed + 7919)


def percentile(sorted_vals: list[float], p: float) -> float:
    idx = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def search(client: QdrantClient, name: str, dim: int, clusters: int, noise: float, seed: int,
           queries: int, limit: int, efs: list[int], repeat: int) -> list[dict]:
    coll = PREFIX + name
    qvecs = make_queries(dim, clusters, noise, seed, queries)

    # 정답: exact=True 는 HNSW 를 쓰지 않고 전부 비교하는 정확한 검색이다
    print(f"  정답 계산(exact) {queries}건 ...")
    truth: list[set[int]] = []
    for q in qvecs:
        res = client.query_points(coll, query=q.tolist(), limit=limit, search_params=SearchParams(exact=True))
        truth.append({p.id for p in res.points})

    rows = []
    for ef in efs:
        # 준비 운동(첫 몇 건은 느릴 수 있어 버린다)
        for q in qvecs[: min(10, queries)]:
            client.query_points(coll, query=q.tolist(), limit=limit, search_params=SearchParams(hnsw_ef=ef))

        lat_ms: list[float] = []
        recalls: list[float] = []
        t0 = time.time()
        for _ in range(repeat):
            for qi, q in enumerate(qvecs):
                ts = time.perf_counter()
                res = client.query_points(coll, query=q.tolist(), limit=limit, search_params=SearchParams(hnsw_ef=ef))
                lat_ms.append((time.perf_counter() - ts) * 1000)
                recalls.append(len({p.id for p in res.points} & truth[qi]) / limit)
        total = time.time() - t0
        s = sorted(lat_ms)
        rows.append({
            "name": name, "hnsw_ef": ef, "limit": limit, "queries": queries * repeat,
            "recall": round(statistics.mean(recalls), 4),
            "min_recall": round(min(recalls), 2),
            "p50_ms": round(percentile(s, 50), 2), "p95_ms": round(percentile(s, 95), 2),
            "avg_ms": round(statistics.mean(lat_ms), 2),
            "qps": round(len(lat_ms) / total, 1),
        })
        r = rows[-1]
        print(f"  ef={ef:<4} recall@{limit}={r['recall']:.4f}  p50={r['p50_ms']}ms p95={r['p95_ms']}ms qps={r['qps']}")
    return rows


# ── 출력 ──────────────────────────────────────────────────
def print_table(rows: list[dict], cols: list[str]) -> None:
    print("| " + " | ".join(cols) + " |")
    print("| " + " | ".join("---" for _ in cols) + " |")
    for r in rows:
        print("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")


def parse_ints(s: str) -> list[int]:
    return [int(x) for x in s.split(",") if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Qdrant HNSW 파라미터 실험 (Day 29)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--dim", type=int, default=DEFAULT_DIM)
        p.add_argument("--clusters", type=int, default=100, help="합성 데이터의 중심점 수")
        p.add_argument("--noise", type=float, default=3.0, help="중심점에서 흐트러뜨리는 정도(클수록 덜 모인다)")
        p.add_argument("--seed", type=int, default=42)
        p.add_argument("--grpc", action="store_true", help="REST 대신 gRPC(6334)로 접속")

    b = sub.add_parser("build"); common(b)
    b.add_argument("--name", required=True)
    b.add_argument("--n", type=int, default=100000)
    b.add_argument("--m", type=int, default=16)
    b.add_argument("--ef-construct", type=int, default=100)

    s = sub.add_parser("search"); common(s)
    s.add_argument("--name", required=True)
    s.add_argument("--queries", type=int, default=200)
    s.add_argument("--limit", type=int, default=10)
    s.add_argument("--ef", default="16,32,64,128,256,512")
    s.add_argument("--repeat", type=int, default=1)

    w = sub.add_parser("sweep"); common(w)
    w.add_argument("--n", type=int, default=100000)
    w.add_argument("--m", default="16")
    w.add_argument("--ef-construct", default="100")
    w.add_argument("--queries", type=int, default=200)
    w.add_argument("--limit", type=int, default=10)
    w.add_argument("--ef", default="64")
    w.add_argument("--repeat", type=int, default=1)
    w.add_argument("--keep", action="store_true", help="끝나도 컬렉션을 지우지 않는다")

    d = sub.add_parser("drop")
    d.add_argument("--name")
    d.add_argument("--all", action="store_true")

    a = ap.parse_args()
    client = get_client(getattr(a, "grpc", False))
    info = client.info() if hasattr(client, "info") else None
    print(f"Qdrant 접속: {os.getenv('QDRANT_HOST', 'localhost')}:{os.getenv('QDRANT_PORT', '6333')}"
          + (f"  (서버 {getattr(info, 'version', '?')})" if info else ""))

    if a.cmd == "build":
        row = build(client, a.name, a.n, a.dim, a.m, a.ef_construct, a.clusters, a.noise, a.seed)
        print(json.dumps(row, ensure_ascii=False))

    elif a.cmd == "search":
        rows = search(client, a.name, a.dim, a.clusters, a.noise, a.seed, a.queries, a.limit, parse_ints(a.ef), a.repeat)
        print_table(rows, ["name", "hnsw_ef", "recall", "min_recall", "p50_ms", "p95_ms", "avg_ms", "qps"])

    elif a.cmd == "sweep":
        build_rows, search_rows = [], []
        for m in parse_ints(a.m):
            for efc in parse_ints(a.ef_construct):
                name = f"m{m}-efc{efc}"
                print(f"[{name}] build")
                br = build(client, name, a.n, a.dim, m, efc, a.clusters, a.noise, a.seed)
                build_rows.append(br)
                print(f"[{name}] search")
                for r in search(client, name, a.dim, a.clusters, a.noise, a.seed, a.queries, a.limit, parse_ints(a.ef), a.repeat):
                    r.update({"m": m, "ef_construct": efc})
                    search_rows.append(r)
                if not a.keep:
                    client.delete_collection(PREFIX + name)
        print("\n## 인덱스 만들기")
        print_table(build_rows, ["name", "m", "ef_construct", "n", "upload_sec", "index_sec", "points", "indexed", "segments"])
        print("\n## 검색")
        print_table(search_rows, ["name", "m", "ef_construct", "hnsw_ef", "recall", "min_recall", "p50_ms", "p95_ms", "avg_ms", "qps"])

    elif a.cmd == "drop":
        names = [c.name for c in client.get_collections().collections if c.name.startswith(PREFIX)]
        if not a.all:
            names = [n for n in names if n == PREFIX + (a.name or "")]
        for n in names:
            client.delete_collection(n)
            print(f"삭제: {n}")
        if not names:
            print("지울 컬렉션 없음")


if __name__ == "__main__":
    main()
