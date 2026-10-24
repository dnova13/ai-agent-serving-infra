"""
Chunking 전략 비교 실험 (Day 12)

같은 긴 문서를 서로 다른 방식으로 잘라 Qdrant 에 넣고, 같은 질문으로 검색 정확도를 비교한다.

전략
  fixed     : 고정 길이(글자 수)로 자름. 문장 중간이 잘릴 수 있어 overlap 으로 보완
  sentence  : 문장 단위로 나눈 뒤 최대 글자 수까지 문장을 묶음
  paragraph : 문서의 문단(빈 줄) 단위로 자름. 주제 단위로 이미 나뉜 문서에서 유리
  semantic  : 문장마다 임베딩해서 인접 문장의 유사도가 떨어지는 지점에서 자름 (의미 단위)

측정
  - 청크 수, 평균 길이     : 저장 규모와, 모델에 넘기는 문맥 크기
  - top-1 / top-3 성공률   : 정답 근거(must 문자열 전부)가 한 청크 안에 들어 있는가
  - 성공 시 top-1 길이      : 길수록 모델에 넘기는 토큰이 늘어난다

실험용 컬렉션 chunk-exp-<전략> 에만 저장하고 끝나면 삭제한다. (기존 documents-12 는 건드리지 않음)

실행: python -m app.experiments.chunk_experiment            (프로젝트 루트 d2-ai-python)
      python -m app.experiments.chunk_experiment --keep     (실험 컬렉션을 남겨 대시보드에서 확인)
      python -m app.experiments.chunk_experiment --size 200 --overlap 40 --max-chars 200
      python -m app.experiments.chunk_experiment --chunks-only   (청크가 어디서 잘렸는지만 출력, Qdrant 저장과 검색 없음)
      python -m app.experiments.chunk_experiment --show          (청크를 출력하고 실험도 계속)
API 호출: 청크 약 60건 + semantic 문장 약 50건 + 질문 12건 = 약 120건. 무료 한도(분당 100건)를 넘으므로
      한도 보호(대기, 429 재시도, 캐시)가 들어 있어 실행에 1~2분 걸릴 수 있다.
"""

# [표준 라이브러리]
# argparse : 터미널에서 준 옵션(--size, --keep 등)을 읽는다
# math     : 제곱근(sqrt)
# re       : 정규식 (문장 나누기, 공백 정리, 에러 메시지에서 숫자 뽑기)
# time     : sleep(대기)과 monotonic(경과 시간 측정)
# uuid     : Qdrant 에 넣을 포인트 ID 만들기
import argparse
import math
import re
import time
import uuid

# [Qdrant 모델]
# VectorParams : 컬렉션의 벡터 크기와 유사도 방식 설정
# Distance     : 유사도 계산 방식 (여기서는 COSINE)
# PointStruct  : 저장할 점 1개 = id + 벡터 + payload(원문 등 부가 정보)
from qdrant_client.models import Distance, PointStruct, VectorParams

from app.data.long_docs import LONG_DOCS, EVAL_QUESTIONS
from app.services.embedding import embed_documents, embed_query, EMBEDDING_DIM
from app.services.qdrant_setup import get_client

# 실험용 컬렉션 이름 앞에 붙이는 접두사. 'chunk-exp-fixed', 'chunk-exp-semantic' 처럼 쓴다.
# 기존 documents-12 컬렉션과 이름이 섞이지 않게 하려는 것이다.
PREFIX = "chunk-exp-"


# ── 0) 임베딩 호출 한도 보호 ───────────────────────────
# 무료 티어는 임베딩이 분당 100건(텍스트 1개 = 1건)이다. 이 실험은 전략 4개 x 청크 + 문장 + 질문으로
# 합계 약 130건을 한 번에 보내서 429(RESOURCE_EXHAUSTED)가 났다. 그래서 최근 60초 동안 보낸 건수를
# 세어 한도에 가까워지면 기다리고, 그래도 429 가 나오면 안내된 시간만큼 쉬고 다시 시도한다.
# 같은 텍스트는 한 번만 임베딩하도록 캐시한다.
# [한도 보호용 설정과 상태]
QUOTA_PER_MIN = 90          # 무료 한도 100 에서 여유를 둔 값
BATCH = 20                  # 한 번에 보내는 텍스트 수
# _sent 는 '슬라이딩 윈도우'다. 텍스트를 1건 보낼 때마다 보낸 시각을 하나씩 쌓고,
# 60초가 지난 기록은 지운다. 그래서 len(_sent) = 최근 60초 동안 보낸 건수가 된다.
_sent: list[float] = []     # 최근에 보낸 텍스트들의 시각
# 캐시: {텍스트: 벡터}. 같은 텍스트를 다시 요청하면 API 를 부르지 않고 저장해 둔 벡터를 쓴다.
# 문서용과 질문용은 task_type 이 달라 같은 텍스트라도 벡터가 다르므로 캐시를 둘로 나눴다.
_doc_cache: dict[str, list[float]] = {}
_query_cache: dict[str, list[float]] = {}


def _wait_for_quota(n: int) -> None:
    """최근 60초 동안 보낸 건수 + n 이 한도를 넘으면 가장 오래된 기록이 빠질 때까지 기다린다."""
    # 한도 안에 들어올 때까지 반복한다. (기다린 뒤 다시 계산해야 하므로 루프)
    while True:
        # monotonic 은 시스템 시계를 바꿔도 영향받지 않는 '경과 시간용' 시계다. 대기 시간 계산에 적합하다.
        now = time.monotonic()
        # 60초가 지난 기록은 윈도우 밖이므로 맨 앞(가장 오래된 것)부터 지운다.
        while _sent and now - _sent[0] >= 60:
            _sent.pop(0)
        # 이번에 n건을 더 보내도 한도(90건) 이하이면 사용 기록을 남기고 통과한다. (n건이면 같은 시각을 n번 기록)
        if len(_sent) + n <= QUOTA_PER_MIN:
            _sent.extend([now] * n)
            return
        # 한도를 넘길 상황이면, 가장 오래된 기록이 60초가 되어 윈도우에서 빠질 때까지 남은 시간 + 여유 1초를 기다린다.
        wait = 60 - (now - _sent[0]) + 1
        print(f"  (임베딩 한도 보호: {wait:.0f}초 대기, 최근 60초 {len(_sent)}건 사용)")
        time.sleep(wait)


def _call_with_retry(fn, n: int):
    """429 가 나오면 에러 메시지의 retry 시간(없으면 50초)만큼 쉬고 최대 3번 다시 시도한다."""
    # 최초 1번 + 재시도 3번 = 최대 4번 시도한다. attempt 는 0, 1, 2, 3.
    for attempt in range(4):
        # 호출하기 전에 항상 한도부터 확인한다. 재시도할 때도 마찬가지다.
        _wait_for_quota(n)
        # fn() 은 호출자가 넘긴 '실제 임베딩 호출'이다. 성공하면 결과를 그대로 반환하고 끝난다.
        try:
            return fn()
        # 어떤 예외든 일단 받아서 메시지 문자열로 429 인지 판별한다. (라이브러리가 감싸서 던져 예외 타입으로는 구분하기 어렵다)
        except Exception as e:
            msg = str(e)
            # 429 또는 RESOURCE_EXHAUSTED 이고, 아직 재시도 기회가 남았을 때만 쉬었다가 다시 시도한다.
            # (attempt < 3 : 마지막 4번째 시도에서 실패하면 재시도하지 않고 아래 raise 로 간다)
            if ("429" in msg or "RESOURCE_EXHAUSTED" in msg) and attempt < 3:
                # 에러 메시지의 'Please retry in 46.76s' 에서 숫자(초)를 뽑는다. 정규식 ([\d.]+) 는 숫자와 점으로 이루어진 덩어리.
                m = re.search(r"retry in ([\d.]+)s", msg)
                # 안내된 시간 + 2초 여유. 메시지에서 숫자를 못 찾으면(m 이 None) 50초를 쉰다.
                delay = float(m.group(1)) + 2 if m else 50
                print(f"  (429 수신: {delay:.0f}초 쉬고 재시도 {attempt + 1}/3)")
                time.sleep(delay)
                # 다음 시도로 넘어간다. (for 의 다음 attempt, 맨 위의 _wait_for_quota 부터 다시)
                continue
            # 429 가 아닌 에러이거나 재시도 기회를 모두 썼으면, 원래 에러를 그대로 다시 던져서 호출한 쪽이 알게 한다.
            raise


def embed_texts(texts: list[str]) -> list[list[float]]:
    """문서용 임베딩. 캐시에 없는 텍스트만 BATCH 개씩 한도를 지키며 보낸다."""
    # 캐시에 없는 텍스트만 골라낸다. dict.fromkeys 는 중복을 없애면서 입력 순서를 유지한다. (set 은 순서가 사라진다)
    todo = list(dict.fromkeys(t for t in texts if t not in _doc_cache))
    # BATCH(20)개씩 끊어서 보낸다. range(0, N, 20) 은 0, 20, 40, ... 를 만든다.
    for i in range(0, len(todo), BATCH):
        part = todo[i:i + BATCH]
        # lambda 는 '나중에 실행할 한 줄 함수'다. _call_with_retry 가 한도 확인과 재시도를 감싸서 이 함수를 실행한다.
        # len(part) 는 이번에 보내는 건수로, 한도 계산에 쓰인다.
        vecs = _call_with_retry(lambda: embed_documents(part), len(part))
        # zip(part, vecs) 는 (텍스트, 벡터) 쌍을 만든다. 이를 캐시에 저장한다.
        _doc_cache.update(zip(part, vecs))
    # 입력 순서 그대로 벡터를 돌려준다. 입력에 같은 텍스트가 여러 번 있어도 각각 같은 벡터가 들어간다.
    return [_doc_cache[t] for t in texts]


def embed_q(text: str) -> list[float]:
    """질문용 임베딩 (캐시, 한도 보호 포함)."""
    # 질문 1건은 한도 계산에 1건으로 센다. 같은 질문은 캐시에서 바로 꺼낸다.
    if text not in _query_cache:
        _query_cache[text] = _call_with_retry(lambda: embed_query(text), 1)
    return _query_cache[text]


# ── 1) 청킹 함수들 ────────────────────────────────────

def split_sentences(text: str) -> list[str]:
    """마침표/물음표/느낌표 뒤 공백, 또는 빈 줄에서 문장을 나눈다."""
    # 정규식으로 나눌 지점을 찾는다.
    #   (?<=[.!?])\s+ : 마침표/물음표/느낌표 '바로 뒤'의 공백. 앞쪽 문장부호는 문장에 그대로 남는다. (뒤돌아보기)
    #   |            : 또는
    #   \n\s*\n       : 빈 줄 (문단 경계)
    parts = re.split(r"(?<=[.!?])\s+|\n\s*\n", text)
    
    # 나눈 조각의 앞뒤 공백을 지우고, 비어 있는 조각은 버린다.
    return [p.strip() for p in parts if p and p.strip()]


def chunk_fixed(text: str, size: int = 150, overlap: int = 30) -> list[str]:
    """고정 글자 수로 자름. 다음 청크는 overlap 글자만큼 앞 청크와 겹친다."""
    
    # 줄바꿈과 연속된 공백을 공백 1개로 바꿔 한 줄로 만든다. (글자 수를 일정하게 세려는 것)
    text = re.sub(r"\s+", " ", text).strip()
    
    # 다음 청크의 시작 위치까지의 간격. 기본값이면 150 - 30 = 120.
    # 즉 0~150, 120~270, 240~390 ... 처럼 앞 청크와 30글자가 겹친다. 겹치면 문장이 경계에서 잘려도 다음 청크에 다시 나온다.
    # max(1, ...) 는 overlap 이 size 이상일 때 간격이 0 이하가 되어 무한 루프가 되는 것을 막는다.
    step = max(1, size - overlap)
    chunks = []
    
    # start 는 각 청크의 시작 글자 위치.
    for start in range(0, len(text), step):
        # start 부터 size 글자를 자른다. 끝을 넘어가도 파이썬 슬라이스는 에러 없이 남은 만큼만 준다.
        piece = text[start:start + size].strip()
        if piece:
            chunks.append(piece)
        # 이 청크가 문서 끝까지 닿았으면 끝낸다. (이후 청크는 앞 청크의 일부일 뿐이라 만들지 않는다)
        if start + size >= len(text):
            break
    return chunks


def chunk_sentence(text: str, max_chars: int = 150) -> list[str]:
    """문장을 순서대로 묶되, 합쳐서 max_chars 를 넘기면 새 청크를 시작한다. (문장은 자르지 않음)"""
    # chunks : 완성된 청크들 / cur : 지금 만들고 있는 청크
    chunks, cur = [], ""
    for s in split_sentences(text):
        # 지금 청크에 이 문장을 붙이면(+1 은 사이 공백) max_chars 를 넘는 경우 -> 현재 청크를 확정하고 이 문장으로 새 청크를 시작한다.
        # cur 가 비어 있으면 문장이 아무리 길어도 그냥 넣는다. 그래서 문장 하나가 max_chars 보다 길면 청크도 그만큼 길어질 수 있다.
        if cur and len(cur) + 1 + len(s) > max_chars:
            chunks.append(cur)
            cur = s
        else:
            cur = f"{cur} {s}".strip()
    # 마지막에 만들던 청크가 남아 있으면 추가한다.
    if cur:
        chunks.append(cur)
    return chunks


def chunk_paragraph(text: str) -> list[str]:
    """빈 줄 기준 문단 단위."""
    # 문서(long_docs)는 문단 사이를 빈 줄('\n\n')로 구분해 두었다. 그 빈 줄에서 나눈다.
    return [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]


def _cosine(a: list[float], b: list[float]) -> float:
    # 코사인 유사도 = 내적 / (벡터 a 의 길이 x 벡터 b 의 길이).
    # 두 벡터가 같은 방향이면 1, 직각이면 0 에 가깝다. Qdrant 가 내부에서 하는 계산을 눈으로 보려고 직접 구현했다.
    # numpy 없이 순수 파이썬으로 써서 설치할 것이 없다.
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    # 길이가 0 인 벡터가 있으면 0 으로 나누게 되므로 0.0 을 돌려준다.
    return dot / (na * nb) if na and nb else 0.0


def chunk_semantic(texts: list[str], max_chars: int = 300, k: float = 0.5) -> dict[int, list[str]]:
    """
    의미 단위 청킹. 문서 전체의 문장을 한 번에 임베딩해서 호출 수를 줄인다.
    인접 문장 유사도가 (평균 - k*표준편차) 보다 낮은 곳을 주제가 바뀌는 지점으로 보고 자른다.
    max_chars 를 넘기면 유사도와 상관없이 자른다. 반환: {문서 번호: 청크 리스트}
    """
    # per_doc : 문서별 문장 리스트의 리스트. 예) [[문서0 문장들], [문서1 문장들], ...]
    per_doc = [split_sentences(t) for t in texts]
    # 모든 문서의 문장을 한 줄로 편 리스트. 임베딩 호출을 묶어서 한 번에(배치로) 보내려는 것이다.
    flat = [s for sents in per_doc for s in sents]
    # flat 의 각 문장을 임베딩한다. (한도 보호와 캐시가 적용된다) vecs[i] 는 flat[i] 의 벡터다.
    vecs = embed_texts(flat)

    # result : {문서 번호: 청크 리스트} / pos : flat 에서 지금 문서가 시작하는 위치
    result, pos = {}, 0
    
    for di, sents in enumerate(per_doc):
        # flat 에서 이 문서의 문장 개수만큼 잘라내 이 문서 문장들의 벡터만 가져온다.
        v = vecs[pos:pos + len(sents)]
        pos += len(sents)
        # sims[i] = i 번째 문장과 i+1 번째 문장의 유사도. 문장이 n 개면 비교 쌍은 n-1 개다.
        # 값이 높으면 이어지는 이야기, 낮으면 주제가 바뀌는 지점으로 본다.
        sims = [_cosine(v[i], v[i + 1]) for i in range(len(sents) - 1)]
        
        # 자를 기준값 계산. 유사도들의 평균과 표준편차(흩어진 정도)를 구하고,
        # '평균보다 k x 표준편차 이상 낮은' 유사도를 주제 전환으로 본다. k 가 작을수록 기준이 평균에 가까워져 더 많이 잘린다.
        # 문장이 1 개뿐이면 비교할 쌍이 없으므로 기준을 -1 로 두어 절대 자르지 않게 한다.
        if sims:
            mean = sum(sims) / len(sims)
            std = math.sqrt(sum((x - mean) ** 2 for x in sims) / len(sims))
            cut_below = mean - k * std
        else:
            cut_below = -1
        # 첫 문장으로 첫 청크를 시작한다. (문장이 없으면 빈 문자열)
        chunks, cur = [], sents[0] if sents else ""
        
        # 두 번째 문장부터 하나씩 보면서, 앞 문장 뒤에 붙일지 새 청크를 시작할지 결정한다.
        for i in range(1, len(sents)):
            # 이 문장을 붙이면 max_chars 를 넘는가. (유사도가 높아도 너무 길어지면 자르려는 안전장치)
            too_long = len(cur) + 1 + len(sents[i]) > max_chars
            # sims[i-1] 은 i 번째 문장과 바로 앞 문장의 유사도다. (sims 는 문장보다 하나 짧아 인덱스가 하나 밀린다)
            # 유사도가 기준보다 낮거나 너무 길면 -> 현재 청크를 확정하고 새 청크 시작. 아니면 현재 청크에 이어 붙인다.
            if sims[i - 1] < cut_below or too_long:
                chunks.append(cur)
                cur = sents[i]
            else:
                cur = f"{cur} {sents[i]}"
        # 마지막 문장까지 본 뒤 남은 청크를 추가한다.
        if cur:
            chunks.append(cur)
        
        result[di] = chunks
        
    return result


# ── 2) 적재와 검색 ────────────────────────────────────

def build_chunks(args) -> dict[str, list[tuple[str, int, str]]]:
    """전략별로 [(doc_id, 청크 번호, 청크 텍스트)] 를 만든다."""
    # 문서 본문만 모은 리스트. semantic 청킹에 한 번에 넘기려는 것이다.
    texts = [d["text"] for d in LONG_DOCS]
    # 전략 이름 -> 청크 목록. 각 청크는 (문서 ID, 문서 안에서의 청크 번호, 텍스트) 튜플이다.
    out = {"fixed": [], "sentence": [], "paragraph": [], "semantic": []}
    
    # fixed, sentence, paragraph 는 문서마다 자르기만 하면 되므로 한 문서씩 처리한다. i 는 그 문서 안에서의 청크 번호다.
    for d in LONG_DOCS:
        for i, c in enumerate(chunk_fixed(d["text"], args.size, args.overlap)):
            out["fixed"].append((d["id"], i, c))
        for i, c in enumerate(chunk_sentence(d["text"], args.max_chars)):
            out["sentence"].append((d["id"], i, c))
        for i, c in enumerate(chunk_paragraph(d["text"])):
            out["paragraph"].append((d["id"], i, c))
            
    # semantic 은 문장을 임베딩해야 해서 API 를 부르는 유일한 청킹이다. 호출을 묶으려고 모든 문서를 한 번에 넘긴다.
    sem = chunk_semantic(texts, max_chars=args.sem_max, k=args.k)
    
    # sem 의 키 di 는 texts 의 순서(= LONG_DOCS 의 순서)와 같으므로 enumerate 번호로 문서를 맞춘다.
    for di, d in enumerate(LONG_DOCS):
        for i, c in enumerate(sem[di]):
            out["semantic"].append((d["id"], i, c))
            
    return out


def ingest(client, name: str, chunks: list[tuple[str, int, str]]) -> None:
    coll = PREFIX + name
    # 같은 이름의 컬렉션이 이미 있으면(이전 실행의 --keep 등) 지우고 새로 만든다. 그래서 몇 번을 실행해도 같은 상태에서 시작한다.
    if client.collection_exists(coll):
        client.delete_collection(coll)
    # 컬렉션 생성. 벡터 크기는 임베딩 모델의 출력 차원(768)과 같아야 하고, 유사도는 코사인을 쓴다.
    client.create_collection(coll, vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE))
    
    # 청크 텍스트(튜플의 3번째 c[2])를 임베딩한다. 한도 보호와 캐시가 적용된다.
    vectors = embed_texts([c[2] for c in chunks])
    
    # 청크마다 Qdrant 의 '점' 하나를 만든다.
    points = [
        PointStruct(
            # uuid5 는 같은 입력이면 항상 같은 ID 를 만든다. 다시 실행해도 같은 청크는 같은 ID 로 덮어써져 중복이 쌓이지 않는다. (Day 8 과 같은 방식)
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{name}:{doc_id}:{i}")),
            vector=vec,
            # payload 는 벡터와 함께 저장하는 부가 정보. 원문(text)을 같이 저장해야 검색 결과에서 텍스트를 바로 꺼낼 수 있다.
            payload={"doc_id": doc_id, "chunk_idx": i, "text": text},
        )
        # 청크 튜플과 그 벡터를 짝지어 순서대로 돈다. 튜플은 (doc_id, i, text) 로 바로 풀어서 받는다.
        for (doc_id, i, text), vec in zip(chunks, vectors)
    ]
    
    # upsert = 없으면 추가, 있으면 덮어쓰기. 한 번에 모두 저장한다.
    client.upsert(collection_name=coll, points=points)


def search_chunks(client, name: str, qvec: list[float], limit: int = 3) -> list[dict]:
    # 질문 벡터와 가까운 순으로 limit 개를 가져온다. with_payload=True 라야 저장해 둔 원문이 결과에 같이 온다.
    res = client.query_points(collection_name=PREFIX + name, query=qvec, limit=limit, with_payload=True)
    # 필요한 값만 추려서 돌려준다. score 는 코사인 유사도(클수록 비슷), 순서는 점수가 높은 것부터다.
    return [{"score": p.score, "doc_id": p.payload["doc_id"], "text": p.payload["text"]} for p in res.points]


def hit_rank(hits: list[dict], must: list[str]) -> int | None:
    """must 문자열이 모두 들어 있는 청크의 순위(1부터). 없으면 None."""
    # 순위는 1부터 센다. hits 는 점수 높은 순으로 정렬되어 있다.
    for rank, h in enumerate(hits, 1):
        # must 의 문자열이 '모두' 이 청크 안에 있어야 성공. 두 사실이 청크 경계에 걸쳐 다른 청크로 갈라졌다면 실패로 처리된다. (경계 문제를 드러내려는 평가 방식)
        if all(m in h["text"] for m in must):
            return rank
    return None


def print_chunks(chunks_by: dict[str, list[tuple[str, int, str]]]) -> None:
    """전략별로 청크가 어디서 잘렸는지 터미널에 출력한다. (--show, --chunks-only 용)"""
    for name, chunks in chunks_by.items():
        print(f"\n{'=' * 70}\n[{name}] 청크 {len(chunks)}개")
        prev_doc = None
        for doc_id, i, text in chunks:
            # 문서가 바뀌면 구분선을 한 번 찍어서 어느 문서의 청크인지 보이게 한다.
            if doc_id != prev_doc:
                print(f"--- {doc_id} ---")
                prev_doc = doc_id
            print(f"  [{i}] ({len(text)}자) {text}")


# ── 3) 실행 ───────────────────────────────────────────

def main() -> None:
    # [옵션 정의] 터미널에서 줄 수 있는 옵션들. 안 주면 default 값을 쓴다.
    # action='store_true' 는 값 없이 '--keep' 만 써도 켜지는 스위치다. (옵션 이름의 '-' 는 코드에서 '_' 로 바뀐다: --max-chars -> args.max_chars)
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", type=int, default=150, help="fixed 청크 글자 수")
    ap.add_argument("--overlap", type=int, default=30, help="fixed 겹치는 글자 수")
    ap.add_argument("--max-chars", type=int, default=150, help="sentence 청크 최대 글자 수")
    ap.add_argument("--sem-max", type=int, default=300, help="semantic 청크 최대 글자 수")
    ap.add_argument("--k", type=float, default=0.5, help="semantic 분할 민감도 (작을수록 많이 자름)")
    ap.add_argument("--keep", action="store_true", help="실험 컬렉션을 삭제하지 않음")
    ap.add_argument("--show", action="store_true", help="전략별로 잘린 청크를 출력하고 실험을 계속 진행")
    ap.add_argument("--chunks-only", action="store_true", help="청크만 출력하고 종료 (Qdrant 저장, 검색 없음)")
    # 실제로 입력된 옵션을 읽어 args.size, args.keep 처럼 꺼내 쓸 수 있게 한다.
    args = ap.parse_args()

    # Qdrant 서버(localhost:6333)에 연결한다. 컨테이너가 떠 있어야 한다.
    client = get_client()
    # 전략별 청크를 만든다. 이 안에서 semantic 때문에 문장 임베딩 호출이 처음으로 일어난다.
    chunks_by = build_chunks(args)

    # --show / --chunks-only : 어디서 잘렸는지 눈으로 확인. --chunks-only 는 여기서 끝낸다.
    # (semantic 은 문장 임베딩 때문에 API 를 부르지만, Qdrant 저장과 질문 검색은 하지 않아 호출이 가장 적다)
    if args.show or args.chunks_only:
        print_chunks(chunks_by)
        if args.chunks_only:
            return

    # 질문 12개를 한 번만 임베딩하고, 4개 전략이 모두 같은 질문 벡터를 쓴다. (공정한 비교이면서 호출도 아낀다)
    qvecs = [embed_q(q) for q, _ in EVAL_QUESTIONS]

    # summary : 전략별 요약 지표 / detail : 전략별로 질문마다의 정답 청크 순위 리스트
    summary, detail = {}, {}
    
    # 전략을 하나씩 돌며 ① 저장 ② 12개 질문으로 검색 ③ 지표 계산을 한다.
    for name, chunks in chunks_by.items():
        ingest(client, name, chunks)
        # ranks : 질문별 정답 순위(1~3, 없으면 None) / top1_len : 1등으로 성공한 경우의 청크 길이들
        ranks, top1_len = [], []
        # 질문(q), 정답 근거(must), 질문 벡터(qv)를 짝지어 돈다.
        for (q, must), qv in zip(EVAL_QUESTIONS, qvecs):
            hits = search_chunks(client, name, qv, limit=3)
            # top-3 안에서 must 가 모두 든 청크의 순위를 구한다. 없으면 None.
            r = hit_rank(hits, must)
            ranks.append(r)
            # 1등으로 맞힌 경우에만 그 청크 길이를 기록한다. 길수록 모델에 넘기는 토큰이 많다는 뜻이라 '정확도가 같을 때 어느 쪽이 싼가' 비교에 쓴다.
            if r == 1:
                top1_len.append(len(hits[0]["text"]))
        n = len(EVAL_QUESTIONS)
        # chunks   : 청크 개수 / avg_len : 평균 글자 수
        # top1     : 1등으로 맞힌 질문 수 / top3 : 3등 안에 맞힌 질문 수 (None 이 아닌 것)
        # top1_len : 1등 성공 청크의 평균 길이 / n : 전체 질문 수
        summary[name] = {
            "chunks": len(chunks),
            "avg_len": sum(len(c[2]) for c in chunks) / len(chunks),
            "top1": sum(1 for r in ranks if r == 1),
            "top3": sum(1 for r in ranks if r is not None),
            "top1_len": (sum(top1_len) / len(top1_len)) if top1_len else 0,
            "n": n,
        }
        detail[name] = ranks

    # [결과 출력] f-string 의 :<10 은 왼쪽 정렬 10칸, :>6 은 오른쪽 정렬 6칸, :.0f 는 소수점 없이 표시한다는 뜻이다. 표의 열을 맞추려는 것이다.
    print("\n[전략별 요약]")
    print(f"{'전략':<10}{'청크수':>6}{'평균길이':>8}{'top-1':>9}{'top-3':>9}{'성공시 top1 길이':>16}")
    for name, s in summary.items():
        print(f"{name:<10}{s['chunks']:>6}{s['avg_len']:>8.0f}{s['top1']:>6}/{s['n']:<2}{s['top3']:>6}/{s['n']:<2}{s['top1_len']:>16.0f}")

    print("\n[질문별 정답 청크 순위]  (1~3 = 순위, X = top-3 안에 없음)")
    # 전략 이름 목록 (dict 는 넣은 순서를 유지하므로 fixed, sentence, paragraph, semantic 순서)
    names = list(summary)
    print(f"{'질문':<46}" + "".join(f"{n:>11}" for n in names))
    for qi, (q, _) in enumerate(EVAL_QUESTIONS):
        # 각 전략의 순위를 한 줄로 이어 붙인다. 순위가 None 이면 'X'(top-3 안에 못 찾음)로 표시한다.
        cells = "".join(f"{(str(detail[n][qi]) if detail[n][qi] else 'X'):>11}" for n in names)
        print(f"{q[:22]:<46}{cells}")

    # --keep 이 없으면 실험 컬렉션을 모두 지운다. 있으면 남겨서 Qdrant 대시보드(localhost:6333/dashboard)에서 청크를 직접 볼 수 있다.
    if not args.keep:
        for name in names:
            client.delete_collection(PREFIX + name)
        print("\n실험 컬렉션을 삭제했습니다. (--keep 으로 남길 수 있음)")
    else:
        print(f"\n실험 컬렉션 유지: {[PREFIX + n for n in names]}")


# 이 파일을 직접 실행했을 때만 main() 을 부른다. 다른 파일에서 import 할 때는 실행되지 않는다.
if __name__ == "__main__":
    main()
