# Day 24 가이드 — Redis TTL(만료) 정책

> 상태: 코드 변경과 확인 스크립트(`ttl_check.py`) 작성 완료. 아래 "샌드박스" 결과는 **코드를 만든 쪽의 샌드박스**(실제 `redis-server`, Kafka/Spring/모델 없음)에서 본 것이고, ①~⑤ 는 내 환경에서 실행해 DAY24_RESULT 3번에 기록했다(2026-10-15).
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, **6379 = Redis(redis1 컨테이너)**, 9092 = Kafka, 6333 = Qdrant.
> 앞 단계는 [DAY23_GUIDE.md](DAY23_GUIDE.md), 코드 흐름은 [DAY24_CODE_GUIDE.md](DAY24_CODE_GUIDE.md).

**전체 목적:** Day 20~23 에서 "임시값"으로 두었던 Redis 키의 만료 시간(TTL)을 **키 종류별 정책**으로 정리한다. 결과 키는 상태(PROCESSING, DONE, FAILED)마다 다른 TTL 을 걸고, 대화와 락은 각자의 규칙을 한곳(`ttl_policy.py`)에 모으고, 그대로 동작하는지와 TTL 없이 남은 키가 없는지를 확인한다.


## 1. 오늘 목표

| 확인할 것                                                                                              | 실험 |
| ------------------------------------------------------------------------------------------------------ | ---- |
| 정책대로 TTL 이 걸리고, 대화 TTL 이 갱신되고, 락이 풀리고, 영구 키가 없는가 (스크립트)                  | ①    |
| 실제 요청 한 건에서 결과 TTL 이 PROCESSING 값에서 DONE 값으로 바뀌는가                                  | ②    |
| 지금 Redis 의 `agent:*` 키가 모두 TTL 을 가지고 있는가                                                  | ③    |
| Redis 의 메모리 설정이 무엇인가 (읽기만)                                                                | ④    |
| (선택) Kafka 경로에서 같은 `sessionId` 로 다시 물으면 대화 TTL 이 새로 시작되는가                       | ⑤    |


## 2. 정책

| 키                               | TTL                          | 환경변수                                         | 이유                                                                                                         |
| -------------------------------- | ---------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `agent:result:<id>` PROCESSING   | 600초 (10분)                 | `RESULT_PROCESSING_TTL_SEC`                      | 워커가 죽으면 PROCESSING 이 남는다. 한 건은 최대 120초라 그보다 길게 잡아 처리 중에 사라지지 않게 한다          |
| `agent:result:<id>` DONE         | 86400초 (1일)                | `RESULT_DONE_TTL_SEC` (없으면 `RESULT_TTL_SEC`)  | 클라이언트가 나중에 조회할 시간. Day 23 까지의 값을 유지                                                       |
| `agent:result:<id>` FAILED       | 86400초 (1일)                | `RESULT_FAILED_TTL_SEC` (없으면 DONE 과 같음)    | 실패 원인을 나중에 볼 수 있게. DONE 과 따로 줄일 수 있게 변수는 나눔                                          |
| `agent:conversation:<sessionId>` | 3600초 (1시간, 슬라이딩)     | `CONVERSATION_TTL_SEC`                           | 마지막 대화 후 1시간이 지나면 새 대화. 저장할 때마다 새로 계산. 크기는 최근 10턴으로 제한(`MAX_TURNS`)         |
| `agent:lock:<name>`              | 150초                        | `REDIS_LOCK_TTL_SEC` (`redis_lock.py`, 바뀐 것 없음) | 락을 잡은 워커가 죽어도 풀리게. 한 건 최대 120초보다 길게                                                |

이 값들은 **설계값**이다. 실제 사용 패턴이나 메모리 사용량을 재서 정한 것이 아니다.


## 3. 개념

| 항목                          | 내용                                                                                                                                                       |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 상태마다 TTL 을 다르게 하는 이유 | 결과 키는 쓸 때마다 TTL 이 새로 시작된다. PROCESSING 으로 쓰면 600초, DONE 으로 갱신하면 86400초가 된다. 워커가 죽어 DONE 이 안 오면 600초 뒤 키가 사라진다 |
| PROCESSING 이 사라지면        | 조회하는 쪽에는 "결과가 없다"(404)가 보인다. 처리 중인 것과 없는 id 가 구분되지 않지만, 10분 넘게 PROCESSING 이었다면 어차피 비정상이다                    |
| 슬라이딩 TTL                  | 저장할 때마다 새로 시작한다. 계속 대화하는 세션은 만료되지 않는다. 절대 수명(예: 하루가 지나면 무조건 삭제)은 두지 않았다                                   |
| 키가 영원히 남는 경우         | `SET` 에 TTL 을 안 주면 영구(-1)다. 이런 키는 쌓이기만 하므로, `agent:*` 키는 모두 TTL 이 있어야 한다는 원칙으로 점검한다(①의 10번, ③)                       |
| 메모리 한도                   | Redis 에 `maxmemory` 를 걸면 가득 찰 때 키를 지우는 정책이 동작한다. 일반적으로 `volatile-*`, `allkeys-*` 정책은 락 키도 지울 수 있어 락의 상호 배제를 깰 수 있다. 지금은 한도를 걸지 않았다 |
| 쓰지 않은 것                  | TTL 에 무작위 편차를 주는 것(jitter). 한꺼번에 만료돼도 이 규모에서는 문제가 되지 않는다고 보고 넣지 않았다. 부하 테스트(Day 25 이후)에서 필요해지면 다시 본다    |


## 4. 바뀐 것

| 파일                          | 바뀐 것                                                                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `services/ttl_policy.py`      | 새 파일. 결과(상태별), 대화 TTL 을 한곳에 모았다. 0 이하 값은 시작할 때 예외. `problems()` 가 값끼리 어긋나는지 본다                  |
| `services/result_store.py`    | `TTL_SEC` 하나 대신 `ttl_policy.result_ttl(status)` 로 상태별 TTL 을 건다. 기존 줄은 `[OLD Day23]` 주석으로 보존                       |
| `services/conversation_store.py` | `TTL_SEC` 를 `ttl_policy` 에서 가져온다(환경변수 이름과 기본값은 그대로). 기존 줄은 `[OLD Day20]` 주석으로 보존                     |
| `checks/result_check.py`    | Day 23 확인 스크립트가 TTL 이 둘 이상이 된 것에 맞게 바뀌었다(7번 항목이 `ttl_policy` 값을 잠깐 바꿔 시험한다)                        |
| `checks/ttl_check.py`       | 새 확인 스크립트(①)                                                                                                                    |

락 TTL(`redis_lock.py`), 결과 조회 주소, 응답 모양은 그대로다.


## 5. 준비

| 항목     | 방법                                                                                                                              |
| -------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 컨테이너 | `docker ps` 에 `redis1`, Kafka, Qdrant 가 있고, Spring(8080)이 떠 있다 (①, ③, ④ 는 Redis 만 있으면 된다)                           |
| 워커     | ②, ⑤ 는 **8000 한 대**면 된다. **코드가 바뀌었으니 이미 떠 있는 워커는 끄고 다시 띄운다** (Ctrl+C 후 아래 명령)                    |
| 공급자   | ②, ⑤ 는 `provider` 를 `claude` 로 하면 빠르다. 서버 `.env` 에 `ANTHROPIC_API_KEY` 가 필요하다                                      |
| 임시 키  | 이 가이드가 만드는 키: `agent:result:*`(요청마다), `agent:conversation:d24-a`. ⑤ 끝에서 지운다                                     |

```bash
# 8000 = 워커 1
cd d2-ai-python
export KAFKA_WORKER_ID=w1
uvicorn app.main:app --port 8000
```


## 6. 실행과 확인

### ① 정책 확인 스크립트

**목적:** 정책대로 TTL 이 걸리는지, 대화 TTL 이 새로 시작되는지, 락이 풀리는지, TTL 없는 키가 없는지를 한 번에 본다. 약 10초.

```bash
cd d2-ai-python
python -m app.checks.ttl_check
```

기대: 12개 항목이 모두 `PASS`, 끝에 `== 결과: PASS (12/12)`. 중간에 현재 Redis 의 `agent:*` 키를 종류별로 세어 보여 준다. 10번이 `FAIL` 이면 TTL 없는 키가 있다는 뜻이고, 어떤 키인지 함께 나온다.

### ② 실제 요청에서 TTL 이 바뀌는 모양

**목적:** 요청 한 건이 PROCESSING 일 때와 DONE 이 된 뒤의 TTL 이 정책대로 다른지 본다. 약 10초.

```bash
ID=$(curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"claude"}' \
  | sed -E 's/.*"requestId":"([^"]+)".*/\1/')
for i in $(seq 1 10); do echo "$(date +%T) $(docker exec redis1 redis-cli TTL agent:result:$ID)"; sleep 1; done
```

기대(확인 전 예상): 처음 한두 줄은 `-2`(키 없음, 워커가 아직 안 받음), 그다음 `59x` 근처(PROCESSING, 600초에서 줄어드는 중), 답이 나온 뒤 `8639x`(DONE, 86400초에서 줄어드는 중)로 **껑충 뛴다**. Claude 로는 처리에 3~4초 걸리므로 600대 줄은 몇 개 안 보일 수 있다.

### ③ 지금 Redis 의 키별 TTL

**목적:** 실제로 쌓인 `agent:*` 키가 모두 TTL 을 가지고 있는지 본다. 즉시.

```bash
docker exec redis1 sh -c "redis-cli --scan --pattern 'agent:*' | while read k; do echo \"\$(redis-cli TTL \"\$k\") \$k\"; done"
```

기대(확인 전 예상): 앞 숫자가 모두 양수(남은 초). `-1`(영구)이 있는 줄이 없다. 키가 하나도 없으면 아무것도 안 나온다.

### ④ Redis 메모리 설정 (읽기만)

**목적:** 지금 Redis 에 메모리 한도와 삭제 정책이 걸려 있는지 본다. 설정은 바꾸지 않는다. 즉시.

```bash
docker exec redis1 redis-cli CONFIG GET maxmemory
docker exec redis1 redis-cli CONFIG GET maxmemory-policy
docker exec redis1 redis-cli INFO memory | grep -E "used_memory_human|maxmemory_human"
```

기대(확인 전 예상): `maxmemory` 가 `0`(한도 없음), `maxmemory-policy` 가 `noeviction`(Redis 기본값으로 알려져 있다). 이 경우 메모리가 차도 키를 자동으로 지우지 않고, 키는 TTL 로만 사라진다.

### ⑤ (선택) 대화 TTL 이 새로 시작되는지

**목적:** Kafka 경로에서 같은 `sessionId` 로 다시 물으면 대화의 남은 TTL 이 처음 값으로 돌아오는지 본다. 약 1분.

```bash
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"환불은 며칠 안에 신청해야 하나요?","provider":"claude","sessionId":"d24-a"}'
sleep 10; curl -s localhost:8000/api/v1/agent/sessions/d24-a | sed -E 's/.*("ttlSec":[0-9]+).*/\1/'

sleep 30
curl -s -X POST localhost:8080/api/agent/requests -H "Content-Type: application/json" \
  -d '{"question":"방금 말한 기간을 넘기면 어떻게 되나요?","provider":"claude","sessionId":"d24-a"}'
sleep 10; curl -s localhost:8000/api/v1/agent/sessions/d24-a | sed -E 's/.*("ttlSec":[0-9]+).*/\1/'

curl -s -X DELETE localhost:8000/api/v1/agent/sessions/d24-a
```

기대(확인 전 예상): 첫 조회는 `"ttlSec":359x`(저장 뒤 몇 초 지난 값), 둘째 조회도 `"ttlSec":359x`. 둘째 질문을 저장하면서 TTL 이 새로 시작됐으므로 30초 이상 지났어도 `356x` 로 줄어 있지 않다.


## 7. 이번에 하지 않은 것 (기록)

| 항목                          | 내용                                                                                                                                         |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| 대화의 절대 수명              | 슬라이딩만 둔다. 계속 대화하는 세션은 만료되지 않는다. 크기는 10턴 제한이 막는다                                                              |
| TTL 편차(jitter)              | 넣지 않았다. 부하 테스트(Day 25 이후)에서 만료가 한꺼번에 몰리는 문제가 보이면 다시 본다                                                      |
| Redis `maxmemory` 설정        | 바꾸지 않았다. ④ 에서 현재 값만 본다                                                                                                          |
| 락 TTL 의 이동                | `redis_lock.py` 에 그대로 둔다. 락은 한 건 최대 시간과 맞물려서 따로 두는 편이 낫다고 보았다                                                  |
| PROCESSING 사라짐 구분         | 처리 중과 없는 id 를 구분해 알려 주는 응답은 만들지 않았다                                                                                    |


## 8. 확인한 것과 하지 않은 것

| 구분              | 내용                                                                                                                                                                                    |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 확인함(샌드박스)  | `ttl_check.py` 12개 항목 PASS(실제 `redis-server`). `result_check.py` 12개 항목 PASS(바뀐 TTL 구조에서). 영구 키를 일부러 넣으면 10번이 FAIL 로 나옴. `RESULT_PROCESSING_TTL_SEC=0` 이면 시작할 때 `ValueError`. `RESULT_TTL_SEC` 만 주면 DONE, FAILED 가 그 값을 씀 |
| 확인하지 못함     | 내 환경의 Redis(`redis1`)에서의 ①, 실제 Kafka, Spring 8080, 워커를 통한 ②, ⑤. 샌드박스에는 Kafka, Spring, aiokafka 가 없다                                                              |
| 확인하지 않을 것  | 실제 사용량에 맞춘 TTL 값의 적정성, 메모리 사용량 예측, 대량 요청에서의 만료 동작 (부하 테스트 이후)                                                                                       |


## 9. 체크리스트

- [x] 코드 변경 (`ttl_policy.py`, `result_store.py`, `conversation_store.py`, `result_check.py`)
- [x] 확인 스크립트 작성 (`ttl_check.py`)
- [x] 샌드박스에서 `ttl_check.py` (12/12 PASS), `result_check.py` (12/12 PASS)
- [x] ① 내 환경에서 `python -m app.checks.ttl_check` (12/12 PASS. DAY24_RESULT 3-1)
- [x] ② 실제 요청의 TTL 변화 (600초대 → 86400초대로 뜀. DAY24_RESULT 3-2)
- [x] ③ 지금 Redis 의 키별 TTL (14개 모두 양수, 영구 없음. DAY24_RESULT 3-3)
- [x] ④ Redis 메모리 설정 보기 (maxmemory 0, noeviction. DAY24_RESULT 3-4)
- [x] ⑤ (선택) 대화 TTL 갱신 (3589 → 3585, 갱신됨. DAY24_RESULT 3-5)
- [x] 결과를 `DAY24_RESULT.md` 에 기록 (①~⑤)
