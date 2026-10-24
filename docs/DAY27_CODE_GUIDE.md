# Day 27 코드 가이드 — 비동기 재측정에 쓴 스크립트와 `health-probe.sh`

> 실행 방법과 확인 항목은 [DAY27_GUIDE.md](DAY27_GUIDE.md), 결과는 [DAY27_RESULT.md](DAY27_RESULT.md). 이 문서는 **Day 27 에서 쓴 스크립트가 무엇을 하는지**를 설명한다.
> 파일 경로는 프로젝트 루트(`ai-agent-serving-infra/`) 기준이다. 줄 번호는 코드를 고치면 밀리므로 함수 이름과 변수 이름으로 찾는다.
> 포트: **8080 = Spring WAS**, **8000 = FastAPI 워커 1**, **8001 = FastAPI 워커 2**, 9092 = Kafka, 6379 = Redis.

**목적:** Day 26 에서 동기 구조의 장애를 재현했다. Day 27 은 같은 부하를 비동기(Kafka) 구조로 다시 보내 접수 시간, 완료 시간, 처리 중 `/health` 를 재고 동기와 비교한다.

**Day 27 의 코드 변경:** 서버 코드(Spring, FastAPI)는 한 줄도 바꾸지 않았다. 새로 만든 것은 `scripts/k6/health-probe.sh` 하나이고, 나머지는 Day 25~26 에서 만든 스크립트를 환경변수만 바꿔 다시 썼다.


## 1. 한눈에 보기

| 파일                         | 만든 날 | Day 27 에서 하는 일                                                                  |
| ---------------------------- | ------- | ------------------------------------------------------------------------------------ |
| `scripts/k6/smoke.js`        | Day 25  | 요청 1건이 Spring → Kafka → 워커 → 결과 조회까지 이어지는지 확인 (실행 전 점검)       |
| `scripts/k6/ramping.js`      | Day 25  | `MODE=async`, `POLL=1` 로 VU 를 올리며 접수 시간과 완료 시간(`e2e_ms`)을 잰다         |
| `scripts/k6/health-probe.sh` | Day 27  | 부하가 걸려 있는 동안 `/health` 응답 시간을 일정 간격으로 잰다 (유일한 새 파일)        |
| `scripts/k6/README.md`       | Day 25  | "쌓인 요청 비우기": 측정 사이에 Kafka 에 남은 요청을 없애는 명령                       |

이전 이력에서의 위치: `ramping.js`, `smoke.js` 는 Day 25 커밋(`f8433ab`)에서, `MODE=sync`/`sync-direct` 는 Day 26 커밋(`16c9cdd`)에서 들어왔다. `health-probe.sh` 는 Day 27 커밋(`7ca9d61`)에서 들어왔다.


## 2. 전체 흐름

```
측정 전  : 워커를 모두 끄고 → 그룹의 읽은 위치를 토픽 끝으로 옮김(README) → 워커 켬 → smoke.js 로 경로 확인
측정 ①  : k6 run -e MODE=async -e MAX_VUS=20 ramping.js          (접수 응답 시간만)
측정 ②  : k6 run -e MODE=async -e POLL=1 ... ramping.js          (접수 + 완료 시간 e2e_ms)
          └ 동시에 health-probe.sh 가 20초 뒤부터 5초 간격으로 /health 를 호출
측정 ③  : 시험 뒤에도 남아 있는 요청(LAG)을 본다
측정 ④  : (선택) 8001 워커를 하나 더 켜서 같은 부하
```

| 단계 | 하는 일                                           | 파일                         | 위치                                  |
| ---- | ------------------------------------------------- | ---------------------------- | ------------------------------------- |
| A    | 접수 요청 보내기, 202 확인                        | `scripts/k6/ramping.js`      | `runAsync` 의 앞부분                  |
| B    | 결과가 DONE/FAILED 가 될 때까지 1초마다 조회      | `scripts/k6/ramping.js`      | `runAsync` 의 `while` 반복            |
| C    | VU 수를 올리고 유지하고 내리기                    | `scripts/k6/ramping.js`      | `options.scenarios.ramp` (`stages`)   |
| D    | 처리 중 `/health` 응답 시간 재기                  | `scripts/k6/health-probe.sh` | 파일 전체                             |
| E    | 경로 점검                                         | `scripts/k6/smoke.js`        | `default` 함수                        |
| F    | 쌓인 요청 비우기                                  | `scripts/k6/README.md`       | "쌓인 요청 비우기" 절                 |


## 3. 주요 코드

### 3-1. ramping.js 의 async 경로 (A, B)

**목적:** 접수가 빠른 것(수십 ms)과 실제로 끝나는 시간(수 초~수십 초)이 다름을 따로 잰다.

- 파일: `scripts/k6/ramping.js` (`runAsync`)

```javascript
const res = http.post(`http://${SPRING}/api/agent/requests`, JSON.stringify({ question: question(), provider: PROVIDER }), {...});
const ok = check(res, { '202 접수': (r) => r.status === 202 });
accepted.add(ok);
if (!ok || !POLL) return;                       // POLL=0 이면 접수 응답만 재고 끝낸다
...
while (Date.now() < deadline) {
    sleep(1);
    const r = http.get(`http://${FASTAPI}/api/v1/agent/results/${requestId}`, {
        responseCallback: http.expectedStatuses(200, 404),
    });
    if (r.status === 200) { ... 'DONE' 이면 e2eMs.add(Date.now() - t0) ... }
}
pollTimeout.add(1);
```

| 부분                         | 내용                                                                                                                                                                      |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `accepted` (Rate)            | 202 를 받은 비율. 통과 기준 `rate>0.99` 가 걸려 있다                                                                                                                       |
| `POLL`                       | 0(기본)이면 접수 응답만 잰다. 1 이면 접수 뒤 결과를 조회해 `e2e_ms`(접수부터 DONE 까지)를 함께 잰다. Day 27 ② 에서 켰다                                                    |
| `responseCallback`           | 아직 워커가 받지 않은 요청은 결과 조회가 404 인 것이 정상이다. k6 는 기본으로 404 를 실패로 세어 `http_req_failed` 가 부풀려지므로, 200 과 404 는 기대한 응답으로 표시한다  |
| `results_done/failed/poll_timeout` | DONE 으로 끝난 건수, FAILED 로 끝난 건수, `POLL_MAX_SEC` 안에 끝나지 않은 건수. 마지막이 많으면 워커가 요청을 따라가지 못한 것이다                                    |
| `question()`                 | 질문 5개 중 하나를 무작위로 고른다. 가짜 모델은 질문을 쓰지 않으므로 응답 시간에는 영향이 없다                                                                              |

### 3-2. VU 를 올리고 내리는 부분 (C)

- 파일: `scripts/k6/ramping.js` (`options`)

`ramping-vus` 실행기가 `RAMP_UP`(30s) 동안 0 → `MAX_VUS`, `HOLD`(30s) 동안 유지, `RAMP_DOWN`(10s) 동안 0 으로 내린다. 각 VU 는 `default` 함수(요청 1건 → `THINK_SEC` 초 쉬기)를 반복한다. `gracefulRampDown: '5s'` 는 VU 가 줄 때 진행 중인 반복에 주는 시간이다. 이 시간을 넘으면 그 반복은 중단되어 `POLL=1` 의 조회가 끝나지 못할 수 있다.

### 3-3. health-probe.sh (D)

**목적:** 워커가 에이전트를 처리하는 도중에도 `/health` 가 막히지 않는지 본다. Day 26 에서 동기 구조는 이때 `/health` 가 3초 넘게 막혔다.

- 파일: `scripts/k6/health-probe.sh`

```bash
DELAY=${1:-20}; COUNT=${2:-3}; INTERVAL=${3:-5}; URL=${4:-http://localhost:8000/health}
sleep "$DELAY"
for i in $(seq 1 "$COUNT"); do
    curl -s -o /dev/null -w "health status=%{http_code} time=%{time_total}s\n" "$URL"
    sleep "$INTERVAL"
done
```

| 인자       | 기본값                          | 의미                                                              |
| ---------- | ------------------------------- | ----------------------------------------------------------------- |
| `$1` DELAY | 20                              | 처음 기다릴 초. k6 가 VU 를 올려 워커가 바빠질 때까지 기다린다     |
| `$2` COUNT | 3                               | 호출 횟수                                                         |
| `$3` INTERVAL | 5                            | 호출 사이 간격(초)                                                |
| `$4` URL   | `http://localhost:8000/health`  | 호출할 주소                                                       |

`curl -w` 로 상태 코드와 전체 시간만 한 줄로 찍는다. k6 와 같이 쓰려면 `bash scripts/k6/health-probe.sh & k6 run ...; wait` 처럼 뒤에 `&` 를 붙여 병렬로 돌린다.

**왜 파일로 뺐나:** 처음에는 한 줄 명령으로 호출했는데, 터미널에 붙여 넣을 때 주소 뒤에 `\;` 가 붙어 `health;` 나 `sleep`, `5` 를 호출한 적이 있었다(DAY27_RESULT 3-3 의 1·2차 시도). 입력이 깨질 여지를 없애려고 파일로 만들었다.

### 3-4. smoke.js 와 쌓인 요청 비우기 (E, F)

- `smoke.js`: VU 1명, 반복 1번. 질문 1건 → 202 확인 → 1초마다 결과 조회 → DONE 이면 완료 시간과 처리한 워커를 출력한다. 부하 측정 전에 "Spring, Kafka, 워커, 가짜 모델이 한 줄로 이어져 있는지"를 보는 용도다.
- 비우기: async 로 계속 보내면 워커가 처리하는 속도보다 요청이 빨리 쌓인다. 측정 사이에 남은 요청이 다음 측정에 섞이지 않게, 워커를 모두 끄고(그룹이 Empty 여야 한다) 아래 명령으로 읽은 위치를 토픽 끝으로 옮긴다.

```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --topic agent.requests --reset-offsets --to-latest --execute
```


## 4. 시간 순서 (② 의 한 VU, POLL=1)

```
t=0.00  VU 가 POST /api/agent/requests            → Spring 이 Kafka 에 넣고 202 (수십 ms)       accepted=true
t=0.05  GET /agent/results/{id}                   → 404 (워커가 아직 안 받음)
...     1초마다 조회 (404 → 200 PROCESSING → 200 DONE)
t=6.1   DONE 확인                                  → e2e_ms = 약 6초(가짜 모델 5초 + 접수/조회 지연. Day 27 결과의 가장 빠른 완료가 6.07초)
        THINK_SEC(1초) 쉬고 다음 요청
```

워커가 요청을 따라가지 못하면 조회 구간이 길어지고 `e2e_ms` 가 늘어난다. 이 차이가 "접수는 빠르지만 완료는 느려진다"는 비동기 구조의 특성이다.


## 5. 설계 선택과 이유

| 선택                                               | 이유                                                                                                         |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 서버 코드를 바꾸지 않고 같은 스크립트를 다시 씀    | 동기와 비동기 비교에서 바뀌는 것은 구조뿐이어야 한다. 부하 모양(VU, 질문, 가짜 모델 5초)을 Day 26 과 같게 유지했다 |
| 접수 시간과 완료 시간을 따로 잼 (`accepted`, `e2e_ms`) | 비동기에서 접수가 빠르다는 것만으로는 시스템이 감당한다는 증거가 못 된다. 끝나는 시간과 남은 요청(LAG)을 함께 본다   |
| 측정 사이에 Kafka 를 비움                          | 앞선 측정의 남은 요청이 다음 측정의 완료 시간을 늘린다(Day 27 2차 측정에서 의심된 현상)                          |
| `/health` 호출을 별도 스크립트로 뺌                | k6 의 VU 와 독립적으로, 정해진 시각에 호출해 "그 순간의 응답 시간"만 본다                                      |


## 6. 코드를 따라 읽는 순서

1. `scripts/k6/ramping.js` 맨 위 주석(환경변수 표) → `runAsync` → `options`
2. `scripts/k6/smoke.js`
3. `scripts/k6/health-probe.sh`
4. `scripts/k6/README.md` 의 "쌓인 요청 비우기"
5. [DAY27_RESULT.md](DAY27_RESULT.md) 의 3-1 ~ 3-4 에서 위 스크립트가 어떤 숫자를 냈는지 확인
