// Day 26: k6 부하 시나리오 - 동기 구조 (ramping VU)
//
// [이 테스트가 하는 일]
//   가상 사용자(VU) 수를 0 → MAX_VUS → 0 으로 올렸다 내리면서, 각 VU 가 "질문 한 건 보내고 답이 올 때까지 기다린 뒤 1초 쉬기"를 반복한다.
//   기본(Spring 경유): Spring(8080) POST /api/agent/sync → FastAPI 답을 기다렸다가 200, 30초 넘으면 504, 접속 실패 502.
//   DIRECT=1: Spring 을 빼고 FastAPI(8000) POST /api/v1/agent/run 을 바로 부른다(Spring 의 영향을 가려내는 비교용).
//   응답 코드별 개수(sync_200 / sync_504 / sync_502 / sync_other_5xx / sync_other)를 센다. 실제 LLM 대신 가짜 모델(약 5초 지연)을 쓴다.
//
// 실행 예 (프로젝트 루트에서)
//   k6 run -e MAX_VUS=20 scripts/k6/ramping-sync.js
//   k6 run -e DIRECT=1 -e MAX_VUS=20 scripts/k6/ramping-sync.js
//
// 환경변수(-e 이름=값)
//   DIRECT 1 이면 FastAPI 직접 호출 (기본 0)
//   POLL_MAX_SEC k6 쪽 요청 시간 제한 (기본 60. Spring 의 30초보다 길어야 504 를 받아볼 수 있다)
//   MAX_VUS, RAMP_UP, HOLD, RAMP_DOWN, THINK_SEC, PROVIDER, SPRING, FASTAPI 는 ramping-async.js 와 같다
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const DIRECT = (__ENV.DIRECT || '0') === '1';
const MAX_VUS = parseInt(__ENV.MAX_VUS || '20');
const RAMP_UP = __ENV.RAMP_UP || '30s';
const HOLD = __ENV.HOLD || '30s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '10s';
const THINK_SEC = parseFloat(__ENV.THINK_SEC || '1');
const PROVIDER = __ENV.PROVIDER || 'fake';
const POLL_MAX_SEC = parseInt(__ENV.POLL_MAX_SEC || '60');
const SUBMIT_NAME = DIRECT ? 'POST /api/v1/agent/run' : 'POST /api/agent/sync';
const SPRING = __ENV.SPRING || 'localhost:8080';
const FASTAPI = __ENV.FASTAPI || 'localhost:8000';

const QUESTIONS = [
    '단순 변심으로 환불하면 배송비는 누가 내요?',
    '환불은 며칠 안에 신청해야 하나요?',
    '교환 시 배송비는 누가 부담하나요?',
    '배송이 늦어지면 어떻게 하나요?',
    '주문을 취소하려면 어떻게 하나요?',
];

// 우리가 정의한 지표
const accepted = new Rate('accepted'); // 200 응답 비율
const syncOk = new Counter('sync_200');
const sync504 = new Counter('sync_504'); // Spring 이 30초 안에 FastAPI 답을 못 받음
const sync502 = new Counter('sync_502'); // Spring 이 FastAPI 에 접속 못 함
const sync5xx = new Counter('sync_other_5xx');
const syncOther = new Counter('sync_other'); // k6 시간 초과(status 0) 등

export const options = {
    scenarios: {
        ramp: {
            // ramping-vus: stages 에 따라 VU 수를 직선으로 늘리고 줄인다 (VU 마다 default 함수를 반복 실행)
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: RAMP_UP, target: MAX_VUS },
                { duration: HOLD, target: MAX_VUS },
                { duration: RAMP_DOWN, target: 0 },
            ],
            gracefulRampDown: '5s', // VU 가 줄 때 진행 중인 반복에 주는 시간. 넘으면 중단(interrupted)된다
        },
    },
    // 통과 기준이 아니라 결과 요약에 PASS/FAIL 표시를 붙이는 용도. 값은 Day 25 의 임시 기준이다.
    // 동기 요청의 응답 시간에 건다
    thresholds: {
        accepted: ['rate>0.99'],
        [`http_req_duration{name:${SUBMIT_NAME}}`]: ['p(95)<2000'],
    },
};

// 질문 5개 중 하나를 무작위로 고른다 (가짜 모델은 질문 내용을 쓰지 않으므로 응답에는 영향 없음)
function question() {
    return QUESTIONS[Math.floor(Math.random() * QUESTIONS.length)];
}

// 답이 올 때까지 기다린다. 본문 필드는 Spring 이 question, FastAPI 가 query 로 다르다
function runSync() {
    const url = DIRECT ? `http://${FASTAPI}/api/v1/agent/run` : `http://${SPRING}/api/agent/sync`;
    const payload = DIRECT ? { query: question(), provider: PROVIDER } : { question: question(), provider: PROVIDER };
    const res = http.post(url, JSON.stringify(payload), {
        headers: { 'Content-Type': 'application/json' },
        timeout: `${POLL_MAX_SEC}s`,
        tags: { name: SUBMIT_NAME },
    });
    const ok = check(res, { '200 응답': (r) => r.status === 200 });
    accepted.add(ok);
    if (res.status === 200) syncOk.add(1);
    else if (res.status === 504) sync504.add(1);
    else if (res.status === 502) sync502.add(1);
    else if (res.status >= 500) sync5xx.add(1);
    else syncOther.add(1);
}

// VU 가 반복 실행하는 한 번의 동작: 요청 한 건 → THINK_SEC 쉬기
export default function () {
    runSync();
    sleep(THINK_SEC);
}
