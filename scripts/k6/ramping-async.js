// Day 25: k6 부하 시나리오 - 비동기 구조 (ramping VU)
//
// [이 테스트가 하는 일]
//   가상 사용자(VU) 수를 0 → MAX_VUS → 0 으로 올렸다 내리면서, 각 VU 가 "질문 한 건 보내고 1초 쉬기"를 반복한다.
//   Spring(8080) POST /api/agent/requests 가 Kafka 에 넣고 202 로 바로 응답하는 구조의 접수 응답 시간을 잰다.
//   POLL=1 이면 접수 뒤 FastAPI(8000) 결과 조회로 DONE 이 될 때까지 기다려, 접수 시간과 완료 시간(e2e_ms)의 차이도 본다.
//   실제 LLM 대신 가짜 모델(provider=fake, 약 5초 지연)을 쓴다. 요청 내용 검증과 쌓인 Kafka 요청 정리는 하지 않는다.
//
// 실행 예 (프로젝트 루트에서)
//   k6 run -e MAX_VUS=20 scripts/k6/ramping-async.js
//   k6 run -e MAX_VUS=300 -e POLL=1 scripts/k6/ramping-async.js
//
// 환경변수(-e 이름=값)
//   MAX_VUS 최대 가상 사용자 수 (기본 20)   RAMP_UP 올라가는 시간 (기본 30s)   HOLD 유지 시간 (기본 30s)   RAMP_DOWN 내려가는 시간 (기본 10s)
//   THINK_SEC 요청 하나 끝난 뒤 쉬는 시간 (기본 1)   PROVIDER (기본 fake. FAKE_LLM_ENABLED=true 워커에서만 처리됨)
//   POLL 1 이면 결과가 DONE/FAILED 가 될 때까지 조회 (기본 0)   POLL_MAX_SEC 조회 최대 시간 (기본 60)
//   SPRING, FASTAPI 주소 (기본 localhost:8080, localhost:8000)
//
// 주의: 계속 보내면 Kafka 에 처리 못 한 요청이 쌓인다. 끝난 뒤 scripts/k6/README 의 "쌓인 요청 비우기"를 본다.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const MAX_VUS = parseInt(__ENV.MAX_VUS || '20');
const RAMP_UP = __ENV.RAMP_UP || '30s';
const HOLD = __ENV.HOLD || '30s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '10s';
const THINK_SEC = parseFloat(__ENV.THINK_SEC || '1');
const PROVIDER = __ENV.PROVIDER || 'fake';
const POLL = (__ENV.POLL || '0') === '1';
const POLL_MAX_SEC = parseInt(__ENV.POLL_MAX_SEC || '60');
const SUBMIT_NAME = 'POST /api/agent/requests';
const SPRING = __ENV.SPRING || 'localhost:8080';
const FASTAPI = __ENV.FASTAPI || 'localhost:8000';

const QUESTIONS = [
    '단순 변심으로 환불하면 배송비는 누가 내요?',
    '환불은 며칠 안에 신청해야 하나요?',
    '교환 시 배송비는 누가 부담하나요?',
    '배송이 늦어지면 어떻게 하나요?',
    '주문을 취소하려면 어떻게 하나요?',
];

// 우리가 정의한 지표 (k6 기본 지표 http_req_duration, http_req_failed 와 따로 본다)
const accepted = new Rate('accepted'); // 접수 성공(202)
const e2eMs = new Trend('e2e_ms', true); // POLL=1 일 때: 접수부터 DONE 까지
const done = new Counter('results_done');
const failed = new Counter('results_failed');
const pollTimeout = new Counter('results_poll_timeout');

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
    // 접수/실행 요청의 응답 시간에만 건다 (POLL=1 의 결과 조회 요청은 제외)
    thresholds: {
        accepted: ['rate>0.99'],
        [`http_req_duration{name:${SUBMIT_NAME}}`]: ['p(95)<2000'],
    },
};

// 질문 5개 중 하나를 무작위로 고른다 (가짜 모델은 질문 내용을 쓰지 않으므로 응답에는 영향 없음)
function question() {
    return QUESTIONS[Math.floor(Math.random() * QUESTIONS.length)];
}

// async: ① Spring 에 접수 요청 → ② 202 확인 → ③ (POLL=1) 결과가 DONE/FAILED 가 될 때까지 1초마다 조회
function runAsync() {
    const t0 = Date.now();
    const res = http.post(`http://${SPRING}/api/agent/requests`, JSON.stringify({ question: question(), provider: PROVIDER }), {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'POST /api/agent/requests' },
    });
    const ok = check(res, { '202 접수': (r) => r.status === 202 });
    accepted.add(ok);
    if (!ok || !POLL) return;

    const requestId = res.json('requestId');
    const deadline = Date.now() + POLL_MAX_SEC * 1000;
    while (Date.now() < deadline) {
        sleep(1);
        // 아직 워커가 받지 않은 요청은 404 가 정상이다. k6 기본 설정은 404 를 실패로 세어 http_req_failed 가 부풀려지므로(Day 25 ⑤ 에서 67.91%), 200 과 404 를 정상 응답으로 둔다.
        const r = http.get(`http://${FASTAPI}/api/v1/agent/results/${requestId}`, {
            tags: { name: 'GET /agent/results' },
            responseCallback: http.expectedStatuses(200, 404),
        });
        if (r.status === 200) {
            const st = r.json('status');
            if (st === 'DONE') {
                done.add(1);
                e2eMs.add(Date.now() - t0);
                return;
            }
            if (st === 'FAILED') {
                failed.add(1);
                return;
            }
        }
    }
    pollTimeout.add(1);
}

// VU 가 반복 실행하는 한 번의 동작: 요청 한 건 → THINK_SEC 쉬기
export default function () {
    runAsync();
    sleep(THINK_SEC);
}
