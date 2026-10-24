// Day 28: Breakpoint 탐색 (async 접수 경로, 계단식 부하)
//
// [이 테스트가 하는 일]
//   가상 사용자(VU) 수를 계단처럼 올린다 (기본 100 → 200 → 400 → 600 → 800 → 1000). 각 계단에서 일정 시간 머물며
//   "질문 한 건 보내고 THINK_SEC 쉬기"를 반복한다. 계단마다 접수 응답 시간(p95)과 접수 성공 비율을 따로 재서,
//   처음으로 기준을 넘는 계단(= Breakpoint)이 어디인지 찾는다.
//   대상은 Spring(8080) POST /api/agent/requests 하나다 (접수만, 결과 조회 없음). 워커는 끄고 시험하는 것이 기본이다.
//   다음은 하지 않는다: 결과 조회(POLL), 동기 경로(sync), Kafka 에 쌓인 요청 정리.
//
// 계단 구분
//   각 계단은 올리는 구간(STEP_RAMP_SEC)과 머무는 구간(STEP_HOLD_SEC)으로 이루어진다.
//   요청마다 step 이름표를 붙인다. 머무는 구간은 그 계단의 VU 수(예: "400"), 올리는 구간은 "ramp", 마지막 내리는 구간은 "down".
//   결과 요약에는 계단별 값만 나온다(머무는 구간). 올리는 구간과 내리는 구간은 "ramp"/"down" 으로 따로 센다.
//
// 실행 예 (프로젝트 루트에서)
//   k6 run scripts/k6/breakpoint.js
//   k6 run -e STEPS=10,25,50,100,200 -e THINK_SEC=0 scripts/k6/breakpoint.js
//
// 환경변수(-e 이름=값)
//   STEPS          계단의 VU 수, 쉼표로 구분 (기본 100,200,400,600,800,1000)
//   STEP_RAMP_SEC  각 계단으로 올라가는 시간, 초 (기본 10)
//   STEP_HOLD_SEC  각 계단에 머무는 시간, 초 (기본 40)
//   RAMP_DOWN_SEC  마지막에 0 으로 내리는 시간, 초 (기본 10)
//   THINK_SEC      요청 하나 끝난 뒤 쉬는 시간, 초 (기본 1. 0 이면 쉬지 않고 계속 보낸다)
//   PROVIDER       요청의 provider (기본 fake)
//   P95_MS         계단별 접수 응답 시간 p(95) 기준, ms (기본 2000. Day 25 의 임시 기준과 같다)
//   MIN_OK_RATE    계단별 접수 성공 비율 기준 (기본 0.99)
//   ABORT          1 이면 어느 계단이 기준을 처음 넘는 순간 시험을 멈춘다 (기본 0. 끝까지 돌려 전체 곡선을 본다)
//   SPRING         주소 (기본 localhost:8080)
//
// 주의: 접수한 요청은 Kafka 에 계속 쌓인다(1초 쉼이면 VU 1000 에서 초당 약 1000건). 끝난 뒤 쌓인 요청을 비운다(DAY28_GUIDE 의 준비).
// 주의: 한 PC 에서 k6, Spring, Kafka 를 같이 돌리면 k6 자신이 병목이 될 수 있다. scripts/k6/run-breakpoint.sh 가 자원 사용량을 같이 기록한다.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const STEPS = (__ENV.STEPS || '100,200,400,600,800,1000').split(',').map((s) => parseInt(s.trim()));
const STEP_RAMP_SEC = parseInt(__ENV.STEP_RAMP_SEC || '10');
const STEP_HOLD_SEC = parseInt(__ENV.STEP_HOLD_SEC || '40');
const RAMP_DOWN_SEC = parseInt(__ENV.RAMP_DOWN_SEC || '10');
const THINK_SEC = parseFloat(__ENV.THINK_SEC || '1');
const PROVIDER = __ENV.PROVIDER || 'fake';
const P95_MS = parseInt(__ENV.P95_MS || '2000');
const MIN_OK_RATE = parseFloat(__ENV.MIN_OK_RATE || '0.99');
const ABORT = (__ENV.ABORT || '0') === '1';
const SPRING = __ENV.SPRING || 'localhost:8080';
const SUBMIT = 'POST /api/agent/requests';

const QUESTIONS = [
    '단순 변심으로 환불하면 배송비는 누가 내요?',
    '환불은 며칠 안에 신청해야 하나요?',
    '교환 시 배송비는 누가 부담하나요?',
    '배송이 늦어지면 어떻게 하나요?',
    '주문을 취소하려면 어떻게 하나요?',
];

// 지표: 접수 성공 비율, 응답 코드별 개수 (모두 step 이름표를 붙여 계단별로 본다)
const accepted = new Rate('accepted');
const st202 = new Counter('status_202');
const st503 = new Counter('status_503');
const stOther5xx = new Counter('status_other_5xx');
const stZero = new Counter('status_0'); // 응답 없음: 접속 거부, 시간 초과, 연결 끊김
const stOther = new Counter('status_other');

// 단계 → stages. 계단마다 [올리기, 머물기] 두 구간, 마지막에 0 으로 내리기
const stages = [];

for (const s of STEPS) {
    stages.push({ duration: `${STEP_RAMP_SEC}s`, target: s });
    stages.push({ duration: `${STEP_HOLD_SEC}s`, target: s });
}
stages.push({ duration: `${RAMP_DOWN_SEC}s`, target: 0 });

// 계단별 임계값. 통과 기준이 아니라 계단마다 PASS/FAIL 을 표시하고 요약에 계단별 값이 나오게 하는 용도다.
// 계단마다 같은 기준을 걸어 두면 "처음 FAIL 이 나온 계단"이 곧 Breakpoint 이다.
const thresholds = {};

for (const s of STEPS) {
    const abort = (t) => (ABORT ? [{ threshold: t, abortOnFail: true, delayAbortEval: '5s' }] : [t]);

    thresholds[`http_req_duration{name:${SUBMIT},step:${s}}`] = abort(`p(95)<${P95_MS}`);
    thresholds[`accepted{step:${s}}`] = abort(`rate>${MIN_OK_RATE}`);
    // 아래는 요약에 계단별 값을 보이게 하는 표시용(항상 통과)
    thresholds[`http_reqs{step:${s}}`] = ['count>=0'];
    thresholds[`status_202{step:${s}}`] = ['count>=0'];
    thresholds[`status_503{step:${s}}`] = ['count>=0'];
    thresholds[`status_other_5xx{step:${s}}`] = ['count>=0'];
    thresholds[`status_0{step:${s}}`] = ['count>=0'];
    thresholds[`status_other{step:${s}}`] = ['count>=0'];
}

export const options = {
    scenarios: {
        steps: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages,
            gracefulRampDown: '5s', // VU 가 줄 때 진행 중인 반복에 주는 시간
        },
    },
    thresholds,
};

// 지금이 어느 구간인지: 시험 시작 뒤 지난 시간으로 계산한다
function currentStep() {
    const elapsed = Date.now() - exec.scenario.startTime;
    const cycleMs = (STEP_RAMP_SEC + STEP_HOLD_SEC) * 1000;
    const idx = Math.floor(elapsed / cycleMs);
    if (idx >= STEPS.length) return 'down';
    const phase = elapsed - idx * cycleMs;
    return phase < STEP_RAMP_SEC * 1000 ? 'ramp' : String(STEPS[idx]);
}

function question() {
    return QUESTIONS[Math.floor(Math.random() * QUESTIONS.length)];
}

export default function () {
    const step = currentStep(); // 요청을 보내기 직전의 구간으로 이름표를 붙인다
    const res = http.post(`http://${SPRING}/api/agent/requests`, JSON.stringify({ question: question(), provider: PROVIDER }), {
        headers: { 'Content-Type': 'application/json' },
        timeout: '10s', // 10초 넘게 응답이 없으면 실패(status 0)로 센다
        tags: { name: SUBMIT, step },
    });
    const ok = check(res, { '202 접수': (r) => r.status === 202 });
    const tag = { step };
    accepted.add(ok, tag);
    if (res.status === 202) st202.add(1, tag);
    else if (res.status === 503) st503.add(1, tag);
    else if (res.status >= 500) stOther5xx.add(1, tag);
    else if (res.status === 0) stZero.add(1, tag);
    else stOther.add(1, tag);
    if (THINK_SEC > 0) sleep(THINK_SEC);
}
