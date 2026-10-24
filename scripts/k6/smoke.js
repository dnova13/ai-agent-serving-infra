// Day 25: 스모크 — 가상 사용자 1명이 요청 1건씩만 보내 경로가 열려 있는지 본다 (부하 아님)
//
// [이 테스트가 하는 일]
//   Spring(8080)에 질문 1건 → 202 확인 → FastAPI(8000)에서 1초마다 결과 조회 → DONE 이면 완료 시간과 처리한 워커를 출력한다.
//   ramping.js 를 돌리기 전에 "서버, Kafka, 워커, 가짜 모델이 한 줄로 이어져 있는지"를 확인하는 용도다. 60초 안에 DONE 이 아니면 실패.
//
// 실행: k6 run scripts/k6/smoke.js        (환경변수는 ramping.js 와 같은 이름: PROVIDER, SPRING, FASTAPI)
// 확인: Spring 202 → 결과가 DONE 이 될 때까지 조회 → 완료 시간 출력
import http from 'k6/http';
import { check, sleep, fail } from 'k6';

const PROVIDER = __ENV.PROVIDER || 'fake';
const SPRING = __ENV.SPRING || 'localhost:8080';
const FASTAPI = __ENV.FASTAPI || 'localhost:8000';

export const options = { vus: 1, iterations: 1 };

export default function () {
  const t0 = Date.now();
  const res = http.post(
    `http://${SPRING}/api/agent/requests`,
    JSON.stringify({ question: '환불은 며칠 안에 신청해야 하나요?', provider: PROVIDER }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { '202 접수': (r) => r.status === 202 });
  if (res.status !== 202) fail(`접수 실패 status=${res.status} body=${res.body}`);
  const requestId = res.json('requestId');
  console.log(`접수 ${res.timings.duration.toFixed(0)}ms requestId=${requestId}`);

  for (let i = 0; i < 60; i++) {
    sleep(1);
    const r = http.get(`http://${FASTAPI}/api/v1/agent/results/${requestId}`);
    if (r.status === 200 && r.json('status') === 'DONE') {
      console.log(`완료 ${Date.now() - t0}ms worker=${r.json('worker')} partition=${r.json('partition')} answer=${r.json('answer')}`);
      check(r, { 'DONE': () => true });
      return;
    }
    if (r.status === 200 && r.json('status') === 'FAILED') fail(`처리 실패 error=${r.json('error')}`);
  }
  fail('60초 안에 DONE 이 되지 않았다');
}
