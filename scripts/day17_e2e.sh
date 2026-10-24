#!/bin/bash
# Day 17: Spring(8080) -> Kafka -> FastAPI(8000) 전 구간 E2E 확인 스크립트
#
# 하는 일
#   (Day 18) FASTAPI 에 주소를 공백으로 여러 개 주면 워커 여러 개를 한꺼번에 본다. 건수는 합산하고,
# Day 23: 결과가 Redis 에 있어 어느 워커 주소로 물어도 200 이 온다(처리한 워커 이름은 응답의 worker 로 안다). 주소를 차례로 묻는 코드는 그대로 둔다.
#
#   1) FastAPI(8000)의 /health 로 시작 시점의 건수를 읽는다
#   2) Spring(8080)에 요청 N건을 보내 202 와 requestId, 응답 시간을 확인한다
#   3) FastAPI(8000)의 /health 를 1초마다 읽어, 보낸 N건이 모두 처리(commit)될 때까지 기다린다
#   4) 요청마다 FastAPI(8000)의 결과 조회 API 로 상태(DONE/FAILED)와 답을 읽는다
#   5) 단계별 PASS/FAIL 과 처리 완료까지 걸린 시간을 출력한다
#
# 전제: Kafka, Spring(8080), FastAPI(8000)가 켜져 있고 다른 요청이 들어오지 않는다.
# Day 23: 답은 Redis 에 있어 재시작해도 남는다(TTL 기본 1일). 답이 "맞는지"는 판정하지 않는다.
#
# 사용: bash scripts/day17_e2e.sh [요청 건수, 기본 3]
# 환경변수: SPRING(기본 localhost:8080), FASTAPI(기본 localhost:8000. 워커가 여럿이면 "localhost:8000 localhost:8001 localhost:8002"),
#           E2E_TIMEOUT_SEC(기본 180), PROVIDER(기본 gemini)

SPRING="${SPRING:-localhost:8080}"      # 8080 = Spring WAS
FASTAPI="${FASTAPI:-localhost:8000}"    # 8000 = FastAPI (워커가 여럿이면 8000, 8001, 8002 ... 를 공백으로 구분)
HOSTS=($FASTAPI)
N="${1:-3}"
TIMEOUT="${E2E_TIMEOUT_SEC:-180}"
PROVIDER="${PROVIDER:-gemini}"
QUESTIONS=("단순 변심으로 환불하면 배송비는 누가 내요?" "환불은 며칠 안에 신청해야 하나요?" "교환 시 배송비는 누가 부담하나요?" "배송이 늦어지면 어떻게 하나요?" "주문을 취소하려면 어떻게 하나요?")

fail=0
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# JSON 에서 필드 하나를 꺼낸다 (콜론 앞뒤 공백이 있어도 된다). 값이 null 이거나 없으면 빈 문자열
num() { echo "$1" | grep -oE "\"$2\" *: *[0-9]+" | head -1 | sed 's/.*: *//'; }
str() { echo "$1" | grep -oE "\"$2\" *: *\"[^\"]*\"" | head -1 | sed 's/.*: *"//; s/"$//'; }

# 모든 워커의 /health 를 읽어 건수를 합산한다. 하나라도 읽지 못하면 실패(return 1).
# 결과: R F S C(합계), BUSY(처리 중인 워커 수), MODE(첫 워커의 commit_mode), PARTS(워커별 맡은 파티션 요약)
read_health() {
  R=0; F=0; S=0; C=0; BUSY=0; MODE=""; PARTS=""
  for h in "${HOSTS[@]}"; do
    j="$(curl -s --max-time 5 "http://$h/health")"
    [ -z "$j" ] && { echo "FAIL: FastAPI($h)에 연결할 수 없다"; return 1; }
    r="$(num "$j" received)"; c="$(num "$j" committed)"
    [ -z "$r" ] || [ -z "$c" ] && { echo "FAIL: /health 응답을 해석하지 못했다($h): $j"; return 1; }
    fv="$(num "$j" failed)"; sv="$(num "$j" skipped)"
    R=$((R + r)); F=$((F + ${fv:-0})); S=$((S + ${sv:-0})); C=$((C + c))
    [ -n "$(str "$j" current_request_id)" ] && BUSY=$((BUSY + 1))
    [ -z "$MODE" ] && MODE="$(str "$j" commit_mode)"
    PARTS="$PARTS $h:$(echo "$j" | grep -oE '"partitions" *: *\[[0-9, ]*\]' | head -1 | sed 's/.*: *//; s/ //g')"
  done
  return 0
}

echo "== 1) 시작 전 /health (${HOSTS[*]})"
read_health || exit 1
r0=$R; f0=$F; s0=$S; c0=$C; mode="$MODE"
echo "   received=$r0 failed=$f0 skipped=$s0 committed=$c0 commit_mode=$mode"
echo "   워커별 맡은 파티션:$PARTS"
if [ "$mode" != "manual" ]; then echo "   주의: commit_mode 가 manual 이 아니라 committed 건수는 검사하지 않는다"; fi

echo "== 2) 요청 ${N}건 전송 ($SPRING)"
ids=""
for i in $(seq 1 "$N"); do
  q="${QUESTIONS[$(( (i-1) % ${#QUESTIONS[@]} ))]}"
  out="$(curl -s -o "$tmp" -w "%{http_code} %{time_total}" --max-time 10 -X POST "http://$SPRING/api/agent/requests" \
    -H "Content-Type: application/json" -d "{\"question\":\"$q\",\"provider\":\"$PROVIDER\"}")"
  code="${out% *}"; t="${out#* }"
  rid="$(str "$(cat "$tmp")" requestId)"
  if [ "$code" = "202" ] && [ -n "$rid" ]; then
    echo "   [$i] 202 ${t}s requestId=$rid"
    ids="$ids $rid"
  else
    echo "   [$i] FAIL http=$code body=$(cat "$tmp")"; fail=1
  fi
done
accepted="$(echo $ids | wc -w | tr -d ' ')"
echo "   접수된 요청: $accepted / $N"
[ "$accepted" -ne "$N" ] && fail=1

if [ "$accepted" -eq 0 ]; then echo "RESULT: FAIL (접수된 요청이 없다)"; exit 1; fi
echo "== 3) 처리 완료 대기 (최대 ${TIMEOUT}초)"
start="$(date +%s)"
done_ok=0
while true; do
  read_health || { fail=1; break; }
  r=$R; f=$F; s=$S; c=$C
  processed=$(( (r - r0) + (f - f0) + (s - s0) ))
  elapsed=$(( $(date +%s) - start ))
  echo "   ${elapsed}s processed=$processed/$accepted committed=$((c - c0)) 처리 중인 워커=$BUSY/${#HOSTS[@]}"
  if [ "$processed" -ge "$accepted" ] && [ "$BUSY" -eq 0 ]; then
    if [ "$mode" != "manual" ] || [ $((c - c0)) -ge "$accepted" ]; then done_ok=1; break; fi
  fi
  [ "$elapsed" -ge "$TIMEOUT" ] && break
  sleep 1
done

echo "== 4) 결과 조회 (GET /api/v1/agent/results/{requestId}, 워커 주소를 차례로 물어 200 이 오는 곳에서 읽는다. Day 23 부터 첫 주소에서 200 이 온다)"
n_done=0; n_failed=0; i=0; who=""; parts=""; sum_ms=0
for rid in $ids; do
  i=$((i+1)); code=""; json=""
  for h in "${HOSTS[@]}"; do
    resp="$(curl -s --max-time 5 -w ' HTTP%{http_code}' "http://$h/api/v1/agent/results/$rid")"
    code="${resp##* HTTP}"; json="${resp% HTTP*}"
    [ "$code" = "200" ] && break
  done
  st="$(str "$json" status)"; ans="$(str "$json" answer)"; ms="$(num "$json" elapsedMs)"; er="$(str "$json" error)"
  wk="$(str "$json" worker)"; pt="$(num "$json" partition)"
  ans="${ans//\\n/ }"   # 답 안의 줄바꿈(JSON 의 \n 표기)은 공백으로 바꿔 한 줄로 보여 준다
  if [ "$code" = "200" ] && [ "$st" = "DONE" ]; then
    n_done=$((n_done+1)); sum_ms=$((sum_ms + ${ms:-0})); echo "   [$i] DONE ${ms}ms worker=${wk:-?} partition=${pt:-?} answer=${ans:0:40}"
  elif [ "$code" = "200" ] && [ "$st" = "FAILED" ]; then
    n_failed=$((n_failed+1)); echo "   [$i] FAILED worker=${wk:-?} partition=${pt:-?} error=$er"
  else
    echo "   [$i] FAIL 어느 워커에서도 결과를 읽지 못했다 http=$code status=${st:-없음} requestId=$rid"; fail=1
  fi
  who="$who$wk
"; parts="$parts$pt
"
done
echo "   DONE ${n_done}건, FAILED ${n_failed}건"
if [ "${#HOSTS[@]}" -gt 1 ] || [ "$N" -gt 1 ]; then
  echo "   워커별 처리 건수:"; printf '%s' "$who" | grep -v '^$' | sort | uniq -c | sed 's/^/     /'
  echo "   파티션별 건수:";   printf '%s' "$parts" | grep -v '^$' | sort | uniq -c | sed 's/^/     /'
fi

echo "== 5) 결과"
if [ "$done_ok" -eq 1 ]; then
  echo "   처리 완료: ${elapsed}초 (실패 $((f - f0))건, 건너뜀 $((s - s0))건)"
  # 서버가 잰 건당 처리 시간의 합 / 전체 완료 시간. 1.0 이면 한 건씩 차례로 처리, 워커가 나눠 처리하면 1 보다 커진다.
  # (요청이 어느 파티션으로 몰렸는지와 상관없이 병렬로 처리된 정도를 보려는 값이다)
  if [ "$elapsed" -gt 0 ] && [ "$sum_ms" -gt 0 ]; then
    ratio10=$(( sum_ms * 10 / (elapsed * 1000) ))
    echo "   처리 시간 합계 $((sum_ms / 1000))초 / 완료 시간 ${elapsed}초 = 동시 처리 배율 $((ratio10 / 10)).$((ratio10 % 10))배 (1.0 이면 한 건씩 처리)"
  fi
  [ "$((f - f0))" -gt 0 ] && echo "   주의: 실패한 요청이 있다. FastAPI 로그에서 requestId 를 찾아 본다"
else
  echo "   FAIL: ${TIMEOUT}초 안에 처리가 끝나지 않았다 (processed=$processed/$accepted)"; fail=1
fi
echo "   보낸 requestId:$ids"
echo "   전체 답은 GET http://${HOSTS[0]}/api/v1/agent/results/{requestId} 로 볼 수 있다 (Day 23 부터 어느 워커 주소로 물어도 된다)"
if [ "$fail" -eq 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; fi
exit "$fail"
