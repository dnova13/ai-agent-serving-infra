#!/usr/bin/env bash
# Day 28: Breakpoint 시험을 한 번에 실행한다 (프로젝트 루트에서).
#   ① 열 수 있는 파일(소켓) 수 한도를 올린다 (macOS 기본 256 은 VU 수백 개에서 부족하다)
#   ② 자원 샘플러를 뒤에서 돌린다
#   ③ k6 breakpoint.js 를 실행한다 (인자는 그대로 k6 로 넘어간다)
#   ④ 끝나면 샘플러를 끈다
#   k6 요약과 자원 기록은 scripts/k6/out/ 아래 파일에도 남는다.
# 사용: bash scripts/k6/run-breakpoint.sh 이름 [k6 옵션...]
#   예) bash scripts/k6/run-breakpoint.sh step1
#   예) bash scripts/k6/run-breakpoint.sh hammer -e STEPS=10,25,50,100,200 -e THINK_SEC=0
NAME=${1:?"이름이 필요합니다. 예: bash scripts/k6/run-breakpoint.sh step1"}
shift

OUT=scripts/k6/out
mkdir -p "$OUT"
ulimit -n 10240 2>/dev/null || echo "ulimit -n 을 올리지 못했다. 현재 한도: $(ulimit -n)"
echo "열 수 있는 파일 한도: $(ulimit -n)"

bash scripts/k6/resource-sampler.sh 5 > "$OUT/day28-$NAME-resources.txt" &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null' EXIT

k6 run "$@" scripts/k6/breakpoint.js 2>&1 | tee "$OUT/day28-$NAME-k6.txt"

kill $SAMPLER 2>/dev/null
echo
echo "자원 기록: $OUT/day28-$NAME-resources.txt"
cat "$OUT/day28-$NAME-resources.txt"
