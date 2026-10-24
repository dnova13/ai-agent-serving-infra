#!/usr/bin/env bash
# 부하가 걸려 있는 동안 /health 응답 시간을 일정 간격으로 잰다 (Day 27 ②-2).
# 사용: bash scripts/k6/health-probe.sh [처음 기다릴 초=20] [횟수=3] [간격 초=5] [주소=http://localhost:8000/health]
# k6 와 같이 쓰는 법: bash scripts/k6/health-probe.sh & k6 run ... ; wait
DELAY=${1:-20}
COUNT=${2:-3}
INTERVAL=${3:-5}
URL=${4:-http://localhost:8000/health}
sleep "$DELAY"
for i in $(seq 1 "$COUNT"); do
    curl -s -o /dev/null -w "health status=%{http_code} time=%{time_total}s\n" "$URL"
    sleep "$INTERVAL"
done
