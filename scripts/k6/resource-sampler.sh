#!/usr/bin/env bash
# Day 28: 부하 시험 중 자원 사용량을 INTERVAL 초마다 한 줄씩 찍는다.
#   Spring(8080), FastAPI(8000), k6 프로세스의 CPU% 와 메모리(MB), kafka 컨테이너의 CPU% 와 메모리.
#   k6 가 CPU 를 다 쓰고 있으면 측정 한계가 서버가 아니라 k6 쪽이라는 뜻이라, 같이 본다.
# 사용: bash scripts/k6/resource-sampler.sh [INTERVAL=5]   (Ctrl+C 또는 kill 로 끝낸다)
INTERVAL=${1:-5}

pid_of_port() { lsof -ti "tcp:$1" -sTCP:LISTEN 2>/dev/null | head -1; }

while true; do
    ts=$(date +%H:%M:%S)
    for item in "spring:$(pid_of_port 8080)" "fastapi:$(pid_of_port 8000)" "k6:$(pgrep -x k6 | head -1)"; do
        name=${item%%:*}
        pid=${item#*:}
        if [ -n "$pid" ]; then
            ps -o %cpu=,rss= -p "$pid" 2>/dev/null | awk -v ts="$ts" -v n="$name" '{printf "%s %-8s cpu=%s%% rss=%.0fMB\n", ts, n, $1, $2/1024}'
        else
            echo "$ts $name   (없음)"
        fi
    done
    docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' kafka 2>/dev/null | sed "s/^/$ts /"
    sleep "$INTERVAL"
done
