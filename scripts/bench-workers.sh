#!/usr/bin/env bash
# Day 34: 가짜 모델 워커를 W대 한꺼번에 띄우고 끈다 (프로젝트 루트에서 실행. 파이썬 가상환경을 켠 터미널에서).
#   시작: bash scripts/bench-workers.sh start <워커 수> <토픽> <그룹> [모델 대기 초(기본 2) | real = 실제 모델]
#   상태: bash scripts/bench-workers.sh status
#   종료: bash scripts/bench-workers.sh stop
# 워커 i(1부터)는 포트 8000+i-1, 이름 w<i>. 로그는 scripts/k6/out/day34-worker-<i>.log, 프로세스 번호는 /tmp/day34-workers.pids.
# --reload 는 붙이지 않는다(부하 중 파일 감시가 측정을 흔든다).
CMD=${1:?"start | status | stop"}
PIDS=/tmp/day34-workers.pids
OUT=scripts/k6/out
case "$CMD" in
  start)
    W=${2:?"워커 수"}; TOPIC=${3:?"토픽"}; GROUP=${4:?"그룹"}; DELAY=${5:-2}
    # 이전에 죽은 워커의 번호가 남아 있으면 정리하고, 살아 있는 워커가 있으면 먼저 stop 하라고 알린다
    if [ -s "$PIDS" ]; then
      alive=""
      while read -r pid; do kill -0 "$pid" 2>/dev/null && alive="$alive $pid"; done < "$PIDS"
      if [ -n "$alive" ]; then echo "이미 떠 있는 워커가 있다($alive ). 먼저 stop"; exit 1; fi
      : > "$PIDS"
    fi
    mkdir -p "$OUT"
    for i in $(seq 1 "$W"); do
      port=$((8000 + i - 1))
      if lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then echo "포트 $port 를 이미 쓰고 있다. 그 프로세스를 먼저 끈다"; bash "$0" stop; exit 1; fi
      # 워커 본체의 프로세스 번호를 기록한다(cd 와 변수 지정을 && 로 묶어 & 를 붙이면 껍데기 번호가 기록된다)
      (cd d2-ai-python
       # 모델 대기 인자가 real 이면 가짜 모델 설정을 주지 않는다(실제 모델 워커. Day 34.2)
       if [ "$DELAY" = "real" ]; then
         KAFKA_WORKER_ID="w$i" KAFKA_TOPIC="$TOPIC" KAFKA_GROUP="$GROUP" \
         nohup uvicorn app.main:app --port "$port" > "../$OUT/day34-worker-$i.log" 2>&1 &
       else
         KAFKA_WORKER_ID="w$i" KAFKA_TOPIC="$TOPIC" KAFKA_GROUP="$GROUP" \
         FAKE_LLM_ENABLED=true FAKE_LLM_DELAY_SEC="$DELAY" FAKE_LLM_JITTER=0 \
         nohup uvicorn app.main:app --port "$port" > "../$OUT/day34-worker-$i.log" 2>&1 &
       fi
       echo $! >> "$PIDS")
    done
    sleep 4
    dead=0
    while read -r pid; do kill -0 "$pid" 2>/dev/null || dead=$((dead + 1)); done < "$PIDS"
    if [ "$dead" -gt 0 ]; then
      echo "FAIL: 워커 ${dead}대가 시작하자마자 죽었다. 로그를 본다: tail -20 $OUT/day34-worker-1.log"
      echo "      (ModuleNotFoundError 면 가상환경이 안 켜진 것이다: source d2-ai-python/venv/bin/activate)"
      bash "$0" stop; exit 1
    fi
    echo "워커 ${W}대 시작 (토픽 $TOPIC, 그룹 $GROUP, 모델 대기 ${DELAY}, 숫자면 초). 파티션 배정까지 10~20초 기다린 뒤 status 로 본다"
    ;;
  status)
    for port in 8000 8001 8002 8003 8004 8005 8006 8007; do
      j=$(curl -s --max-time 2 "localhost:$port/health") || continue
      [ -z "$j" ] && continue
      echo "$port $(echo "$j" | grep -oE '"worker_id" *: *"[^"]*"') $(echo "$j" | grep -oE '"partitions" *: *\[[0-9, ]*\]') $(echo "$j" | grep -oE '"topic" *: *"[^"]*"') $(echo "$j" | grep -oE '"group" *: *"[^"]*"')"
    done
    ;;
  stop)
    if [ -s "$PIDS" ]; then
      while read -r pid; do kill "$pid" 2>/dev/null; done < "$PIDS"
      sleep 2
      while read -r pid; do kill -9 "$pid" 2>/dev/null; done < "$PIDS"
      : > "$PIDS"
    fi
    # 기록된 번호로 못 끈 워커(이전 버전 스크립트가 남긴 것 등)는 8000~8007 포트를 쓰는 python 프로세스를 찾아 끈다
    for port in 8000 8001 8002 8003 8004 8005 8006 8007; do
      for pid in $(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null); do
        if ps -p "$pid" -o command= | grep -qi python; then kill "$pid" 2>/dev/null; fi
      done
    done
    sleep 2
    for port in 8000 8001 8002 8003 8004 8005 8006 8007; do
      for pid in $(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null); do
        if ps -p "$pid" -o command= | grep -qi python; then kill -9 "$pid" 2>/dev/null; fi
      done
    done
    echo "워커 종료. 남은 포트:"; left=0
    for port in 8000 8001 8002 8003 8004 8005 8006 8007; do lsof -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && { echo "  $port 아직 사용 중"; left=1; }; done
    [ "$left" -eq 0 ] && echo "  없음"; true
    ;;
  *) echo "start | status | stop"; exit 1;;
esac
