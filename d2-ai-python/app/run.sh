#!/bin/bash
# 어느 폴더에서 실행해도 d2-ai-python 에서 서버를 띄운다: source app/run.sh
cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.."
uvicorn app.main:app --reload --port 8000
