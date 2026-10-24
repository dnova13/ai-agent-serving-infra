#!/bin/bash
# d2-ai-python 폴더에서 실행한다: source run.sh
cd "$(dirname "$0")"
uvicorn app.main:app --reload --port 8000
