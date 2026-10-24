# archive

지금의 시스템에서는 쓰지 않지만 지운 대신 옮겨 둔 파일이다(Day 43). 실행 경로(Spring, Kafka, FastAPI 워커, Redis, Qdrant)와는 관계가 없다.

| 위치 | 무엇 | 옮긴 이유 |
| --- | --- | --- |
| `claude/` | 프로젝트 초반에 만든 예제와 첫 작업본(FastAPI, NestJS, React 샘플, 첫 ai-test 앱) | 지금 코드는 `d2-ai-python`, `d3-was-spring` 에 있다 |
| `d1-ai-agent/` | 첫 docker-compose | 루트의 `docker-compose.yml` 하나만 쓴다 |
| `d2-ai-python/ai_test.py`, `test.py`, `weather-ai.py` | Day 1~2 의 모델 호출 시험 스크립트 | 에이전트는 `app/services/agent_graph.py` 로 옮겨 갔다 |
| `d2-ai-python/agent_core.py` | Day 2~5 의 첫 에이전트(수동 루프, mock 검색) | Day 3 부터 LangGraph(`agent_graph.py`)를 쓰고, 어디에서도 가져오지 않는다 |
| `d2-ai-python/READEME.md`, `CODE_ANALYSIS.md` | 초반 메모와 코드 분석(옛 폴더 `d1-ai-agent/ai-python` 기준) | 지금 구조와 맞지 않는다 |
