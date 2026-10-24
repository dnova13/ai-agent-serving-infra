# 코드 분석

대상 디렉토리: `d1-ai-agent/ai-python`

## 1. 프로젝트 개요

LLM 기반 에이전트 실험을 위한 초기 단계 파이썬 프로젝트로, 두 가지 축으로 나뉘어 있습니다.

1. **멀티 LLM 프로바이더 추상화** (`app.py`) — LangChain 래퍼를 이용해 provider 문자열만 바꾸면 Gemini/Claude를 교체할 수 있도록 설계.
2. **Gemini 네이티브 기능 실험** (`test.py`, `weather-ai.py`) — Google `google-genai` SDK의 Interactions API를 이용한 함수 호출(Function Calling) + RAG 도구 연동 실험. Qdrant 연동 전이라 mock 데이터로 대체.

README에는 참고 링크(Gemini function-calling 공식 문서) 하나만 있고, 별도의 `requirements.txt`는 존재하지 않습니다(가상환경 `venv/`에 패키지가 직접 설치되어 있음).

## 2. 파일별 분석

### 2.1 `app.py` — 멀티 LLM 프로바이더 추상화

- `get_model(provider)` 함수 하나로 Gemini(`ChatGoogleGenerativeAI`)와 Claude(`ChatAnthropic`) 중 선택.
- Claude는 `claude-3-5-haiku-20241022`로 고정 — 주석에 "가볍고 저렴한 테스트용"이라 명시. 이후 새 모델(Sonnet 5, Opus 5 등)로 교체가 쉬운 구조.
- `__main__` 블록은 동일 질문을 두 provider에 각각 던져 응답을 비교하는 간단한 스모크 테스트.
- 존재하지 않는 provider 값에는 `ValueError`로 방어.
- 향후 OpenAI 등 추가 시 `elif` 블록만 추가하면 되는 확장 구조 — 단, provider가 늘어나면 이 방식은 각 provider별 파라미터(temperature, max_tokens 등) 차이를 다루기 어려워질 수 있음.

### 2.2 `test.py` — Gemini Interactions API + RAG 도구 실험

에이전트 루프를 5단계로 구현:

1. `search_vector_db` 도구 스펙 정의 (OpenAI 스타일 function schema)
2. `client.interactions.create()`로 질문 + 도구 목록 전달
3. 응답 `steps`를 순회하며 `function_call` 타입 존재 여부 확인
4. 있으면 `execute_search_vector_db()` mock 함수 실행 (실제 Qdrant 연동은 TODO)
5. `previous_interaction_id`로 이전 인터랙션에 실행 결과를 이어붙여 최종 답변 요청

파일 상단 주석에 "원본 예시는 함수 호출 요청 확인까지만 하는 반쪽짜리 코드였다"는 문제의식이 명시되어 있고, 이를 실제 실행 → 결과 반환까지 완성한 것이 이 파일의 목적입니다. 또한 "Interactions API는 Google 전용이라 멀티 모델 라우팅 목적이면 LangGraph + `app.py`(구 `model_provider.py`) 조합을 메인으로 유지하라"는 설계 의도가 주석으로 남아 있습니다.

### 2.3 `weather-ai.py` — 도구 2개 라우팅 실험

`test.py`를 확장해 도구를 2개(`weather_function`, `search_vector_db`)로 늘리고, 모델이 질문에 따라 올바른 도구를 선택하는지 라우팅을 테스트합니다. 상단에 직접 남긴 메모가 눈에 띕니다.

> "현재 구동은 완료. 그런데 테스트시 실제 ai 서비스가 제대로 동작하지 않음"

## 3. 발견된 이슈

### 3.1 (버그 의심) `test.py`와 `weather-ai.py`의 결과 반환 스키마 불일치

같은 목적(도구 실행 결과를 모델에 돌려주기)의 코드인데 스키마가 다릅니다.

| 구분 | `test.py` (동작 확인용 최초 버전) | `weather-ai.py` (확장 버전, 미동작) |
|---|---|---|
| `type` | `"function_call_output"` | `"function_result"` |
| 결과 필드명 | `"output"` | `"result"` |
| id 필드 | `function_call_step.call_id` | `function_call_step.id` |

`weather-ai.py`가 "구동은 되지만 결과가 제대로 동작하지 않는다"는 증상과 정확히 일치하는 원인으로 보입니다. Interactions API가 기대하는 스키마는 `test.py` 쪽(`function_call_output` / `output` / `call_id`)일 가능성이 높으며, `weather-ai.py`는 이를 임의로 바꿔서 작성된 것으로 추정됩니다.

### 3.2 (확인 필요) 존재 여부가 불확실한 모델명

두 파일 모두 `model="gemini-3.6-flash"`를 사용 중인데, 이는 공개적으로 알려진 Gemini 모델 네이밍 체계와 다릅니다. 오타이거나 실제 존재하지 않는 모델일 경우 API 호출 자체가 실패해 3.1과 별개로 "동작 안 함"의 원인이 될 수 있습니다. Google AI Studio에서 사용 가능한 정확한 모델 ID를 재확인할 필요가 있습니다.

### 3.3 의존성 관리 부재

`requirements.txt` / `pyproject.toml`이 없어 `venv/`를 통째로 커밋하지 않는 한(현재 `.gitignore` 확인 필요) 재현 가능한 환경 구성이 어렵습니다.

### 3.4 Mock 처리된 부분 (설계상 의도됨, 결함 아님)

- `execute_search_vector_db()`: Qdrant 미연동, mock 텍스트 반환 (`TODO(2주 차)` 주석으로 명시)
- `execute_get_current_temperature()`: 실제 날씨 API 미연동, mock 값 반환

## 4. 아키텍처 관찰

- **역할 분리가 명확함**: `app.py`(멀티 모델 라우팅, 프로덕션 지향) vs `test.py`/`weather-ai.py`(Gemini 전용 기능 실험, 프로토타이핑 지향)를 코드 내 주석으로 스스로 문서화하고 있음.
- **점진적 확장 전략**: mock → 실제 연동(Qdrant, 날씨 API)으로 교체하는 지점을 TODO 주석으로 미리 표시해 둠.
- **Interactions API의 이식성 한계**를 인지하고, 장기적으로는 LangGraph 기반 통합으로 수렴시키겠다는 방향을 코드에 남겨둔 점이 특징적.

## 5. 제안

1. `weather-ai.py`의 결과 반환 스키마를 `test.py` 쪽(`function_call_output`/`output`/`call_id`)으로 통일해서 실제 동작 여부를 재검증.
2. `gemini-3.6-flash` 모델 ID의 실존 여부를 Google AI Studio 콘솔에서 확인.
3. `requirements.txt` 또는 `pyproject.toml`을 추가해 의존성을 명시적으로 고정.
4. 두 실험 파일(`test.py`, `weather-ai.py`)의 중복 코드(도구 실행 루프)를 공통 함수로 추출하면 이후 도구가 늘어날 때 유지보수가 쉬워짐.
