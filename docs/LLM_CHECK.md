# llm_check — LLM 연결 확인 스크립트

Gemini / GPT / Claude 에 아주 짧은 질문을 한 번씩 보내서 **키가 설정됐는지, 응답이 오는지, 얼마나 걸리는지** 확인한다.
파일: `app/checks/llm_check.py` (에이전트 코드와 독립)

## 실행

프로젝트 루트에서 venv 활성화 후 실행한다.

```bash
cd d2-ai-python
source venv/bin/activate

python -m app.checks.llm_check                  # 3개 모두
python -m app.checks.llm_check gemini           # 하나만
python -m app.checks.llm_check gemini gpt       # 골라서
python -m app.checks.llm_check gemini --tools   # 도구 호출(tool calling)까지
```

## 결과 읽는 법

| 표시 | 의미 |
|---|---|
| `[ OK ]` | 응답 받음. 모델명, 걸린 시간, 응답 일부가 같이 나온다 |
| `[SKIP]` | API 키가 비었거나 패키지가 없어서 시도하지 않음 (에러 아님) |
| `[FAIL]` | 호출했는데 실패. 에러 종류와 힌트가 같이 나온다 |

`--tools` 를 주면 모델이 `get_current_time` 도구 호출을 요청하는지 본다. (`도구 호출 OK` / `NO`)

## 자주 보는 FAIL 과 힌트

| 힌트 | 원인 / 대응 |
|---|---|
| 무료 한도 초과(429) | 잠시 후 재시도, 또는 다른 모델/티어 |
| 모델명이 없거나 종료됨 | 환경변수로 다른 모델명 지정 |
| API 키가 잘못됐거나 권한 없음 | `.env` 의 키 확인 |
| 30초 안에 응답 없음 | 네트워크/서버 상태 확인 |

## 설정 (`.env`)

| 변수 | 용도 | 기본 모델 |
|---|---|---|
| `GOOGLE_API_KEY` | Gemini | `gemini-3.1-flash-lite` (`GEMINI_MODEL` 로 변경) |
| `ANTHROPIC_API_KEY` | Claude | `claude-3-5-haiku-20241022` (`ANTHROPIC_MODEL` 로 변경) |
| `OPENAI_API_KEY` | GPT | `gpt-4o-mini` (`OPENAI_MODEL` 로 변경) |

- 키가 비어 있으면 해당 provider 는 `SKIP` 이다. 현재 Claude 는 API 비용 때문에 비워 둔 상태.
- GPT 를 쓰려면 `pip install langchain-openai` 가 필요하다. (`requirements.txt` 에는 아직 없음)
- Claude 기본 모델명은 오래된 이름이다. 404 가 나면 `ANTHROPIC_MODEL` 로 현재 모델명을 지정한다.

## 알아둘 점

- 호출할 때마다 토큰이 소모되고 무료 한도에 걸릴 수 있어서 질문은 일부러 짧게 했다.
- 실패하면 바로 보이도록 자동 재시도는 껐다. (`max_retries=0`, 타임아웃 30초)
- 에러 메시지에 API 키 값이 섞이면 `***` 로 가려서 출력한다.
- 에이전트의 `get_model()` 은 지금 gemini, claude 만 지원한다. 여기서 GPT 가 `OK` 여도 에이전트에서는 아직 쓸 수 없다.
