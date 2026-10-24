# Day 36 코드 가이드 — 모델 폴백 + 서킷 브레이커

실험은 [DAY36_GUIDE.md](DAY36_GUIDE.md). 파일은 `d2-ai-python/app/services/` 아래.

## 1. 흐름

`agent_node` 는 이전과 똑같이 `get_model(state["provider"])` 를 부르고 `bind_tools` → `invoke` 한다. provider 에 `>` 가 있으면 `get_model` 이 `FallbackChatModel` 을 돌려주고, 그 안에서 모델을 차례로 시도한다. 그래서 그래프 코드, Kafka 워커, Spring 은 바뀌지 않는다.

    provider "gemini>claude"
      → get_model: names=["gemini","claude"], 각각 get_model 로 만들어 FallbackChatModel 에 넣는다
      → invoke:
          gemini 차례: 브레이커가 열려 있으면 건너뜀(skipped) → claude 로
                       아니면 호출, 예외면 실패로 세고(error) → claude 로, 성공이면 연속 실패를 0으로(served)
          claude 는 마지막이라 건너뛰지 않는다. 실패하면 그 예외를 그대로 낸다

## 2. model_fallback.py

| 이름                                  | 하는 일                                                                                      |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| `threshold()`, `cooldown()`           | 환경변수 값(기본 3번, 30초). 호출할 때마다 읽는다                                            |
| `forced_down(name)`                   | `MODEL_FAULT_INJECTION=true` 이고 `MODEL_FORCE_DOWN` 에 이름이 있으면 True                   |
| `_state`, `STATS`                     | 모델별 연속 실패 수와 열린 시각, 호출 통계(`served:`, `error:`, `skipped:` + 모델 이름)       |
| `breaker_allow(name)`                 | 연속 실패 < 기준이면 True. 기준 이상이어도 쉬는 시간이 지났으면 True 로 한 번만 통과시키고 시각을 갱신(다시 쉼) |
| `breaker_success`, `breaker_failure`  | 성공이면 연속 실패 0, 실패면 +1 하고 기준에 닿으면 열린 시각을 지금으로                      |
| `FallbackChatModel.invoke`            | 위 흐름. 장애 주입이면 `MODEL_DOWN_DELAY_SEC` 만큼 기다린 뒤 `ModelDownError`                 |
| `FallbackChatModel.bind_tools`        | 안의 모델마다 `bind_tools` 한 새 객체를 돌려준다                                             |

락(`_lock`)으로 상태를 보호한다(워커는 스레드로 돈다). 상태는 프로세스 안에만 있다.

## 3. model_fallback_check.py

- 기본(가짜): `fake>fake-b` 로 A 정상, B 앞 모델 장애(+브레이커 효과 비교: 기준을 1000 으로 올려 브레이커를 사실상 끈 실행과 시간 비교), D 쉬는 시간 뒤 재시도 실패, C 복구, E 모두 장애, F 모르는 모델 이름. 모델 호출을 직접 하므로 서버가 필요 없고, 가짜 모델 지연은 0 으로 둔다.
- `--real`: `--only gemini|claude|fallback` 으로 한 단계만 돌릴 수 있고(`run_real` 의 `stages` 에서 고른 키가 아닌 단계는 건너뛴다), 없으면 세 단계가 모두 돈다. `run_agent` 를 질문마다 직접 부른다(Redis 대화 저장 없음). ① `gemini`(참고, 판정 안 함) ② `claude` ③ `gemini>claude`. 가짜 장애 없이 실제로 부른다. 질문은 5개를 돌려 쓰며(`QUESTIONS[i % 5]`) `--n`(한 단계에서 보낼 질문 수, 기본 3)으로 건수를 정한다. 질문은 한 건씩 차례로 보낸다. 성공 수, 평균·최대 시간, ③ 은 호출 기록(`STATS`)을 출력한다.
- 종료 코드: FAIL 이 하나라도 있으면 1, 아니면 0.

## 4. 보지 않는 것

- Kafka 워커, Spring 경유 요청. 폴백은 `get_model` 안에서 일어나므로 경로와 무관하지만 이 점검은 직접 호출만 한다.
- 서버 장애, 응답 지연(타임아웃) 같은 Gemini 장애. 실제 모델 단계는 무료 한도 등으로 Gemini 가 실제로 실패할 때만 폴백을 본다(안 걸리면 폴백이 안 일어난다). 가짜 모델 점검은 장애 주입(`MODEL_FORCE_DOWN`)을 쓴다.
- 워커가 여러 대일 때(상태가 프로세스마다 따로다).
