# k6 부하 테스트 스크립트 (Day 25~)

| 파일         | 용도                                                                                          |
| ------------ | --------------------------------------------------------------------------------------------- |
| `smoke.js`   | 1명이 1건: 접수(202)와 결과(DONE)까지 경로가 열려 있는지                                      |
| `ramping.js` | 가상 사용자를 0 → MAX_VUS 로 올렸다 내리는 시나리오. `MODE=async` 또는 `sync`                 |

실행 방법과 기대값은 `docs/DAY25_GUIDE.md`, 코드 설명은 `docs/DAY25_CODE_GUIDE.md`.

## 쌓인 요청 비우기 (async 시험 뒤)

async 로 보내면 워커가 처리하는 속도보다 요청이 빨리 쌓인다. 시험이 끝나도 워커는 남은 요청을 계속 처리한다.
비우려면 워커를 모두 끈 뒤 그룹의 읽은 위치를 토픽 끝으로 옮긴다.

```
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group agent-workers --topic agent.requests --reset-offsets --to-latest --execute
```

그 뒤 워커를 다시 켠다. 남은 요청의 결과는 `PROCESSING` 이나 없음으로 남을 수 있다.
