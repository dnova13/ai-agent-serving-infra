"""
모델 폴백 + 서킷 브레이커 (Day 36)

[문제]
  provider 를 "gemini" 로 보내면 Gemini 만 부른다. Gemini 가 장애이면 요청이 그대로 실패한다.
  장애인 모델이 타임아웃까지 기다리게 하면 모든 요청이 그만큼 느려진다.

[해결]
  provider 에 "gemini>claude" 처럼 ">" 로 모델을 이어 쓰면 왼쪽부터 차례로 시도한다.
    - 앞 모델이 예외를 내면 다음 모델로 넘어간다 (폴백).
    - 앞 모델이 연속 MODEL_BREAKER_THRESHOLD(기본 3)번 실패하면 MODEL_BREAKER_COOLDOWN_SEC(기본 30)초 동안
      그 모델을 부르지 않고 바로 다음 모델로 보낸다 (서킷 브레이커 "열림").
    - 쉬는 시간이 지나면 한 번만 다시 시도한다. 성공하면 "닫힘"(정상), 실패하면 다시 쉰다.
    - 맨 마지막 모델은 건너뛰지 않는다. 마지막 모델까지 실패하면 그 예외를 그대로 낸다.
  provider 에 ">" 가 없으면 이전과 똑같이 동작한다(agent_graph.get_model 참고).

[장애 주입 (시험용)]
  실제 장애를 일부러 만들 수 없으니, MODEL_FAULT_INJECTION=true 일 때만 MODEL_FORCE_DOWN(모델 이름, 쉼표로 여러 개)에
  적힌 모델을 호출 전에 실패시킨다. MODEL_DOWN_DELAY_SEC(기본 0) 만큼 기다린 뒤 실패해 "타임아웃까지 기다리는 장애"를
  흉내 낼 수 있다. 환경변수는 호출할 때마다 읽으므로 실행 중에 바꿔도 반영된다.

[주의]
  - 브레이커 상태와 통계는 프로세스 안에만 있다. 워커가 여러 대이면 대마다 따로 센다.
  - 어떤 예외든 실패로 센다. 요청 내용이 잘못돼 생긴 오류도 같이 센다.
  - 대화 중간에 모델이 바뀌면 같은 대화를 다른 모델이 이어받는다(도구 호출 기록 형식은 두 모델 모두 LangChain 공통 형식).
"""
import os
import threading
import time
from collections import Counter

from app.services.metrics import MODEL_FALLBACKS, MODEL_SERVED


class ModelDownError(RuntimeError):
    """장애 주입으로 일부러 낸 오류."""


def _num(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def threshold() -> int:
    return int(_num("MODEL_BREAKER_THRESHOLD", 3))


def cooldown() -> float:
    return _num("MODEL_BREAKER_COOLDOWN_SEC", 30.0)


def forced_down(name: str) -> bool:
    if os.getenv("MODEL_FAULT_INJECTION", "false").lower() != "true":
        return False
    return name in [x.strip() for x in os.getenv("MODEL_FORCE_DOWN", "").split(",") if x.strip()]


_lock = threading.Lock()
_state: dict[str, dict] = {}   # 모델 이름 -> {"failures": 연속 실패 수, "opened_at": 열린 시각}
STATS: Counter = Counter()     # served:모델, error:모델, skipped:모델 (모델 호출 단위)


def reset() -> None:
    with _lock:
        _state.clear()
        STATS.clear()


def breaker_allow(name: str) -> bool:
    with _lock:
        s = _state.setdefault(name, {"failures": 0, "opened_at": 0.0})
        if s["failures"] < threshold():
            return True
        now = time.monotonic()
        if now - s["opened_at"] >= cooldown():
            s["opened_at"] = now   # 시험 호출 한 건만 통과시키고 다시 쉰다. 성공하면 breaker_success 가 닫는다
            return True
        return False


def breaker_success(name: str) -> None:
    with _lock:
        _state.setdefault(name, {"failures": 0, "opened_at": 0.0})["failures"] = 0


def breaker_failure(name: str) -> None:
    with _lock:
        s = _state.setdefault(name, {"failures": 0, "opened_at": 0.0})
        s["failures"] += 1
        if s["failures"] >= threshold():
            s["opened_at"] = time.monotonic()


def breaker_state(name: str) -> str:
    with _lock:
        s = _state.get(name)
        return "open" if s and s["failures"] >= threshold() else "closed"


class FallbackChatModel:
    """agent_node 가 쓰는 두 메서드(bind_tools, invoke)만 가진 모델. names 와 models 는 같은 순서다."""

    def __init__(self, names: list[str], models: list):
        self.names = names
        self.models = models

    def bind_tools(self, tools):
        return FallbackChatModel(self.names, [m.bind_tools(tools) for m in self.models])

    def invoke(self, messages):
        last_error: Exception | None = None
        for i, (name, model) in enumerate(zip(self.names, self.models)):
            is_last = i == len(self.models) - 1
            if not is_last and not breaker_allow(name):
                STATS[f"skipped:{name}"] += 1
                MODEL_FALLBACKS.labels(name, "breaker_open").inc()
                continue
            try:
                if forced_down(name):
                    time.sleep(_num("MODEL_DOWN_DELAY_SEC", 0.0))
                    raise ModelDownError(f"{name} 장애(주입)")
                response = model.invoke(messages)
            except Exception as e:
                last_error = e
                breaker_failure(name)
                STATS[f"error:{name}"] += 1
                if not is_last:
                    MODEL_FALLBACKS.labels(name, "error").inc()
                    print(f"[fallback] {name} 실패({type(e).__name__}: {str(e)[:80]}) → {self.names[i + 1]} 로 넘어간다")
                continue
            breaker_success(name)
            STATS[f"served:{name}"] += 1
            MODEL_SERVED.labels(name).inc()
            return response
        raise last_error  # 마지막 모델까지 실패
