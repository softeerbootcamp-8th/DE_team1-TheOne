"""합성 데이터 생성 설정의 단일 소스.

값의 소유자는 이 파일이 아니라 `config/generation.json` 입니다. 그래서 여기에는
기본값이 **하나도 없습니다** — 두는 순간 파일과 코드 두 곳에 값이 생기고, 그게 이
통합이 없애려던 상태입니다. 모든 필드가 필수이고 누락은 예외입니다.

이 파일에 오지 않는 값이 두 종류 있습니다 (분류 근거는 `docs/config_inventory.md`).

  - **실측 상수**: 요일·시간대 비중, 거리 tertile(1.93/4.75mi). `analysis.md` 재실행
    산출물이라 손으로 조정할 값이 아닙니다. `driver_master/traits.py` 가 소유합니다.
  - **가정 파라미터**: 분포 모수(beta·gamma), `GROUP_COUNTS`, 각종 범위 상수.
    바꿔가며 돌려볼 값이지만 이번 회차 범위 밖입니다. 각 모듈의 **이름 붙은 상수
    한 곳**이 소유하고, 함수 시그니처 기본값으로는 두지 않습니다 — 시그니처에 두면
    소유자가 몇 곳인지 세는 것부터 어려워집니다(통합 전 14곳이었습니다).
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, fields
from datetime import date
from pathlib import Path

# 실행 위치가 아니라 이 파일 위치로 저장소 루트를 확정합니다. Spark 작업은 main/spark
# 에서 실행하므로 상대경로를 쓰면 main/config 를 보게 됩니다 (generate.py 와 같은 규칙).
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "generation.json"

SCORE_WEIGHT_NAMES = ("time", "distance", "airport", "manhattan", "tier")
# 합이 1.0 이어야 preference_score 가 0~1 을 유지합니다 (candidates.py). 부동소수라
# `== 1.0` 으로 보면 0.2975+0.2550+0.1700+0.1275+0.1500 같은 정당한 조합도 떨어집니다.
WEIGHT_SUM_TOLERANCE = 1e-9


class ConfigError(ValueError):
    """설정이 계약을 지키지 않을 때. 조용한 fallback 없이 즉시 실패합니다."""


def _int(value: object, where: str, *, minimum: int) -> int:
    # bool 은 int 의 하위 타입이라 isinstance 만으로는 `true` 가 1 로 통과합니다.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{where}: 정수여야 합니다 (받은 값: {value!r})")
    if value < minimum:
        raise ConfigError(f"{where}: {minimum} 이상이어야 합니다 (받은 값: {value})")
    return value


def _ratio(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}: 0~1 실수여야 합니다 (받은 값: {value!r})")
    if not 0.0 <= float(value) <= 1.0:
        raise ConfigError(f"{where}: 0 이상 1 이하여야 합니다 (받은 값: {value})")
    return float(value)


def _month_start(value: object, where: str) -> date:
    # 이미 `date` 인 경우를 받는 이유: `dataclasses.replace` 로 CLI 오버라이드를 얹으면
    # __post_init__ 이 다시 돌면서 한 번 변환해 둔 값을 또 봅니다. 문자열만 받으면
    # 오버라이드가 "날짜 형식이 아니다" 로 죽습니다.
    if isinstance(value, date):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ConfigError(f"{where}: 날짜 형식이 아닙니다 (받은 값: {value!r})") from exc
    else:
        raise ConfigError(f"{where}: 'YYYY-MM-DD' 문자열이어야 합니다 (받은 값: {value!r})")
    # 월별 상태는 매월 1일 스냅샷에서만 한 달씩 전진합니다
    # (`monthly.prepare_monthly_state`). 여기서 막지 않으면 부트스트랩은 성공하고
    # 다음 달 실행이 실패합니다 — 실패 지점이 원인에서 한 달 떨어집니다.
    if parsed.day != 1:
        raise ConfigError(f"{where}: 매월 1일이어야 합니다 (받은 값: {value})")
    return parsed


def _build(cls: type, data: object, where: str):
    """알 수 없는 키·누락 키를 먼저 잡고 dataclass 를 만듭니다."""
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: 객체여야 합니다 (받은 값: {type(data).__name__})")
    allowed = {field.name for field in fields(cls)}
    if unknown := sorted(set(data) - allowed):
        raise ConfigError(f"{where}: 알 수 없는 키 {unknown} (허용: {sorted(allowed)})")
    if missing := sorted(allowed - set(data)):
        raise ConfigError(f"{where}: 필수 키 누락 {missing}")
    return cls(**data)


@dataclass(frozen=True)
class DriverConfig:
    """기사 생애주기 비율.

    `join_rate`·`exit_rate`·`vehicle_change_rate` 는 아직 **소비되지 않습니다**.
    현재 생성기는 유입=유출이 정의상 같은 `change_rate` 하나로만 돕니다
    (`snapshot.evolve_company_snapshot`). 세 값을 실제로 읽는 것은 후속 lifecycle
    작업입니다 — 검증만 되고 결과에 영향이 없다는 사실을 `config/README.md` 와
    `docs/config_inventory.md` 에 표기해 둡니다.
    """

    initial_count: int
    join_rate: float
    exit_rate: float
    vehicle_change_rate: float

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "initial_count", _int(self.initial_count, "driver.initial_count", minimum=1)
        )
        for name in ("join_rate", "exit_rate", "vehicle_change_rate"):
            object.__setattr__(self, name, _ratio(getattr(self, name), f"driver.{name}"))


@dataclass(frozen=True)
class BootstrapConfig:
    snapshot_date: date
    sample_per_month: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "snapshot_date", _month_start(self.snapshot_date, "bootstrap.snapshot_date")
        )
        object.__setattr__(
            self,
            "sample_per_month",
            _int(self.sample_per_month, "bootstrap.sample_per_month", minimum=1),
        )


@dataclass(frozen=True)
class AllocationConfig:
    score_weights: dict[str, float]
    bucket_size: int

    def __post_init__(self) -> None:
        weights = self.score_weights
        if not isinstance(weights, dict):
            raise ConfigError(
                f"allocation.score_weights: 객체여야 합니다 (받은 값: {type(weights).__name__})"
            )
        unknown = sorted(set(weights) - set(SCORE_WEIGHT_NAMES))
        missing = sorted(set(SCORE_WEIGHT_NAMES) - set(weights))
        if unknown or missing:
            raise ConfigError(
                f"allocation.score_weights: 키가 계약과 다릅니다 "
                f"(알 수 없음={unknown}, 누락={missing}, 허용={list(SCORE_WEIGHT_NAMES)})"
            )
        cleaned = {
            name: _ratio(weights[name], f"allocation.score_weights.{name}")
            for name in SCORE_WEIGHT_NAMES
        }
        total = math.fsum(cleaned.values())
        if not math.isclose(total, 1.0, abs_tol=WEIGHT_SUM_TOLERANCE):
            raise ConfigError(
                f"allocation.score_weights: 합이 1.0 이어야 합니다 (받은 합: {total!r})"
            )
        object.__setattr__(self, "score_weights", cleaned)
        object.__setattr__(
            self, "bucket_size", _int(self.bucket_size, "allocation.bucket_size", minimum=1)
        )


@dataclass(frozen=True)
class GenerationConfig:
    """`target_month` 는 여기 없습니다.

    대상 월은 설정이 아니라 실행 인자입니다 — TLC 공개 지연 때문에 런타임에
    발견되고(`resolve_source_year_month`), 설정 안에 넣으면 같은 설정으로 다른 달을
    돌릴 수 없게 됩니다(config_hash 가 달라짐). `RunContext.create` 가 받습니다.
    """

    global_seed: int
    driver: DriverConfig
    bootstrap: BootstrapConfig
    allocation: AllocationConfig

    def __post_init__(self) -> None:
        object.__setattr__(self, "global_seed", _int(self.global_seed, "global_seed", minimum=0))


_NESTED = {"driver": DriverConfig, "bootstrap": BootstrapConfig, "allocation": AllocationConfig}


def build_config(raw: object) -> GenerationConfig:
    """이미 읽어 둔 매핑으로 설정을 만듭니다. 파일을 읽지 않습니다."""
    if not isinstance(raw, dict):
        raise ConfigError(f"설정 최상위: 객체여야 합니다 (받은 값: {type(raw).__name__})")
    data = dict(raw)
    for key, cls in _NESTED.items():
        if key in data:
            data[key] = _build(cls, data[key], key)
    return _build(GenerationConfig, data, "설정 최상위")


def load_config(path: str | Path | None = None) -> GenerationConfig:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.is_file():
        raise ConfigError(f"설정 파일이 없습니다: {config_path}")
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"설정 파일이 올바른 JSON 이 아닙니다: {config_path} ({exc})") from exc
    return build_config(raw)


def _json_default(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    # config_hash 의 입력이라 조용히 str() 로 넘기면 안 됩니다 — 서로 다른 값이 같은
    # 문자열이 되면 설정을 바꿔도 run_id 가 그대로일 수 있습니다.
    raise TypeError(f"설정에 직렬화할 수 없는 값이 있습니다: {value!r}")


def canonical_json(config: GenerationConfig) -> str:
    """정렬 직렬화. `config_hash` 의 입력이라 키 순서가 안정해야 합니다."""
    return json.dumps(
        asdict(config), sort_keys=True, separators=(",", ":"), default=_json_default
    )
