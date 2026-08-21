"""실행 한 건의 계보. `run_id`·`config_hash` 로 manifest 에 실립니다."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

from sub.config import ConfigError, GenerationConfig, canonical_json

# 12자면 이 저장소가 다룰 설정 조합 규모에서 충돌이 실질적으로 없고, 경로·로그에
# 그대로 실어도 읽힙니다.
CONFIG_HASH_LENGTH = 12


def validate_target_month(value: object) -> str:
    """'YYYY-MM' 만 통과시킵니다.

    `strptime("%Y-%m")` 은 `2026-8` 처럼 0 없는 달도 받습니다. 그러면 같은 달이
    `2026-8` 과 `2026-08` 두 run_id 로 갈라지고, 파티션 경로도 둘이 됩니다. 왕복
    비교로 정확히 zero-padded 형식만 남깁니다.
    """
    if not isinstance(value, str):
        raise ConfigError(f"target_month: 'YYYY-MM' 문자열이어야 합니다 (받은 값: {value!r})")
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ConfigError(f"target_month: 'YYYY-MM' 형식이 아닙니다 (받은 값: {value!r})") from exc
    if parsed.strftime("%Y-%m") != value:
        raise ConfigError(f"target_month: 월을 두 자리로 적어야 합니다 (받은 값: {value!r})")
    return value


@dataclass(frozen=True)
class RunContext:
    target_month: str
    config: GenerationConfig
    config_hash: str
    run_id: str
    created_at: str

    @classmethod
    def create(
        cls,
        target_month: str,
        config: GenerationConfig,
        *,
        created_at: str | None = None,
    ) -> RunContext:
        month = validate_target_month(target_month)
        digest = hashlib.sha256(canonical_json(config).encode("utf-8")).hexdigest()
        config_hash = digest[:CONFIG_HASH_LENGTH]
        return cls(
            target_month=month,
            config=config,
            config_hash=config_hash,
            run_id=f"{month}_{config_hash}",
            # 계보 기록 전용입니다. `run_id`·`config_hash` 에는 들어가지 않습니다 —
            # 들어가면 같은 설정을 두 번 돌렸을 때 run_id 가 달라져서 멱등성 판정이
            # 매번 새 릴리스를 만듭니다.
            created_at=created_at or datetime.now(timezone.utc).isoformat(),
        )
