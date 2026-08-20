"""단계별 파생 시드 (blue_print.md D9).

전역 시드 하나를 모든 난수 지점이 공유하면, 한 단계의 로직만 고쳐도 뒤에 실행되는
단계의 난수 시퀀스가 밀려 무관한 산출물이 전부 바뀝니다. 단계마다 시드를 파생하면
그 결합이 끊어집니다.

`month_key` 를 넣을지 말지가 이 모듈의 유일한 판단입니다.

  - 기사에게 귀속되어 시간에 대해 안정적이어야 하는 값 → 넣지 않는다
  - 그 달에만 해당하는 사건·샘플링 → 넣는다

인자 이름이 `target_month` 가 아니라 `month_key` 인 이유: D8 의 부트스트랩 풀은
`target_month` 가 아니라 기사의 가입 시점(`traits_pool_month`)을 넣습니다. 어떤 월을
넣는지는 호출하는 stage 가 정하고, 이 함수는 그것을 모릅니다.
"""

from __future__ import annotations

import hashlib
from enum import Enum

# 63비트 마스킹. Spark `lit(seed)` 에 2**63 이상을 넘기면 LongType 이 아니라
# DecimalType 으로 승격되고, 그러면 `xxhash64` 가 같은 시드에 다른 값을 냅니다
# (버킷 분할이 조용히 달라짐). numpy 는 큰 정수를 받지만 소비 지점마다 폭이
# 다르면 추적이 어려워서 파생 함수 한 곳에서 통일합니다.
SEED_BITS = 63
SEED_MASK = (1 << SEED_BITS) - 1


class Stage(str, Enum):
    """난수 소비 단계. 문자열 리터럴 직접 사용 금지 — 오타는 조용히 다른 시드를 씁니다."""

    # 초기 픽스처는 특정 월의 산물이 아닙니다.
    SNAPSHOT_INIT = "snapshot_init"
    # 그 달의 리스 종료·신규 계약.
    SNAPSHOT_EVOLVE = "snapshot_evolve"
    # customer_id / taxi_id / lease_id. 기사에 영구 귀속되므로 월을 넣지 않습니다.
    ENTITY_ID = "entity_id"
    # 기사 고유 기준값 (D7 의 A층). 월을 넣으면 8월과 9월의 성향이 달라집니다.
    DRIVER_TRAITS = "driver_traits"
    # 월별 실현값 (D7 의 B층). 그 달의 충격이므로 월을 넣습니다.
    MONTHLY_REALIZATION = "monthly_realization"
    # 전 기사 공통 계절 요인. 기사 단위가 아니라 월 단위 단일 draw 입니다.
    SEASONAL_FACTOR = "seasonal_factor"
    # 실측 트립 풀의 서브샘플.
    MONTHLY_SAMPLE = "monthly_sample"
    # 배정용 선호(per-driver sha256 파생의 입력).
    DRIVER_PROFILE = "driver_profile"
    # 차량 배정의 합리성 추첨 (D6).
    VEHICLE_ASSIGNMENT = "vehicle_assignment"
    # lifecycle join / exit / vehicle_change 추첨.
    LIFECYCLE = "lifecycle"
    # 운행을 버킷에 샤딩. tie-break 가 아니라 후보 집합을 정하는 값입니다.
    ALLOCATION_BUCKET = "allocation_bucket"


def derive_seed(global_seed: int, stage: Stage, month_key: str | None = None) -> int:
    """`(global_seed, stage, month_key)` 의 순수 함수. 63비트 부호 없는 정수."""
    if not isinstance(stage, Stage):
        raise TypeError(
            f"stage 는 Stage 열거형이어야 합니다 (받은 값: {stage!r}). "
            "문자열 리터럴을 넘기면 오타가 조용히 다른 시드를 씁니다."
        )
    material = f"{global_seed}:{stage.value}"
    if month_key:
        material += f":{month_key}"
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & SEED_MASK


def derive_entity_seed(stage_seed: int, *parts: object) -> int:
    """단계 시드 아래에서 엔티티(기사·차량) 단위로 한 번 더 파생합니다.

    단계 시드 하나를 루프 전체가 돌려쓰면 500번째 기사의 값이 앞 499명이 소비한
    draw 수에 의존합니다. 그러면 기사 한 명이 늘거나 줄 때 나머지 전원의 산출물이
    바뀌고, 단계별 파생으로 끊어 낸 결합이 기사 축에 그대로 남습니다.
    """
    material = ":".join([str(stage_seed), *(str(part) for part in parts)])
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & SEED_MASK


def _demo() -> None:
    """완료 조건 자체 점검. `python -m sub.seeds` 로 실행합니다."""
    s = 42
    # 1. 같은 입력 → 같은 값
    assert derive_seed(s, Stage.SNAPSHOT_INIT) == derive_seed(s, Stage.SNAPSHOT_INIT)
    # 2. stage 만 달라도 다른 값
    assert derive_seed(s, Stage.SNAPSHOT_INIT) != derive_seed(s, Stage.SNAPSHOT_EVOLVE)
    # 3. month=None 과 month 지정이 다른 값
    assert derive_seed(s, Stage.SNAPSHOT_EVOLVE) != derive_seed(s, Stage.SNAPSHOT_EVOLVE, "2026-01")
    # 4. month 만 달라도 다른 값
    assert derive_seed(s, Stage.SNAPSHOT_EVOLVE, "2026-01") != derive_seed(
        s, Stage.SNAPSHOT_EVOLVE, "2026-02"
    )
    # 5. 기사 고유 stage 는 월을 받지 않으므로 어느 달에서 계산해도 같습니다
    traits = derive_seed(s, Stage.DRIVER_TRAITS)
    assert derive_entity_seed(traits, "DRIVER_0001") == derive_entity_seed(traits, "DRIVER_0001")
    # 6. 63비트 상한
    for stage in Stage:
        assert 0 <= derive_seed(s, stage, "2026-01") <= SEED_MASK
    # 7. Enum 강제
    try:
        derive_seed(s, "snapshot_init")  # type: ignore[arg-type]
    except TypeError:
        pass
    else:
        raise AssertionError("문자열 stage 가 통과했습니다")
    print(f"sub.seeds 자체 점검 통과 (stage {len(Stage)}개)")


if __name__ == "__main__":
    _demo()
