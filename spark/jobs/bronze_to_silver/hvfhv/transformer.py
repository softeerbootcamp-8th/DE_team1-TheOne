"""기존 HVFHV Silver DAG를 위한 전환기 호환 import.

정제 구현은 가짜 원천 생성 영역이 소유합니다. API 전환 완료 후 이 모듈과 기존 DAG를
함께 제거합니다.
"""

from jobs.driver_assignment.source_transformer import (
    FINAL_SCHEMA,
    MIN_OD_OBSERVATIONS,
    NATURAL_KEY_COLLISION_RATIO_LIMIT,
    PREMIUM_FARE_RATIO,
    REQUIRED_COLUMNS,
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer,
)

__all__ = [
    "FINAL_SCHEMA",
    "MIN_OD_OBSERVATIONS",
    "NATURAL_KEY_COLLISION_RATIO_LIMIT",
    "PREMIUM_FARE_RATIO",
    "REQUIRED_COLUMNS",
    "TRIP_KEY_COLUMNS",
    "HVFHVCleanTransformer",
]
