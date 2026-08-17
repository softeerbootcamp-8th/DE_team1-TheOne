"""HVFHV+taxi_id 데이터의 ``taxi_id`` 계약을 적용한 정제기."""

from typing import Optional

from pyspark.sql import DataFrame

from jobs.driver_assignment.source_transformer import (
    MIN_OD_OBSERVATIONS,
    NATURAL_KEY_COLLISION_RATIO_LIMIT,
    PREMIUM_FARE_RATIO,
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer as SourceHVFHVCleanTransformer,
)
from schema.silver.hvfhv import FINAL_SCHEMA, REQUIRED_COLUMNS


class HVFHVCleanTransformer(SourceHVFHVCleanTransformer):
    def __init__(
        self,
        df_zone: Optional[DataFrame] = None,
        zone_lookup_path: Optional[str] = None,
        error_threshold: float = 0.05,
    ):
        super().__init__(
            df_zone=df_zone,
            zone_lookup_path=zone_lookup_path,
            error_threshold=error_threshold,
            require_taxi_id=True,
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
