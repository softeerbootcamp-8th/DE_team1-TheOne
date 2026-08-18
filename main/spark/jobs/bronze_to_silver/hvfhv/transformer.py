"""HVFHV+taxi_id 데이터의 타입·품질 계약을 적용한 정제기."""

import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, when

from shared.spark.hvfhv_clean_transformer import (
    MIN_OD_OBSERVATIONS,
    NATURAL_KEY_COLLISION_RATIO_LIMIT,
    PREMIUM_FARE_RATIO,
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer as SourceHVFHVCleanTransformer,
)
from schema.silver.hvfhv import FINAL_SCHEMA, REQUIRED_COLUMNS


logger = logging.getLogger(__name__)

_SILVER_TYPES = {field.name: field.dataType for field in FINAL_SCHEMA}
_RANGE_BOUNDS = {
    "trip_miles": (0, 1000, False),
    "trip_time": (0, 86400, False),
    "base_passenger_fare": (0, 5000, True),
    "driver_pay": (0, 5000, True),
}


def _reject_reason_counts(df: DataFrame, casts: dict) -> dict[str, int]:
    type_mismatch = lit(False)
    missing_value = lit(False)
    condition = lit(False)
    for name, casted in casts.items():
        type_mismatch |= col(name).isNotNull() & casted.isNull()
    for name in REQUIRED_COLUMNS:
        if name in df.columns:
            missing_value |= col(name).isNull()
    for name, (low, high, inclusive_low) in _RANGE_BOUNDS.items():
        if name not in casts:
            continue
        value = casts[name]
        too_low = value < low if inclusive_low else value <= low
        condition |= value.isNotNull() & (too_low | (value > high))
    row = df.select(
        count(when(type_mismatch, 1)).alias("type_mismatch"),
        count(when(condition, 1)).alias("out_of_range"),
        count(when(missing_value, 1)).alias("missing_value"),
    ).first()
    return {
        key: int(row[key] or 0)
        for key in ("type_mismatch", "out_of_range", "missing_value")
    }


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

    def transform(self, df: DataFrame) -> DataFrame:
        if df.isEmpty():
            return super().transform(df)

        casts = {
            name: col(name).cast(_SILVER_TYPES[name])
            for name in REQUIRED_COLUMNS
            if name in df.columns and name in _SILVER_TYPES
        }
        reasons = _reject_reason_counts(df, casts)
        log = logger.warning if any(reasons.values()) else logger.info
        log(
            "불합격 사유별 행 수: 타입 불일치=%(type_mismatch)s "
            "범위 이탈=%(out_of_range)s NULL=%(missing_value)s",
            reasons,
        )
        typed = df.select(
            *(casts.get(name, col(name)).alias(name) for name in df.columns)
        )
        return super().transform(typed)


__all__ = [
    "FINAL_SCHEMA",
    "MIN_OD_OBSERVATIONS",
    "NATURAL_KEY_COLLISION_RATIO_LIMIT",
    "PREMIUM_FARE_RATIO",
    "REQUIRED_COLUMNS",
    "TRIP_KEY_COLUMNS",
    "HVFHVCleanTransformer",
]
