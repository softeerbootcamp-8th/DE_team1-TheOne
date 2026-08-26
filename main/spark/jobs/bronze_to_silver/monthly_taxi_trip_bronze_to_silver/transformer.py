"""원천 API의 월별 택시 운행을 Silver 계약으로 정제합니다."""

import logging
from dataclasses import asdict, dataclass

from pipeline_core.transformer import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, date_format, lit, trim

from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver import (
    quality,
)

from schema.silver.monthly_taxi_trip import (
    FINAL_SCHEMA,
    REQUIRED_COLUMNS,
    REQUIRED_NON_NULL_COLUMNS,
)


logger = logging.getLogger(__name__)


_SILVER_TYPES = {field.name: field.dataType for field in FINAL_SCHEMA}
_STRING_COLUMNS = (
    "taxi_id",
    "hvfhs_license_num",
    "pickup_zone",
    "dropoff_zone",
    "estimated_service_tier",
)


@dataclass(frozen=True)
class ReconCounts:
    """변환이 몇 건을 받아 몇 건을 남겼는지, 무엇 때문에 걸렀는지.

    Spark GX 가 전체 레코드를 판정한 결과 그대로다 — Airflow 로 넘어가는 GX 결과의
    유일한 통로다(#1120). 예전에는 같은 값을 `_GX_VALIDATION.json` 으로 한 번 더
    내보냈는데, 대조 상대 없이 로그만 찍는 쪽이라 걷어냈다.

    `invalid` 는 사유별 합이 아니라 `total - valid` 다. 한 행이 여러 사유에 걸릴 수
    있어 사유별 건수를 더하면 중복 집계된다 — 사유별 값은 원인을 좁히는 참고용이고
    보존식에 쓰는 건 `invalid` 뿐이다.
    """

    total: int
    valid: int
    invalid: int
    missing_or_type_mismatch: int
    invalid_value: int
    invalid_service_tier: int
    extra_columns: tuple[str, ...]
    invalid_ratio: float
    warning: bool
    warning_threshold: float
    error_threshold: float
    data_docs_path: str | None

    def as_payload(self) -> dict:
        return asdict(self)


class MonthlyTaxiTripCleanTransformer(Transformer):
    """타입·필수값·운행 등급을 검증하고 원천 등급을 그대로 전달합니다."""

    def __init__(
        self,
        error_threshold: float = 0.05,
        warning_threshold: float = 0.01,
        gx_data_docs_location: str | None = None,

    ):
        self._error_threshold = error_threshold
        self._warning_threshold = warning_threshold
        self._gx_data_docs_location = gx_data_docs_location
        # `transform()` 이 센 값을 Loader 가 sidecar 로 내보낸다. 로그로만 흘리면
        # Airflow 가 Bronze·Silver 행 수와 맞대볼 수 없다.
        self.recon: ReconCounts | None = None

    def transform(self, df: DataFrame) -> DataFrame:
        missing = [name for name in REQUIRED_COLUMNS if name not in df.columns]
        if missing:
            raise ValueError(f"원천 데이터에 필수 컬럼이 누락되었습니다: {missing}")

        extra_columns = tuple(sorted(set(df.columns) - set(REQUIRED_COLUMNS)))
        typed = df.select(
            *(
                col(name).cast(_SILVER_TYPES[name]).alias(name)
                for name in REQUIRED_COLUMNS
            ),
            *(col(name) for name in extra_columns),
        )
        present = lit(True)
        for name in REQUIRED_NON_NULL_COLUMNS:
            present &= col(name).isNotNull()
        for name in _STRING_COLUMNS:
            present &= trim(col(name)) != ""

        valid_time = col("pickup_datetime") < col("dropoff_datetime")
        valid_range = (
            (col("trip_miles") > 0.0)
            & (col("trip_miles") <= 1000.0)
            & col("trip_time").between(1, 86400)
            & col("driver_pay").between(0.0, 5000.0)
            & col("tips").between(0.0, 5000.0)
        )
        valid_service_tier = (
            (col("hvfhs_license_num") == "HV0003")
            & col("estimated_service_tier").isin("Standard", "Comfort")
        ) | (
            (col("hvfhs_license_num") == "HV0005")
            & col("estimated_service_tier").isin("Standard", "Extra Comfort")
        )
        missing_or_type_valid = coalesce(present, lit(False))
        value_valid = coalesce(valid_time & valid_range, lit(False))
        service_tier_valid = coalesce(valid_service_tier, lit(False))
        valid = missing_or_type_valid & value_valid & service_tier_valid
        candidates = (
            typed.withColumn(
                quality.MISSING_OR_TYPE_VALID_COLUMN,
                missing_or_type_valid,
            )
            .withColumn(quality.VALUE_VALID_COLUMN, value_valid)
            .withColumn(
                quality.SERVICE_TIER_VALID_COLUMN,
                service_tier_valid,
            )
            .withColumn(quality.RECORD_VALID_COLUMN, valid)
        )
        counts = quality.validate_monthly_taxi_trip_records(
            candidates,
            warning_threshold=self._warning_threshold,
            error_threshold=self._error_threshold,
            data_docs_location=self._gx_data_docs_location,
        )

        self.recon = ReconCounts(
            total=counts.total,
            valid=counts.valid,
            invalid=counts.invalid,
            missing_or_type_mismatch=counts.missing_or_type_mismatch,
            invalid_value=counts.invalid_value,
            invalid_service_tier=counts.invalid_service_tier,
            extra_columns=counts.extra_columns,
            invalid_ratio=counts.invalid_ratio,
            warning=counts.warning,
            warning_threshold=counts.warning_threshold,
            error_threshold=counts.error_threshold,
            data_docs_path=self._gx_data_docs_location,
        )
        if counts.invalid:
            logger.warning(
                "불합격 사유별 행 수: NULL/타입=%d 값 범위=%d 등급=%d",
                counts.missing_or_type_mismatch,
                counts.invalid_value,
                counts.invalid_service_tier,
            )
        if counts.extra_columns:
            logger.warning("원천 추가 컬럼: %s", ",".join(counts.extra_columns))

        transformed = candidates.filter(col(quality.RECORD_VALID_COLUMN)).withColumn(
            "year_month", date_format(col("pickup_datetime"), "yyyy-MM")
        )
        return transformed.select(
            *(
                col(field.name).cast(_SILVER_TYPES[field.name]).alias(field.name)
                for field in FINAL_SCHEMA
            )
        )


__all__ = [
    "FINAL_SCHEMA",
    "REQUIRED_COLUMNS",
    "REQUIRED_NON_NULL_COLUMNS",
    "MonthlyTaxiTripCleanTransformer",
]
