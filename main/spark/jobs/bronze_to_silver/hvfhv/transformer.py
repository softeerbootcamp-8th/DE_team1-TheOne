"""원천 API의 월별 택시 운행을 Silver 계약으로 정제합니다."""

import logging

import pyarrow as pa
from pipeline_core.transformer import Transformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, date_format, lit, trim, when

from schema.silver import CLEAN_MONTHLY_TAXI_TRIP_SCHEMA as FINAL_SCHEMA


logger = logging.getLogger(__name__)


def _spark_type(data_type: pa.DataType) -> str:
    if pa.types.is_string(data_type):
        return "string"
    if pa.types.is_timestamp(data_type):
        return "timestamp"
    if pa.types.is_int32(data_type):
        return "int"
    if pa.types.is_int64(data_type):
        return "bigint"
    if pa.types.is_float64(data_type):
        return "double"
    raise TypeError(f"지원하지 않는 Silver 타입입니다: {data_type}")


REQUIRED_COLUMNS = list(FINAL_SCHEMA.names)
_SILVER_TYPES = {field.name: _spark_type(field.type) for field in FINAL_SCHEMA}
_STRING_COLUMNS = (
    "taxi_id",
    "hvfhs_license_num",
    "pickup_zone",
    "dropoff_zone",
    "estimated_service_tier",
)


class HVFHVCleanTransformer(Transformer):
    """타입·필수값·운행 등급을 검증하고 원천 등급을 그대로 전달합니다."""

    def __init__(self, error_threshold: float = 0.05):
        self._error_threshold = error_threshold

    def transform(self, df: DataFrame) -> DataFrame:
        missing = [name for name in REQUIRED_COLUMNS if name not in df.columns]
        if missing:
            raise ValueError(f"원천 데이터에 필수 컬럼이 누락되었습니다: {missing}")

        typed = df.select(
            *(
                col(name).cast(_SILVER_TYPES[name]).alias(name)
                for name in REQUIRED_COLUMNS
            )
        )
        present = lit(True)
        for name in REQUIRED_COLUMNS:
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
        valid = present & valid_time & valid_range & valid_service_tier

        stats = typed.select(
            count(lit(1)).alias("total_count"),
            count(when(valid, 1)).alias("valid_count"),
            count(when(~present, 1)).alias("missing_or_type_mismatch"),
            count(when(~(valid_time & valid_range), 1)).alias("invalid_value"),
            count(when(~valid_service_tier, 1)).alias("invalid_service_tier"),
        ).first()
        total_count = int(stats["total_count"] or 0)
        valid_count = int(stats["valid_count"] or 0)
        invalid_count = total_count - valid_count

        if invalid_count:
            logger.warning(
                "불합격 사유별 행 수: NULL/타입=%d 값 범위=%d 등급=%d",
                stats["missing_or_type_mismatch"],
                stats["invalid_value"],
                stats["invalid_service_tier"],
            )
        if total_count and invalid_count / total_count >= self._error_threshold:
            ratio = invalid_count / total_count
            raise ValueError(
                f"불합격 비율이 {ratio:.2%}로 임계치"
                f"({self._error_threshold:.2%}) 이상입니다"
            )

        transformed = typed.filter(valid).withColumn(
            "year_month", date_format(col("pickup_datetime"), "yyyy-MM")
        )
        return transformed.select(
            *(
                col(field.name).cast(_SILVER_TYPES[field.name]).alias(field.name)
                for field in FINAL_SCHEMA
            ),
            col("year_month"),
        )


__all__ = ["FINAL_SCHEMA", "REQUIRED_COLUMNS", "HVFHVCleanTransformer"]
