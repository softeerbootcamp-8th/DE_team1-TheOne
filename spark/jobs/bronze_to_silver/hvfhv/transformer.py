import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import col, count, date_format, lit, percentile_approx, sha2, struct, to_json, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

from pipeline_core.transformer import Transformer

logger = logging.getLogger(__name__)

FINAL_SCHEMA = StructType([
    StructField("trip_key", StringType(), False),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("platform_name", StringType(), False),
    StructField("estimated_service_tier", StringType(), False),
    StructField("taxi_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("taxi_model_id", StringType(), True),
    StructField("year_month", StringType(), True),
    StructField("pickup_borough", StringType(), True),
    StructField("pickup_zone", StringType(), True),
    StructField("pickup_service_zone", StringType(), True),
    StructField("dropoff_borough", StringType(), True),
    StructField("dropoff_zone", StringType(), True),
    StructField("dropoff_service_zone", StringType(), True)
])

REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "driver_pay",
    "hvfhs_license_num",
]

TRIP_KEY_COLUMNS = [
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "driver_pay",
]

PREMIUM_FARE_RATIO = 1.15
MIN_OD_OBSERVATIONS = 20


class HVFHVCleanTransformer(Transformer):
    """
    HVFHV 데이터의 클렌징, 변환, 파생 컬럼 추가를 수행하는 Transformer.
    """

    def __init__(
        self,
        df_zone: Optional[DataFrame] = None,
        zone_lookup_path: Optional[str] = None,
        error_threshold: float = 0.05,
    ):
        self._df_zone = df_zone
        self._zone_lookup_path = zone_lookup_path
        self._error_threshold = error_threshold

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info("데이터 정제 및 변환 시작...")
        if df.isEmpty():
            return df

        # =========================================================================
        # 1. 조기 검증 및 필터링
        # =========================================================================
        missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_required:
            error_msg = f"원천 데이터에 필수 컬럼이 누락되었습니다: {missing_required}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        valid_condition = (
            col("pickup_datetime").isNotNull() &
            col("dropoff_datetime").isNotNull() &
            col("PULocationID").isNotNull() &
            col("DOLocationID").isNotNull() &
            col("trip_miles").isNotNull() & (col("trip_miles") > 0) & (col("trip_miles") <= 1000) &
            col("trip_time").isNotNull() & (col("trip_time") > 0) & (col("trip_time") <= 86400) &
            col("base_passenger_fare").isNotNull() &
            (col("base_passenger_fare") >= 0) & (col("base_passenger_fare") <= 5000) &
            col("driver_pay").isNotNull() & (col("driver_pay") >= 0) & (col("driver_pay") <= 5000)
        )

        df_valid = df.filter(valid_condition)

        # total_count 및 valid_count 동시 집계 
        stats = df.select(
            count(lit(1)).alias("total_count"),
            count(when(valid_condition, 1)).alias("valid_count")
        ).first()

        total_count = stats["total_count"] if stats else 0
        valid_count = stats["valid_count"] if stats else 0
        invalid_count = total_count - valid_count

        logger.info(f"정상(Valid) 데이터: {valid_count:,} 건 / 불합격(Invalid): {invalid_count:,} 건")

        if total_count > 0:
            invalid_ratio = invalid_count / total_count
            if invalid_ratio >= self._error_threshold:
                error_msg = f"불합격 비율이 {invalid_ratio:.2%}로 임계치({self._error_threshold:.2%})를 초과했습니다."
                logger.error(error_msg)
                raise ValueError(error_msg)

        # =========================================================================
        # 2. 파생 컬럼 생성 및 조인
        # =========================================================================
        df_zone = self._df_zone
        if df_zone is None and self._zone_lookup_path:
            spark = df.sparkSession
            zone_path = Path(self._zone_lookup_path)
            if not zone_path.is_absolute():
                zone_path = Path(__file__).resolve().parents[4] / zone_path
            df_zone = spark.read.option("header", "true").csv(str(zone_path))

        # 2.1 파생 컬럼 추가
        canonical_trip = to_json(
            struct(*(col(name).alias(name) for name in TRIP_KEY_COLUMNS)),
            options={"ignoreNullFields": "false"},
        )
        df_transformed = df_valid.withColumn("trip_key", sha2(canonical_trip, 256)).withColumn(
            "platform_name",
            when(col("hvfhs_license_num") == "HV0002", "Juno")
            .when(col("hvfhs_license_num") == "HV0003", "Uber")
            .when(col("hvfhs_license_num") == "HV0004", "Via")
            .when(col("hvfhs_license_num") == "HV0005", "Lyft")
            .otherwise("Unknown")
        ).withColumn("taxi_id", lit(None).cast("string")) \
         .withColumn("driver_id", lit(None).cast("string")) \
         .withColumn("taxi_model_id", lit(None).cast("string")) \
         .withColumn("year_month", date_format(col("pickup_datetime"), "yyyy-MM"))

        duplicate_key = (
            df_transformed.groupBy("year_month", "trip_key")
            .count()
            .filter(col("count") > 1)
            .limit(1)
            .first()
        )
        if duplicate_key:
            raise ValueError(
                "대상 월에 trip_key 중복이 있습니다: "
                f"year_month={duplicate_key['year_month']}, trip_key={duplicate_key['trip_key']}"
            )

        # TLC 원천에는 실제 상품 등급이 없으므로 플랫폼·OD별 기본 운임으로만 추정한다.
        # 수요 할증 등 다른 원인도 포함될 수 있어 관측 등급이 아닌 estimated 값이다.
        od_window = Window.partitionBy("platform_name", "PULocationID", "DOLocationID")
        df_transformed = (
            df_transformed
            .withColumn("_od_observation_count", count(lit(1)).over(od_window))
            .withColumn(
                "_od_fare_median",
                percentile_approx("base_passenger_fare", 0.5).over(od_window),
            )
        )
        premium_fare = (
            (col("_od_observation_count") >= MIN_OD_OBSERVATIONS)
            & (col("base_passenger_fare") >= col("_od_fare_median") * PREMIUM_FARE_RATIO)
        )
        df_transformed = df_transformed.withColumn(
            "estimated_service_tier",
            when(premium_fare & (col("platform_name") == "Uber"), "Comfort")
            .when(premium_fare & (col("platform_name") == "Lyft"), "Extra Comfort")
            .otherwise("Standard"),
        ).drop("_od_observation_count", "_od_fare_median")

        # 2.2 Taxi Zone Join (Pickup & Dropoff)
        if df_zone is not None:
            df_zone_pu = df_zone.select(
                col("LocationID").alias("PU_LocationID_join"),
                col("Borough").alias("pickup_borough"),
                col("Zone").alias("pickup_zone"),
                col("service_zone").alias("pickup_service_zone")
            )
            df_zone_do = df_zone.select(
                col("LocationID").alias("DO_LocationID_join"),
                col("Borough").alias("dropoff_borough"),
                col("Zone").alias("dropoff_zone"),
                col("service_zone").alias("dropoff_service_zone")
            )

            df_transformed = df_transformed.join(
                df_zone_pu, df_transformed.PULocationID == df_zone_pu.PU_LocationID_join, "left"
            ).drop("PU_LocationID_join")

            df_transformed = df_transformed.join(
                df_zone_do, df_transformed.DOLocationID == df_zone_do.DO_LocationID_join, "left"
            ).drop("DO_LocationID_join")
        else:
            for col_name in ["pickup_borough", "pickup_zone", "pickup_service_zone",
                            "dropoff_borough", "dropoff_zone", "dropoff_service_zone"]:
                if col_name not in df_transformed.columns:
                    df_transformed = df_transformed.withColumn(col_name, lit(None).cast("string"))

        # =========================================================================
        # 3. 최종 스키마 맞춤 및 패딩
        # =========================================================================
        # 3.1 FINAL_SCHEMA 필수 필드 안전 패딩
        for field in FINAL_SCHEMA:
            if field.name not in df_transformed.columns:
                df_transformed = df_transformed.withColumn(field.name, lit(None).cast(field.dataType))

        # 3.2 금액 관련 선택 필드 결측치 처리 (Null -> 0.0)
        fare_cols = [
            "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "tips"
        ]
        df_transformed = df_transformed.fillna(0.0, subset=fare_cols)

        # 3.3 FINAL_SCHEMA에 명시된 컬럼 화이트리스트 선택 및 캐스팅
        select_exprs: list[Column] = [col(field.name).cast(field.dataType).alias(field.name) for field in FINAL_SCHEMA]
        return df_transformed.select(*select_exprs)
