import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, lit, when, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

from pipeline_core.transformer import Transformer

logger = logging.getLogger(__name__)

FINAL_SCHEMA = StructType([
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


class HVFHVCleanTransformer(Transformer):
    """
    HVFHV 데이터의 클렌징, 변환, 파생 컬럼 추가를 수행하는 Transformer.
    """

    def __init__(
        self,
        df_zone: Optional[DataFrame] = None,
        zone_lookup_path: Optional[str] = None,
        error_threshold: float = 0.2,
    ):
        self._df_zone = df_zone
        self._zone_lookup_path = zone_lookup_path
        self._error_threshold = error_threshold

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info("데이터 정제 및 변환 시작...")
        total_count = df.count()
        if total_count == 0:
            return df

        # df_zone 지연 로딩
        df_zone = self._df_zone
        if df_zone is None and self._zone_lookup_path:
            spark = df.sparkSession
            zone_path = Path(self._zone_lookup_path)
            if not zone_path.is_absolute():
                zone_path = Path(__file__).resolve().parents[4] / zone_path
            df_zone = spark.read.option("header", "true").csv(str(zone_path))

        # 1. 파생 컬럼 추가
        df_transformed = df.withColumn(
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

        # 1.1 Taxi Zone Join (Pickup & Dropoff)
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

        # 1.2 FINAL_SCHEMA 필수 필드 안전 패딩 (원천 데이터 누락 필드는 Null로 채움)
        for field in FINAL_SCHEMA:
            if field.name not in df_transformed.columns:
                df_transformed = df_transformed.withColumn(field.name, lit(None).cast(field.dataType))

        # 1.3 driver_pay 결측치(Null) 제거
        df_transformed = df_transformed.dropna(subset=["driver_pay"])

        # 1.4 금액 관련 필드 결측치 처리 (Null -> 0.0)
        fare_cols = [
            "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "tips"
        ]
        df_transformed = df_transformed.fillna(0.0, subset=fare_cols)

        # 1.5 FINAL_SCHEMA에 명시된 컬럼만 화이트리스트 선택 (미정의 신규 컬럼 차단)
        select_exprs: list[Column] = [col(field.name).cast(field.dataType).alias(field.name) for field in FINAL_SCHEMA]
        df_transformed = df_transformed.select(*select_exprs)

        # 2. 클렌징 룰 (정상 조건)
        valid_condition = (
            col("trip_miles").isNotNull() & (col("trip_miles") > 0) & (col("trip_miles") <= 1000) &
            col("trip_time").isNotNull() & (col("trip_time") > 0) & (col("trip_time") <= 86400) &
            col("driver_pay").isNotNull() & (col("driver_pay") >= 0) & (col("driver_pay") <= 5000)
        )

        df_valid = df_transformed.filter(valid_condition)
        df_invalid = df_transformed.filter(~valid_condition)

        valid_count = df_valid.count()
        invalid_count = df_invalid.count()

        logger.info(f"정상(Valid) 데이터: {valid_count:,} 건")
        logger.info(f"불합격(Invalid) 데이터: {invalid_count:,} 건")

        invalid_ratio = invalid_count / total_count
        if invalid_ratio >= self._error_threshold:
            error_msg = f"불합격 비율이 {invalid_ratio:.1%}로 임계치({self._error_threshold:.1%})를 초과했습니다."
            logger.error(error_msg)
            raise ValueError(error_msg)

        return df_valid
