import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import (
    col, count, date_format, lit, percentile_approx, row_number, sha2, struct, to_json, when
)

from pipeline_core.transformer import Transformer

from jobs.driver_assignment.source_schema import FINAL_SCHEMA, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)

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

# 실측 자연키 충돌률은 2024-01 기준 4/19,663,930 ≈ 2e-7 이고, 같은 기간을 통째로 다시
# 적재하면 0.5 다. 6자릿수 차이라 그 사이면 되는데, 하루치 백필처럼 입력이 작을 때
# 정상 충돌 1건이 비율을 끌어올리는 쪽만 피하면 된다.
# ponytail: 고정 비율. 월별 충돌률이 자릿수로 움직이면 관측 기반으로 바꿀 것
NATURAL_KEY_COLLISION_RATIO_LIMIT = 0.05


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
            # 승차보다 하차가 앞선 행이 실제로 들어옵니다. 2026-06 원본에 2천만 건 중
            # 1건 있었고(pickup 23:17:23 / dropoff 23:15:19), `trip_time` 이 97 로
            # 양수라 아래 검사들을 전부 통과해 Silver 까지 올라왔습니다.
            #
            # 그 한 행이 기사 배정의 계약 검증(`allocator._validate`)에서 전체 job 을
            # 죽입니다. 시간 순서는 Silver 가 보장해야 하는 성질이라 여기서 거릅니다 —
            # 하류마다 따로 막으면 Gold 에서 또 같은 자리에 걸립니다.
            (col("pickup_datetime") < col("dropoff_datetime")) &
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
                zone_path = Path(__file__).resolve().parents[3] / zone_path
            df_zone = spark.read.option("header", "true").csv(str(zone_path))

        # 2.1 파생 컬럼 추가
        # 9개 자연키가 완전히 같은 별개 운행이 실데이터에 존재한다 (2024-01 기준 2쌍).
        # 그룹 내 순번을 키에 섞어 유일성을 만든다 — 구성 값이 모두 같은 행들이라 정렬
        # 기준과 무관하게 생성되는 키 집합이 같고, 재실행 결정성은 그대로 유지된다.
        occurrence_window = Window.partitionBy(*TRIP_KEY_COLUMNS).orderBy(lit(1))
        df_keyed = df_valid.withColumn("_trip_occurrence", row_number().over(occurrence_window))

        # 순번이 유일성을 만들어 주는 대신, 같은 달을 통째로 다시 적재해도 키가 갈려
        # 조용히 2배가 된다. 그 경우만 비율로 가려낸다.
        collided_count = df_keyed.filter(col("_trip_occurrence") > 1).count()
        if valid_count > 0:
            collision_ratio = collided_count / valid_count
            if collision_ratio >= NATURAL_KEY_COLLISION_RATIO_LIMIT:
                error_msg = (
                    f"자연키 충돌 비율이 {collision_ratio:.2%}로 임계치"
                    f"({NATURAL_KEY_COLLISION_RATIO_LIMIT:.2%})를 초과했습니다. "
                    "같은 기간을 중복 적재했는지 확인하세요."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

        canonical_trip = to_json(
            struct(
                *(col(name).alias(name) for name in TRIP_KEY_COLUMNS),
                col("_trip_occurrence"),
            ),
            options={"ignoreNullFields": "false"},
        )
        df_transformed = df_keyed.withColumn(
            "trip_key", sha2(canonical_trip, 256)
        ).drop("_trip_occurrence").withColumn(
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
