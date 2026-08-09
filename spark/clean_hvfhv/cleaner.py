import logging
from pyspark.sql.functions import col, lit, when, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

logger = logging.getLogger(__name__)

FINAL_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("tolls", DoubleType(), True),
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

def clean_hvfhv(df, df_zone, error_threshold=0.2):
    """
    HVFHV 데이터의 클렌징, 변환, 파생 컬럼 추가를 수행합니다.
    불합격(Invalid) 데이터가 (error_threshold)를 초과할 경우 Exception을 발생시킵니다.
    연산이 단순하므로 캐싱 X

    Returns:
        (df_valid, df_invalid) : 정상 DataFrame과 불합격 DataFrame 튜플
    """
    logger.info("데이터 정제 및 변환 시작...")
    total_count = df.count()
    if total_count == 0:
        return df, df

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

    # 1.1. Taxi Zone Join (Pickup & Dropoff) (broadcast join)
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

    # 1.2. 불필요 원본 컬럼 삭제
    cols_to_drop = [
        "hvfhs_license_num", "dispatching_base_num", "originating_base_num",
        "request_datetime", "on_scene_datetime", "base_passenger_fare", 
        "bcf", "sales_tax", "congestion_surcharge", "airport_fee",
        "shared_request_flag", "shared_match_flag", "access_a_ride_flag", 
        "wav_request_flag", "wav_match_flag"
    ]
    df_transformed = df_transformed.drop(*cols_to_drop)

    # 1.3. 수익/비용 데이터 결측치 처리 (Null -> 0.0)
    df_transformed = df_transformed.fillna(0.0, subset=["tolls", "tips", "driver_pay"])

    # 1.4. 스키마 순서 및 타입 강제
    select_exprs = [col(field.name).cast(field.dataType).alias(field.name) for field in FINAL_SCHEMA]
    df_transformed = df_transformed.select(*select_exprs)

    # 2. 클렌징 룰 (정상 조건)
    valid_condition = (
        col("trip_miles").isNotNull() & (col("trip_miles") > 0) & (col("trip_miles") <= 1000) &
        col("trip_time").isNotNull() & (col("trip_time") > 0) & (col("trip_time") <= 86400) &
        col("driver_pay").isNotNull() & (col("driver_pay") >= 0) & (col("driver_pay") <= 5000)
    )
    
    # 3. 분리
    df_valid = df_transformed.filter(valid_condition)
    df_invalid = df_transformed.filter(~valid_condition)
    
    valid_count = df_valid.count()
    invalid_count = df_invalid.count()
    
    logger.info(f"정상(Valid) 데이터: {valid_count:,} 건")
    logger.info(f"불합격(Invalid) 데이터: {invalid_count:,} 건")
    
    # 4. 에러율 체크 로직
    invalid_ratio = invalid_count / total_count
    if invalid_ratio >= error_threshold:
        error_msg = f"불합격 비율이 {invalid_ratio:.1%}로 임계치({error_threshold:.1%})를 초과했습니다."
        logger.error(error_msg)
        raise ValueError(error_msg)
        
    return df_valid, df_invalid
