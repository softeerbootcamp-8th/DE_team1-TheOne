import logging
from pyspark.sql.functions import col, lit, when, date_format

logger = logging.getLogger(__name__)

def clean_hvfhv(df, error_threshold=0.2):
    """
    HVFHV 데이터의 클렌징, 변환, 파생 컬럼 추가를 수행합니다.
    불합격(Invalid) 데이터가 20%를 초과할 경우 Exception을 발생시킵니다.
    
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
        when(col("hvfhs_license_num") == "HV0003", "Uber")
        .when(col("hvfhs_license_num") == "HV0005", "Lyft")
        .otherwise("Unknown")
    ).withColumn("taxi_id", lit(None).cast("string")) \
     .withColumn("driver_id", lit(None).cast("string")) \
     .withColumn("taxi_model_id", lit(None).cast("string")) \
     .withColumn("year_month", date_format(col("pickup_datetime"), "yyyy-MM"))

    # 2. 클렌징 룰 (정상 조건)
    valid_condition = (
        col("trip_miles").isNotNull() & (col("trip_miles") > 0) &
        col("trip_time").isNotNull() & (col("trip_time") > 0) &
        col("driver_pay").isNotNull() & (col("driver_pay") >= 0) &
        col("base_passenger_fare").isNotNull() & (col("base_passenger_fare") >= 0)
    )
    
    # 3. 분리 및 캐싱
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
