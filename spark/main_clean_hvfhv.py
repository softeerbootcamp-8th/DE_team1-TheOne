import sys
import argparse
import logging
from pyspark.sql import SparkSession

from clean_hvfhv.extractor import extract_hvfhv
from clean_hvfhv.cleaner import clean_hvfhv
from clean_hvfhv.loader import load_parquet

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="HVFHV Clean Pipeline")
    parser.add_argument("--input_path", required=True, help="Path to bronze raw data")
    parser.add_argument("--output_path", required=True, help="Path to save silver clean data")
    parser.add_argument("--error_log_path", required=True, help="Path to save invalid data logs")
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory (default: 4g)")
    parser.add_argument("--error_threshold", type=float, default=0.2, help="Validation error threshold (default: 0.2)")
    args = parser.parse_args()

    # 1. Spark Session
    logger.info(f"Initializing Spark Session with memory: {args.spark_memory}")
    spark = SparkSession.builder \
        .appName("HVFHV_Clean_Pipeline") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", args.spark_memory) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. Extract
        df_raw = extract_hvfhv(spark, args.input_path)
        
        if df_raw.count() == 0:
            logger.info("처리할 데이터가 없습니다.")
            sys.exit(0)
            
        # 3. Clean
        df_valid, df_invalid = clean_hvfhv(df_raw, error_threshold=args.error_threshold)
        
        # 4. Load (Error logs)
        if df_invalid.count() > 0:
            load_parquet(df_invalid, args.error_log_path, partition_col=None)
            
        # 5. Load (Cleaned Data)
        if df_valid.count() > 0:
            load_parquet(df_valid, args.output_path, partition_col="year_month")
            
        logger.info("파이프라인 처리가 모두 완료되었습니다.")

    except Exception as e:
        logger.error(f"파이프라인 실행 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
