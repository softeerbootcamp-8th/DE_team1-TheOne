import logging

logger = logging.getLogger(__name__)

def extract_hvfhv(spark, input_path):
    """
    주어진 경로에서 HVFHV 파켓 데이터를 로드하여 DataFrame으로 반환합니다.
    """
    try:
        logger.info(f"데이터 로드 경로: {input_path}")
        df = spark.read.parquet(input_path)
        return df
    except Exception as e:
        logger.error(f"데이터를 읽는 데 실패했습니다: {e}")
        raise
