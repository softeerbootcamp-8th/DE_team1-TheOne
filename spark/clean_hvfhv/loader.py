import logging

logger = logging.getLogger(__name__)

def load_parquet(df, output_path, partition_col=None, mode="overwrite"):
    """
    DataFrame을 Parquet 포맷으로 지정된 경로에 적재합니다.
    """
    logger.info(f"데이터 적재 시작 -> {output_path}")
    try:
        writer = df.write.mode(mode)
        if partition_col:
            logger.info(f"파티션 컬럼 적용: {partition_col}")
            writer = writer.partitionBy(partition_col)
            
        writer.parquet(output_path)
        logger.info("적재 완료.")
    except Exception as e:
        logger.error(f"적재 실패: {e}")
        raise
