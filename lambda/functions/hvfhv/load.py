"""NYC FHVHV Trip Record 적재(load).

extract 가 다운로드한 bytes 데이터를 지정된 Hive 파티션 경로에 Parquet 파일로 직접 저장합니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa

logger = logging.getLogger(__name__)

# 데이터셋 고유 명칭
DATASET = "hvfhv"

# NYC TLC High Volume For-Hire Vehicle(FHVHV) 데이터셋 사양 기반의 표시/참조용 스키마 정의
SCHEMA = pa.schema(
    [
        ("hvfhs_license_num", pa.string()),
        ("dispatching_base_num", pa.string()),
        ("originating_base_num", pa.string()),
        ("request_datetime", pa.timestamp("us")),
        ("on_scene_datetime", pa.timestamp("us")),
        ("pickup_datetime", pa.timestamp("us")),
        ("dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("trip_miles", pa.float64()),
        ("trip_time", pa.int64()),
        ("base_passenger_fare", pa.float64()),
        ("tolls", pa.float64()),
        ("bcf", pa.float64()),
        ("sales_tax", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64()),
        ("tips", pa.float64()),
        ("driver_pay", pa.float64()),
        ("shared_request_flag", pa.string()),
        ("shared_match_flag", pa.string()),
        ("access_a_ride_flag", pa.string()),
        ("wav_request_flag", pa.string()),
        ("wav_match_flag", pa.string()),
    ]
)


def partition_path(base_dir: str, collected_at: datetime) -> Path:
    """collected_date 로 분류한 Hive 파티션 디렉토리 경로를 생성합니다."""
    return (
        Path(base_dir)
        / DATASET
        / f"collected_date={collected_at:%Y-%m-%d}"
    )


def load(content: bytes, base_dir: str, collected_at: datetime) -> str:
    """다운로드한 Parquet 파일 바이너리(bytes)를 파티션 내 단일 parquet 파일로 저장합니다."""
    partition = partition_path(base_dir, collected_at)
    partition.mkdir(parents=True, exist_ok=True)
    
    file_name = f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    path = partition / file_name

    logger.info("바이너리 데이터 저장 시도: %s", path)
    
    # 전달받은 원본 bytes 내용을 파싱 없이 파일로 직접 씁니다.
    path.write_bytes(content)

    logger.info(
        "적재 완료: %s (%d bytes)", 
        path, 
        len(content)
    )
    return str(path)
