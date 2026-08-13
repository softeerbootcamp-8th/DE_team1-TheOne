"""NYC FHVHV Trip Record 적재(load).

extract 가 다운로드한 bytes 데이터를 지정된 Hive 파티션 경로에 Parquet 파일로 직접 저장합니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from pipeline_core.loader import Loader, WriteResult

from ..common.schema_validator import validate_parquet_schema
from ..common.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)

# 데이터셋 고유 명칭
DATASET = "hvfhv"

# NYC TLC High Volume For-Hire Vehicle(FHVHV) 데이터셋 사양 기반의 표시/참조용 스키마 정의
# TLC 원본 Parquet 의 **물리 스키마**입니다. Bronze 는 받은 바이트를 파싱 없이 그대로
# 쓰므로(`write` 참고) 여기 적힌 타입이 실제 파일과 다르면 검증이 영원히 통과하지
# 못합니다 — `pa.string()` / `pa.int64()` 로 두었다가 그렇게 됐습니다(#324).
#
# 월별 원본 footer 를 직접 확인한 값입니다.
#
#     2024-06  필드 24  large_string  int32  cbd_congestion_fee 없음
#     2025-01  필드 25  large_string  int32  cbd_congestion_fee 있음  <- 이때 추가
#     2026-06  필드 25  large_string  int32  cbd_congestion_fee 있음
SCHEMA = pa.schema(
    [
        ("hvfhs_license_num", pa.large_string()),
        ("dispatching_base_num", pa.large_string()),
        ("originating_base_num", pa.large_string()),
        ("request_datetime", pa.timestamp("us")),
        ("on_scene_datetime", pa.timestamp("us")),
        ("pickup_datetime", pa.timestamp("us")),
        ("dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
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
        ("shared_request_flag", pa.large_string()),
        ("shared_match_flag", pa.large_string()),
        ("access_a_ride_flag", pa.large_string()),
        ("wav_request_flag", pa.large_string()),
        ("wav_match_flag", pa.large_string()),
        # CBD 혼잡통행료. 2025-01 부터 있습니다 — 그 이전 달에는 이 컬럼이 없어서
        # 스키마 변동으로 잡히지만, 원본 그대로가 맞으므로 적재는 그대로 진행합니다.
        ("cbd_congestion_fee", pa.float64()),
    ]
)

# 2024-12 이전 원본에는 `cbd_congestion_fee` 가 없습니다. 부트스트랩 풀
# (`spark/jobs/driver_master/traits.py`)이 2024년 12개월을 쓰므로 그 달들도
# 백필할 수 있어야 합니다.
CBD_CONGESTION_FEE_SINCE = "2025-01"
LEGACY_SCHEMA = pa.schema(
    [field for field in SCHEMA if field.name != "cbd_congestion_fee"]
)


class HvfhvBronzeLoader(Loader):
    """다운로드한 Parquet 바이너리를 파티션 내 단일 parquet 파일로 저장합니다."""

    def __init__(self, base_dir: str, year_month: str, collected_at: datetime):
        self._base_dir = base_dir
        self._year_month = year_month
        self._collected_at = collected_at

    def partition_path(self) -> Path:
        return (
            Path(self._base_dir)
            / DATASET
            / f"year_month={self._year_month}"
        )

    def write(self, data: bytes) -> WriteResult:
        # 스키마 검증 및 Drift 감지 시 Slack 알림 발송 (적재 실패를 유발하지 않음)
        try:
            diffs:list[str] = validate_parquet_schema(data, SCHEMA)
            if diffs:
                logger.warning(
                    "HVFHV 스키마 변동(Schema Drift) 감지 (%d건): %s",
                    len(diffs),
                    diffs,
                )
                SlackNotifier().send_schema_drift_alert(
                    DATASET, self._year_month, diffs
                )
        except Exception as exc:
            logger.error("스키마 검사 중 오류 발생 (Raw 데이터 적재는 진행): %s", exc)

        partition = self.partition_path()
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"{self._collected_at:%Y%m%dT%H%M%SZ}.parquet"

        # 전달받은 원본 bytes 내용을 파싱 없이 파일로 직접 씁니다.
        path.write_bytes(data)

        logger.info("bronze_load done path=%s bytes=%d", path, len(data))
        # 원본 파일을 그대로 쓰므로 행 수를 세려면 parquet 을 열어야 합니다.
        # Bronze 는 원본 보존이 목적이라 파일 1개를 1로 셉니다.
        return WriteResult(location=str(path), row_count=1)

