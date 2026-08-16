"""NYC FHVHV Trip Record 적재(load).

extract 가 다운로드한 bytes 데이터를 지정된 Hive 파티션 경로에 Parquet 파일로 직접 저장합니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from pipeline_core.loader import Loader, WriteResult

from schema.bronze.hvfhv import LEGACY_SCHEMA, SCHEMA

from ..common.atomic_write import atomic_write
from ..common.schema_validator import validate_parquet_schema
from ..common.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)

# 데이터셋 고유 명칭
DATASET = "hvfhv"

# 2024-12 이전 원본에는 cbd_congestion_fee 가 없습니다. 부트스트랩 풀
# (`spark/jobs/driver_master/traits.py`)이 2024년 12개월을 쓰므로 그 달들도
# 백필할 수 있어야 합니다.
CBD_CONGESTION_FEE_SINCE = "2025-01"


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
        partition = self.partition_path()
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"{self._collected_at:%Y%m%dT%H%M%SZ}.parquet"

        def write_valid_parquet(temporary: Path) -> None:
            temporary.write_bytes(data)
            try:
                pq.read_schema(temporary)
            except Exception as exc:
                raise ValueError(
                    "HVFHV 원본이 읽을 수 있는 Parquet이 아닙니다"
                ) from exc

            # 읽을 수 있는 원본의 schema drift는 알리되 Bronze 적재는 유지합니다.
            try:
                diffs: list[str] = validate_parquet_schema(data, SCHEMA)
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
                logger.error(
                    "스키마 검사 중 오류 발생 (Raw 데이터 적재는 진행): %s",
                    exc,
                )

        # 임시 파일을 Parquet으로 읽을 수 있을 때만 최종 경로에 공개합니다.
        atomic_write(path, write_valid_parquet)

        logger.info("bronze_load done path=%s bytes=%d", path, len(data))
        # 원본 파일을 그대로 쓰므로 행 수를 세려면 parquet 을 열어야 합니다.
        # Bronze 는 원본 보존이 목적이라 파일 1개를 1로 셉니다.
        return WriteResult(location=str(path), row_count=1)
