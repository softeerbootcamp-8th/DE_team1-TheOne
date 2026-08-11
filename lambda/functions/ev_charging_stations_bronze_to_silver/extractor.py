"""대상 월의 일별 전기차 충전소 Bronze 원문 JSON을 읽습니다."""

import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from pipeline_core.extractor import Extractor

from ..common import ev_charging_layout as layout
from ..ev_charging_stations_raw_to_bronze.extractor import API_URL

logger = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class EvChargingBronzeExtractor(Extractor):
    """대상 월의 각 일별 파티션에서 최신 JSON 스냅샷을 읽습니다."""

    name = "ev_charging_bronze"

    def __init__(self, base_dir: str, collected_month: str):
        if not MONTH_RE.fullmatch(collected_month):
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")
        self._base_dir = base_dir
        self.collected_month = collected_month

    def extract(self) -> Iterator[dict]:
        dataset_path = layout.bronze_dataset_path(self._base_dir)
        partitions = sorted(
            dataset_path.glob(
                f"{layout.BRONZE_PARTITION_KEY}={self.collected_month}-*"
            )
        )
        paths = [
            files[-1]
            for partition in partitions
            if (files := sorted(partition.glob("*.json")))
        ]
        if not paths:
            raise FileNotFoundError(
                f"Bronze JSON 파일이 없습니다: "
                f"{dataset_path}/{layout.BRONZE_PARTITION_KEY}={self.collected_month}-*"
            )

        logger.info(
            "bronze_extract ready collected_month=%s snapshots=%d",
            self.collected_month,
            len(paths),
        )
        return map(self._read_snapshot, paths)

    @staticmethod
    def _read_snapshot(path: Path) -> dict:
        try:
            payload = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Bronze JSON을 읽지 못했습니다: {path}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"Bronze JSON이 객체 형식이 아닙니다: {path}")

        stations = payload.get("fuel_stations")
        if not isinstance(stations, list) or not stations:
            raise RuntimeError(f"Bronze JSON에 충전소 데이터가 없습니다: {path}")

        try:
            collected_at = datetime.strptime(
                path.stem, "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise RuntimeError(
                f"Bronze 파일명의 수집시각 형식이 잘못됐습니다: {path}"
            ) from exc

        partition_date = path.parent.name.removeprefix(
            f"{layout.BRONZE_PARTITION_KEY}="
        )
        if partition_date != collected_at.date().isoformat():
            raise RuntimeError(f"Bronze 파티션과 파일의 수집일이 다릅니다: {path}")

        logger.info("bronze_extract done path=%s rows=%d", path, len(stations))
        return {
            "fuel_stations": stations,
            "source_url": API_URL,
            "collected_at": collected_at,
            "bronze_path": str(path),
        }
