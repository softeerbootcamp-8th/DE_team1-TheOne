"""Lyft Premium 차량 원본 JSON과 Bronze Parquet 적재."""

import json
import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

logger = logging.getLogger(__name__)

DATASET = "lyft_eligible_vehicles"

SCHEMA = pa.schema(
    [
        ("city_slug", pa.string()),
        ("make", pa.string()),
        ("model", pa.string()),
        ("min_year", pa.int16()),
        ("ride_types", pa.list_(pa.string())),
        ("raw_eligibility", pa.string()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)


def partition_path(base_dir: str, city_slug: str, collected_at: datetime) -> Path:
    return (
        Path(base_dir)
        / DATASET
        / f"collected_date={collected_at:%Y-%m-%d}"
        / f"city={city_slug}"
    )


def raw_file(base_dir: str, city_slug: str, collected_at: datetime) -> Path:
    return partition_path(base_dir, city_slug, collected_at) / (
        f"{collected_at:%Y%m%dT%H%M%SZ}.json"
    )


def bronze_file(base_dir: str, city_slug: str, collected_at: datetime) -> Path:
    return partition_path(base_dir, city_slug, collected_at) / (
        f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    )


def _raw_vehicles(rows: list[dict]) -> list[dict]:
    """연식별로 펼친 행에서 모델별 원문 한 건만 남깁니다."""
    vehicles: list[dict] = []
    seen: set[tuple] = set()
    for row in rows:
        key = (row["make"], row["model"], row["raw_eligibility"])
        if key in seen:
            continue
        seen.add(key)
        vehicles.append(
            {
                "make": row["make"],
                "model": row["model"],
                "raw_eligibility": row["raw_eligibility"],
            }
        )
    return vehicles


class LyftEligibleVehiclesLoader(Loader):
    """원문은 Raw JSON, 정형 행은 Bronze Parquet으로 함께 저장합니다."""

    def __init__(self, raw_dir: str, bronze_dir: str, collected_at: datetime):
        self._raw_dir = raw_dir
        self._bronze_dir = bronze_dir
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 Lyft 차량 데이터가 없습니다")

        cities = {row.get("city_slug") for row in data}
        if len(cities) != 1 or None in cities:
            raise ValueError(
                f"한 번에 한 도시만 적재할 수 있습니다: cities={cities}"
            )
        city_slug = data[0]["city_slug"]

        raw_path = raw_file(self._raw_dir, city_slug, self._collected_at)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_temporary = raw_path.with_suffix(".tmp")
        raw_temporary.write_text(
            json.dumps(
                {
                    "city_slug": city_slug,
                    "source_url": data[0]["source_url"],
                    "collected_at": data[0]["collected_at"].isoformat(),
                    "vehicles": _raw_vehicles(data),
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        raw_temporary.replace(raw_path)

        path = bronze_file(self._bronze_dir, city_slug, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(data, schema=SCHEMA)
        temporary = path.with_suffix(".tmp")
        pq.write_table(table, temporary, compression="snappy")
        temporary.replace(path)

        logger.info(
            "lyft_load done raw_path=%s path=%s rows=%d bytes=%d",
            raw_path,
            path,
            table.num_rows,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)
