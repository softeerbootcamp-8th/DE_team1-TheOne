"""정제된 리스 업체 보유 차량 대장을 Silver Parquet 으로 적재합니다."""

import logging
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATASET = "lease_vehicle_catalog"

# vendor 는 파티션 키(vendor=)로만 남깁니다. 파일 안에 같은 이름의 컬럼을 또 두면
# 읽을 때 파티션 값(dictionary)과 타입이 충돌합니다.
PARTITION_KEY = "vendor"

# 소비자가 실제로 쓰는 것만 남깁니다. 표기 원문(make/model/raw_name), 링크,
# 상수(currency/price_unit)는 전부 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
# vendor / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("weekly_price_usd", pa.float64()),
        ("bronze_path", pa.string()),  # 계보
    ]
)


def partition_path(base_dir: str, collected_date: date, vendor: str) -> Path:
    """수집일 / 업체로 나눈 Silver Hive 파티션 경로."""
    return (
        Path(base_dir)
        / DATASET
        / f"collected_date={collected_date.isoformat()}"
        / f"vendor={vendor}"
    )


def load(rows: list[dict], base_dir: str) -> list[str]:
    """업체별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    Bronze 와 달리 Silver 는 재실행하면 덮어씁니다. 같은 수집일을 다시 변환한
    결과가 여러 개 남으면 읽는 쪽에서 무엇이 맞는지 알 수 없기 때문입니다.
    """
    if not rows:
        raise ValueError("적재할 차량 대장 Silver 데이터가 없습니다.")

    by_vendor: dict[str, list[dict]] = {}
    for row in rows:
        by_vendor.setdefault(row[PARTITION_KEY], []).append(row)

    paths: list[str] = []
    for vendor, vendor_rows in sorted(by_vendor.items()):
        collected_date = vendor_rows[0]["collected_at"].date()
        partition = partition_path(base_dir, collected_date, vendor)
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"{DATASET}.parquet"

        table = pa.Table.from_pylist(vendor_rows, schema=SCHEMA)
        pq.write_table(table, path, compression="snappy")

        logger.info("차량 대장 Silver 적재 완료: %s (%d행)", path, table.num_rows)
        paths.append(str(path))

    return paths
