"""뉴욕시 일별 평균 충전 요금을 Silver JSON으로 적재합니다."""

import json
import logging
import math
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)

DATASET = "ev_charging_price"
COUNT_FIELDS = (
    "nyc_station_count",
    "normalized_price_count",
    "free_station_count",
    "missing_price_count",
    "unsupported_price_count",
)


def partition_path(base_dir: str, price_date: date) -> Path:
    """가격 기준일의 Silver Hive 파티션 경로를 반환합니다."""
    return Path(base_dir) / DATASET / f"price_date={price_date.isoformat()}"


def load(row: dict, base_dir: str) -> str:
    """정제된 일별 평균 행 하나를 JSON 파일로 저장합니다."""
    price_date = row.get("price_date")
    collected_at = row.get("collected_at")
    if not isinstance(price_date, date) or isinstance(price_date, datetime):
        raise ValueError("price_date가 date 형식이 아닙니다.")
    if not isinstance(collected_at, datetime) or collected_at.tzinfo is None:
        raise ValueError(
            "collected_at은 시간대가 있는 datetime이어야 합니다."
        )
    collected_at = collected_at.astimezone(timezone.utc)
    if price_date != collected_at.date():
        raise ValueError("price_date와 collected_at의 UTC 날짜가 다릅니다.")
    if row.get("city") != "New York City" or row.get("state") != "NY":
        raise ValueError("뉴욕시 데이터가 아닙니다.")
    if row.get("fuel_type_code") != "ELEC":
        raise ValueError("fuel_type_code가 ELEC가 아닙니다.")
    if row.get("currency") != "USD" or row.get("price_unit") != "kWh":
        raise ValueError(
            "가격 통화 또는 단위가 표준 형식이 아닙니다."
        )

    try:
        average_price = float(row["average_price_usd_per_kwh"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("평균 충전 요금이 숫자가 아닙니다.") from exc
    if not math.isfinite(average_price) or average_price < 0:
        raise ValueError("평균 충전 요금이 유효하지 않습니다.")

    counts: dict[str, int] = {}
    for field in COUNT_FIELDS:
        value = row.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{field}는 0 이상의 정수여야 합니다.")
        counts[field] = value
    if not counts["normalized_price_count"]:
        raise ValueError("표준화된 충전 요금이 없습니다.")
    if counts["nyc_station_count"] != sum(
        counts[field] for field in COUNT_FIELDS if field != "nyc_station_count"
    ):
        raise ValueError(
            "충전소 품질 검증 건수의 합계가 맞지 않습니다."
        )

    source_url = str(row.get("source_url") or "").strip()
    bronze_path = str(row.get("bronze_path") or "").strip()
    if not source_url or not bronze_path:
        raise ValueError("source_url 또는 bronze_path가 비어 있습니다.")

    partition = partition_path(base_dir, price_date)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / "ev_charging_price.json"
    payload = {
        **row,
        "average_price_usd_per_kwh": average_price,
        "price_date": price_date.isoformat(),
        "collected_at": collected_at.isoformat(),
    }

    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            existing_collected_at = datetime.fromisoformat(
                str(existing["collected_at"]).replace("Z", "+00:00")
            )
            if existing_collected_at.tzinfo is None:
                raise ValueError("collected_at에 시간대가 없습니다")
        except (
            OSError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise RuntimeError(
                f"기존 Silver JSON을 읽지 못했습니다: {path}"
            ) from exc

        if collected_at < existing_collected_at:
            logger.info(
                "기존 Silver JSON이 더 최신이어서 유지합니다: %s", path
            )
            return str(path)
        if collected_at == existing_collected_at:
            if existing != payload:
                raise ValueError(
                    f"동일한 collected_at의 Silver 값이 충돌합니다: {path}"
                )
            return str(path)

    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)

    logger.info("EV Charging Silver 적재 완료: %s", path)
    return str(path)
