"""정제된 Gas Price 데이터를 날짜별 Silver JSON으로 적재합니다."""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

DATASET = "gas_price"


def partition_path(base_dir: str, price_date: date) -> Path:
    """가격 기준일의 Silver Hive 파티션 경로를 반환합니다."""
    return Path(base_dir) / DATASET / f"price_date={price_date.isoformat()}"


def load(rows: list[dict], base_dir: str) -> list[str]:
    """정제된 레코드를 가격 기준일별 JSON 파일로 저장합니다."""
    if not rows:
        raise ValueError("적재할 Gas Price Silver 데이터가 없습니다.")

    paths: list[str] = []
    written_count = 0
    for row in rows:
        price_date = row.get("price_date")
        collected_at = row.get("collected_at")
        if not isinstance(price_date, date) or isinstance(price_date, datetime):
            raise ValueError("price_date가 date 형식이 아닙니다.")
        if not isinstance(collected_at, datetime):
            raise ValueError("collected_at이 datetime 형식이 아닙니다.")
        if collected_at.tzinfo is None:
            raise ValueError("collected_at에 시간대가 없습니다.")

        partition = partition_path(base_dir, price_date)
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / "gas_price.json"
        payload = {
            **row,
            "price_date": price_date.isoformat(),
            "collected_at": collected_at.astimezone(timezone.utc).isoformat(),
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
                paths.append(str(path))
                continue
            if collected_at == existing_collected_at:
                if any(
                    existing.get(key) != payload[key]
                    for key in (
                        "state",
                        "fuel_type",
                        "price_usd_per_gallon",
                        "price_date",
                        "source_url",
                    )
                ):
                    raise ValueError(
                        "동일한 collected_at의 Silver 값이 충돌합니다: "
                        f"{path}"
                    )
                paths.append(str(path))
                continue

        temporary_path = path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
        paths.append(str(path))
        written_count += 1

    logger.info(
        "Gas Price Silver 처리 완료: %d건 중 %d건 저장",
        len(paths),
        written_count,
    )
    return paths
