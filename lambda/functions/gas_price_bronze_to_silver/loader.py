"""정제된 Gas Price 데이터를 날짜별 Silver JSON으로 적재합니다."""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from pipeline_core.loader import Loader, WriteResult

logger = logging.getLogger(__name__)

DATASET = "gas_price"


class GasPriceSilverLoader(Loader):
    """정제된 레코드를 가격 기준일별 JSON 파일로 저장합니다."""

    def __init__(self, base_dir: str):
        self._base_dir = base_dir
        # 이번 실행이 처리한 price_date -> 파일 경로. 새로 쓴 것과 이미 최신이라
        # 건너뛴 것을 모두 담습니다. 핸들러가 대상 날짜 반영 여부를 확인하는 데 씁니다.
        self.handled: dict[str, str] = {}

    def partition_path(self, price_date: date) -> Path:
        """가격 기준일의 Silver Hive 파티션 경로를 반환합니다."""
        return Path(self._base_dir) / DATASET / f"price_date={price_date.isoformat()}"

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 Gas Price Silver 데이터가 없습니다.")

        written_count = 0
        for row in data:
            price_date = row.get("price_date")
            collected_at = row.get("collected_at")
            if not isinstance(price_date, date) or isinstance(price_date, datetime):
                raise ValueError("price_date가 date 형식이 아닙니다.")
            if not isinstance(collected_at, datetime):
                raise ValueError("collected_at이 datetime 형식이 아닙니다.")
            if collected_at.tzinfo is None:
                raise ValueError("collected_at에 시간대가 없습니다.")

            partition = self.partition_path(price_date)
            partition.mkdir(parents=True, exist_ok=True)
            path = partition / "gas_price.json"
            self.handled[price_date.isoformat()] = str(path)
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
                    continue

            temporary_path = path.with_suffix(".json.tmp")
            temporary_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary_path.replace(path)
            written_count += 1

        logger.info(
            "silver_load done processed=%d written=%d", len(data), written_count
        )
        # row_count 는 실제로 기록한 건수입니다. 이미 최신이라 건너뛴 건은 제외됩니다.
        return WriteResult(
            location=str(Path(self._base_dir) / DATASET),
            row_count=written_count,
        )
