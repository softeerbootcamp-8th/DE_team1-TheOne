"""Gas Price Bronze 원문을 월별 Silver 레코드로 정제합니다."""

import math
import re
from datetime import date, datetime, timezone

from pipeline_core.transformer import Transformer

PRICE_RE = re.compile(r"^\$\s*(\d+(?:\.\d+)?)$")


class GasPriceSilverTransformer(Transformer):
    """핵심 필드를 검증하고 UTC 수집일별 최신 가격만 남깁니다."""

    def transform(self, data: list[dict]) -> list[dict]:
        if not data:
            raise ValueError("변환할 Gas Price Bronze 데이터가 없습니다.")

        latest_by_date: dict[date, dict] = {}
        errors: list[str] = []

        for row in data:
            bronze_path = str(row.get("bronze_path") or "<unknown>")
            try:
                state = str(row.get("state") or "").strip().upper()
                fuel_type = str(row.get("fuel_type") or "").strip().lower()
                price_match = PRICE_RE.fullmatch(str(row["price_raw"]).strip())
                if not price_match:
                    raise ValueError("price_raw 형식이 올바르지 않습니다")
                price = float(price_match.group(1))
                price_date = datetime.strptime(
                    str(row["price_date_raw"]).strip(), "%m/%d/%y"
                ).date()
                collected_at = datetime.fromisoformat(
                    str(row["collected_at"]).replace("Z", "+00:00")
                )
                source_url = str(row.get("source_url") or "").strip()

                if state != "NY":
                    raise ValueError("state가 NY가 아닙니다")
                if fuel_type != "regular":
                    raise ValueError("fuel_type이 regular가 아닙니다")
                if not math.isfinite(price) or price <= 0:
                    raise ValueError("가격은 0보다 큰 유한 숫자여야 합니다")
                if collected_at.tzinfo is None:
                    raise ValueError("collected_at에 시간대가 없습니다")
                if not source_url:
                    raise ValueError("source_url이 비어 있습니다")

                collected_at_utc = collected_at.astimezone(timezone.utc)
                collected_date = collected_at_utc.date()
                if price_date > collected_date:
                    raise ValueError("price_date가 수집일보다 미래입니다")

                cleaned = {
                    "date": collected_date,
                    "gas_price": price,
                    "_collected_at": collected_at_utc,
                }
                previous = latest_by_date.get(collected_date)
                if (
                    previous is None
                    or cleaned["_collected_at"] > previous["_collected_at"]
                ):
                    latest_by_date[collected_date] = cleaned
                elif (
                    cleaned["_collected_at"] == previous["_collected_at"]
                    and cleaned["gas_price"] != previous["gas_price"]
                ):
                    raise ValueError(
                        "동일한 collected_at에 서로 다른 값이 있습니다"
                    )
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"{bronze_path}: {exc}")

        if errors:
            raise ValueError("Gas Price Silver 변환 실패:\n- " + "\n- ".join(errors))

        return [
            {
                "date": latest_by_date[key]["date"],
                "gas_price": latest_by_date[key]["gas_price"],
            }
            for key in sorted(latest_by_date)
        ]
