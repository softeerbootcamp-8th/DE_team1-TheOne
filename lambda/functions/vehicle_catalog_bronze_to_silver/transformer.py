"""리스 업체 보유 차량 대장을 다른 데이터셋과 조인 가능한 형태로 정제합니다.

Bronze 의 차종 표기는 업체 카드 이미지에서 읽은 값이라 전부 대문자입니다
("OUTLANDER SPORT"). 반면 차종별 제원(fueleconomy)과 배차 가능 목록(uber)은
"Outlander Sport" 처럼 일반 표기를 씁니다. 그대로는 붙지 않아서 대문자
조인 키를 따로 만듭니다 — 규칙과 그 이유는 `common.join_keys` 를 보세요.

원문(make/model/raw_name)은 그대로 남겨 추적할 수 있게 합니다.
"""

import math
from datetime import datetime, timezone

from pipeline_core.transformer import Transformer

from ..common.join_keys import normalize_key

EXPECTED_PRICE_PERIOD = "week"
# 주간 렌트료로 볼 수 없는 값을 걸러냅니다. 관측된 범위는 514~749 USD 입니다.
MIN_WEEKLY_PRICE_USD = 50.0
MAX_WEEKLY_PRICE_USD = 5000.0


def _as_utc(value: object) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ValueError("collected_at에 시간대가 없습니다")
    return parsed.astimezone(timezone.utc)


def _weekly_price(value: object) -> float:
    try:
        price = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("price_usd가 숫자가 아닙니다") from exc
    if not math.isfinite(price):
        raise ValueError("price_usd가 유효하지 않습니다")
    if not MIN_WEEKLY_PRICE_USD <= price <= MAX_WEEKLY_PRICE_USD:
        raise ValueError(f"주간 렌트료가 허용 범위를 벗어났습니다: {price}")
    return price


class VehicleCatalogSilverTransformer(Transformer):
    """Bronze 차량 대장을 조인 가능한 Silver 행 목록으로 정제합니다."""

    def transform(self, data: list[dict]) -> list[dict]:
        rows = data
        if not rows:
            raise ValueError("변환할 차량 대장 Bronze 데이터가 없습니다.")

        errors: list[str] = []
        seen: set[tuple[str, str, str]] = set()
        silver: list[dict] = []

        for row in rows:
            bronze_path = str(row.get("bronze_path") or "<unknown>")
            label = row.get("raw_name") or row.get("model") or "<unknown>"
            try:
                vendor = str(row.get("vendor") or "").strip()
                if not vendor:
                    raise ValueError("vendor가 비어 있습니다")

                make_key = normalize_key(row.get("make"))
                model_key = normalize_key(row.get("model"))
                if not make_key or not model_key:
                    # 이미지에서 차종을 못 읽은 행입니다. 조인 키를 만들 수 없습니다.
                    raise ValueError("make 또는 model이 비어 있습니다")

                if row.get("price_period") != EXPECTED_PRICE_PERIOD:
                    raise ValueError(f"price_period가 {EXPECTED_PRICE_PERIOD}가 아닙니다")

                identity = (vendor, make_key, model_key)
                if identity in seen:
                    raise ValueError("같은 업체에 동일 차종이 중복됩니다")
                seen.add(identity)

                source_url = str(row.get("source_url") or "").strip()
                if not source_url:
                    raise ValueError("source_url이 비어 있습니다")

                silver.append(
                    {
                        "vendor": vendor,
                        # 조인 키 — 상대 데이터셋도 대문자로 맞춰서 붙입니다.
                        "make_key": make_key,
                        "model_key": model_key,
                        "weekly_price_usd": _weekly_price(row.get("price_usd")),
                        # 어느 Bronze 파일에서 나왔는지. 같은 날 여러 번 수집하면
                        # 파일이 여러 개라 파티션 경로만으로는 특정이 안 됩니다.
                        "bronze_path": bronze_path,
                        # 아래는 적재하지 않고 검증에만 씁니다.
                        "collected_at": _as_utc(row.get("collected_at")),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"{bronze_path} {label}: {exc}")

        if errors:
            raise ValueError("차량 대장 Silver 변환 실패:\n- " + "\n- ".join(errors))

        collected_dates = {row["collected_at"].date() for row in silver}
        if len(collected_dates) != 1:
            raise ValueError("하나의 Bronze 스냅샷에 수집일이 섞여 있습니다.")

        return silver
