"""Lyft 배차 가능 차량을 차량 대장과 조인 가능한 Silver 행으로 정제합니다.

Bronze 입력은 make, model, min_year, products, collected_at을 사용합니다.
products 예상값은 Extra Comfort, XL, XXL, Black, Black SUV이며,
Extractor가 파티션의 city와 원본 위치인 bronze_path를 추가합니다.
Silver에서는 make/model 조인 키와 상품별 최소 허용 연식으로 사용합니다.
"""

import logging
from datetime import datetime, timezone

from pipeline_core.transformer import Transformer

from ..common.join_keys import normalize_key

logger = logging.getLogger(__name__)

MIN_MODEL_YEAR = 1980
MAX_MODEL_YEAR = 2100

# 지역 한정 안내는 상품명이 아니므로 Lyft의 표준 상품명으로 통일합니다.
PRODUCT_ALIASES = {
    "Extra Comfort": "Extra Comfort",
    "XL": "XL",
    "XXL": "XXL",
    "Black": "Black",
    "Black only in select regions": "Black",
    "Black SUV": "Black SUV",
    "Black SUV only in select regions": "Black SUV",
}


def _as_utc(value: object) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ValueError("collected_at에 시간대가 없습니다")
    return parsed.astimezone(timezone.utc)


def _model_year(value: object) -> int:
    try:
        year = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("min_year가 숫자가 아닙니다") from exc
    if not MIN_MODEL_YEAR <= year <= MAX_MODEL_YEAR:
        raise ValueError(f"연식이 허용 범위를 벗어났습니다: {year}")
    return year


def _product(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("빈 상품명이 섞여 있습니다")
    try:
        return PRODUCT_ALIASES[raw]
    except KeyError as exc:
        raise ValueError(f"알 수 없는 Lyft 상품명입니다: {raw}") from exc


class LyftEligibleVehiclesSilverTransformer(Transformer):
    """Bronze 차량 목록을 (차종, 상품) 하나당 한 행으로 펼칩니다."""

    def transform(self, data: list[dict]) -> list[dict]:
        if not data:
            raise ValueError("변환할 Lyft 배차 가능 목록 Bronze 데이터가 없습니다.")

        errors: list[str] = []
        best: dict[tuple[str, str, str, str], dict] = {}

        for row in data:
            bronze_path = str(row.get("bronze_path") or "<unknown>")
            label = f"{row.get('make')} {row.get('model')}"
            try:
                city = str(row.get("city") or "").strip()
                if not city:
                    raise ValueError("city가 비어 있습니다")

                make_key = normalize_key(row.get("make"))
                model_key = normalize_key(row.get("model"))
                if not make_key or not model_key:
                    raise ValueError("make 또는 model이 비어 있습니다")

                min_year = _model_year(row.get("min_year"))
                collected_at = _as_utc(row.get("collected_at"))
                products = row.get("products") or []
                if not isinstance(products, list) or not products:
                    raise ValueError("products가 비어 있습니다")

                for raw_product in products:
                    product = _product(raw_product)
                    identity = (city, make_key, model_key, product)
                    previous = best.get(identity)
                    if previous is not None and previous["min_year"] <= min_year:
                        continue

                    best[identity] = {
                        "city": city,
                        "make_key": make_key,
                        "model_key": model_key,
                        "product": product,
                        "min_year": min_year,
                        "bronze_path": bronze_path,
                        "collected_at": collected_at,
                    }
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"{bronze_path} {label}: {exc}")

        if errors:
            raise ValueError(
                "Lyft 배차 가능 목록 Silver 변환 실패:\n- " + "\n- ".join(errors)
            )

        collected_dates = {row["collected_at"].date() for row in best.values()}
        if len(collected_dates) != 1:
            raise ValueError("하나의 Bronze 스냅샷에 수집일이 섞여 있습니다.")

        silver = sorted(
            best.values(),
            key=lambda row: (
                row["city"],
                row["make_key"],
                row["model_key"],
                row["product"],
            ),
        )
        logger.info("silver_transform done rows=%d", len(silver))
        return silver
