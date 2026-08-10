"""Uber 배차 가능 차량을 차량 대장과 조인 가능한 형태로 정제합니다.

Bronze 한 행은 "이 차종은 2018년식부터 Comfort, Comfort Electric 가능" 처럼
**상품이 여러 개 묶인** 형태입니다. 그대로 두면 쓰는 쪽이 매번 리스트를
풀어야 하고, "이 차로 Comfort 를 받을 수 있나?" 를 조인 조건으로 쓸 수 없습니다.

그래서 Silver 는 **(차종, 상품) 하나당 한 행**으로 펼칩니다.

    Bronze  ZDX  "2010 (UberX, Comfort) / 2018 (Comfort Electric)"
    Silver  ZDX  UberX             min_year=2010
            ZDX  Comfort           min_year=2010
            ZDX  Comfort Electric  min_year=2018

같은 상품이 여러 연식 묶음에 나오면 **가장 낮은 연식**을 남깁니다. min_year 는
"그 상품에 허용되는 가장 오래된 차량 연식" 이므로, 둘 중 낮은 쪽이 실제 기준입니다.

조인 키 규칙은 `common.join_keys` 를 따릅니다 (차량 대장과 반드시 동일해야 함).
"""

import logging
from datetime import datetime, timezone

from pipeline_core.transformer import Transformer

from ..common.join_keys import normalize_key

logger = logging.getLogger(__name__)

# 관측된 값은 1990~. 오탈자나 파싱 사고로 들어온 값을 걸러냅니다.
MIN_MODEL_YEAR = 1980
MAX_MODEL_YEAR = 2100


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


class UberEligibleVehiclesSilverTransformer(Transformer):
    """Bronze 배차 가능 목록을 (차종, 상품) 단위 Silver 행으로 펼칩니다."""

    def transform(self, data: list[dict]) -> list[dict]:
        if not data:
            raise ValueError("변환할 Uber 배차 가능 목록 Bronze 데이터가 없습니다.")

        errors: list[str] = []
        # (city, make_key, model_key, product) -> 행. 같은 키가 또 나오면 낮은 연식을 남깁니다.
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
                    product = str(raw_product or "").strip()
                    if not product:
                        raise ValueError("빈 상품명이 섞여 있습니다")

                    identity = (city, make_key, model_key, product)
                    previous = best.get(identity)
                    if previous is not None and previous["min_year"] <= min_year:
                        continue

                    best[identity] = {
                        "city": city,
                        # 조인 키 — 차량 대장 / 제원과 같은 규칙으로 만듭니다.
                        "make_key": make_key,
                        "model_key": model_key,
                        # 상품명은 Uber 표기를 그대로 씁니다. 대시보드 문구가 아니라
                        # 원천 식별자라 임의로 줄이면 되살릴 수 없습니다.
                        "product": product,
                        # 이 상품을 받으려면 차량 연식이 이 값 이상이어야 합니다.
                        "min_year": min_year,
                        "bronze_path": bronze_path,
                        # 아래는 적재하지 않고 검증에만 씁니다.
                        "collected_at": collected_at,
                    }
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"{bronze_path} {label}: {exc}")

        if errors:
            raise ValueError(
                "Uber 배차 가능 목록 Silver 변환 실패:\n- " + "\n- ".join(errors)
            )

        collected_dates = {row["collected_at"].date() for row in best.values()}
        if len(collected_dates) != 1:
            raise ValueError("하나의 Bronze 스냅샷에 수집일이 섞여 있습니다.")

        silver = sorted(
            best.values(),
            key=lambda r: (r["city"], r["make_key"], r["model_key"], r["product"]),
        )
        logger.info("silver_transform done rows=%d", len(silver))
        return silver
