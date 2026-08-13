"""네 개 원천 Silver 를 차량 마스터 한 장으로 합칩니다.

그레인은 **(도시, 업체, 차종, 플랫폼, 상품)** 입니다. 차종 하나가 받을 수 있는
상품 수만큼 행이 늘어납니다.

    city  vendor     make_key model_key platform product        min_year
    NYC   fasttrack  TOYOTA   CAMRY     uber     UberX          2010
    NYC   fasttrack  TOYOTA   CAMRY     uber     Comfort        2015
    NYC   fasttrack  TOYOTA   CAMRY     lyft     Extra Comfort  2016
    NYC   fasttrack  HONDA    FIT       NULL     NULL           NULL      <- 자격 없음

상품별 `min_year` 가 다르므로 플랫폼당 한 행으로 접으면 그 값이 사라집니다.
펼친 채로 두고, 차종 단위로 접는 것은 Gold 가 합니다.

기준(base)은 **차량 대장**입니다. 리스 업체가 취급하지 않는 차는 추천할 수
없으므로 자격 목록에만 있는 차종은 버립니다. 반대로 대장에 있으면 자격이 하나도
없어도 남깁니다 — 아무 상품도 못 받는 차라는 사실 자체가 Gold 에서 필요합니다
(현재 차량이 그런 차라면 그게 바로 교체 후보이기 때문입니다).
"""

import logging
from typing import Optional

from pipeline_core.transformer import Transformer

from .extractor import SourceTables

logger = logging.getLogger(__name__)

PLATFORM_UBER = "uber"
PLATFORM_LYFT = "lyft"

# 제원을 어느 키로 붙였는지. 조인이 헐거워지는 순간을 Gold 가 알아야 합니다.
MATCH_MODEL = "MODEL"  # model_key 로 정확히 붙음
MATCH_BASE_MODEL = "BASE_MODEL"  # 구동방식 접미사를 뗀 base_model_key 로 붙음
MATCH_NONE = "NONE"  # 제원을 못 찾음 (연비/전비 전부 NULL)

# atv_type -> 연료 구분. 에너지 단가를 어느 쪽(휘발유 $/gal, 전기 $/kWh)으로
# 곱할지가 여기서 갈립니다. 디젤·CNG 는 별도 단가를 수집하지 않아 GAS 로 둡니다
# (뉴욕 리스 대장에는 아직 없습니다. 생기면 단가 수집부터 늘려야 합니다).
FUEL_TYPE_EV = "EV"
FUEL_TYPE_PHEV = "PHEV"
FUEL_TYPE_HYBRID = "HYBRID"
FUEL_TYPE_GAS = "GAS"


def _fuel_type(atv_type: object) -> str:
    text = str(atv_type or "").strip().casefold()
    if not text:
        return FUEL_TYPE_GAS
    if text == "ev":
        return FUEL_TYPE_EV
    if "plug-in" in text:
        return FUEL_TYPE_PHEV
    if "hybrid" in text:
        return FUEL_TYPE_HYBRID
    return FUEL_TYPE_GAS


def _better_spec(current: Optional[dict], candidate: dict) -> dict:
    """대표 제원 고르는 순서 — 연비 있는 것 > 최신 연식 > source_id 작은 것.

    연비가 비면 Gold 의 에너지비 계산이 통째로 NULL 이 되므로, 최신 연식보다
    값이 있는 쪽을 먼저 봅니다. source_id 는 동점일 때 결과를 고정하려고 씁니다
    (같은 입력이면 같은 행이 나와야 재실행 결과를 비교할 수 있습니다).
    """
    if current is None:
        return candidate

    def rank(spec: dict) -> tuple[bool, int]:
        return (spec.get("combined_mpg") is not None, spec.get("year") or 0)

    if rank(candidate) != rank(current):
        return candidate if rank(candidate) > rank(current) else current
    current_id = str(current.get("source_id") or "")
    candidate_id = str(candidate.get("source_id") or "")
    return candidate if candidate_id < current_id else current


class VehicleMasterSilverTransformer(Transformer):
    """대장 × 자격 곱집합에 제원을 붙여 차량 마스터 행을 만듭니다."""

    def transform(self, data: SourceTables) -> list[dict]:
        catalog = self._catalog_rows(data.catalog)
        by_model, by_base_model = self._build_spec_index(data.specs)
        eligibility = self._eligibility_index(data.uber, data.lyft)

        cities = sorted(eligibility)
        if not cities:
            raise ValueError("배차 자격 목록에 도시가 없습니다.")

        rows: list[dict] = []
        unmatched_specs = 0
        for city in cities:
            city_eligibility = eligibility[city]
            for vehicle in catalog:
                identity = (vehicle["make_key"], vehicle["model_key"])
                spec, match_level = self._resolve_spec(identity, by_model, by_base_model)
                if match_level == MATCH_NONE:
                    unmatched_specs += 1

                products = city_eligibility.get(identity, [])
                # 자격이 하나도 없으면 플랫폼·상품이 빈 행 하나를 남깁니다.
                for product in products or [None]:
                    rows.append(
                        self._row(city, vehicle, spec, match_level, product)
                    )

        if not rows:
            raise ValueError("차량 마스터로 만들 행이 없습니다.")

        logger.info(
            "vehicle_master_transform done cities=%d vehicles=%d rows=%d spec_unmatched=%d",
            len(cities),
            len(catalog),
            len(rows),
            unmatched_specs,
        )
        return rows

    @staticmethod
    def _catalog_rows(catalog: list[dict]) -> list[dict]:
        """대장을 검증하고 (업체, 차종) 순으로 정렬합니다."""
        if not catalog:
            raise ValueError("차량 대장 Silver 가 비어 있습니다.")

        seen: set[tuple[str, str, str]] = set()
        rows: list[dict] = []
        for row in catalog:
            vendor = str(row.get("vendor") or "").strip()
            make_key = str(row.get("make_key") or "").strip()
            model_key = str(row.get("model_key") or "").strip()
            if not vendor or not make_key or not model_key:
                raise ValueError(f"차량 대장에 조인 키가 없는 행이 있습니다: {row}")

            identity = (vendor, make_key, model_key)
            # 대장 Silver 단계에서 이미 걸러지지만, 여기서 통과시키면 자격 수만큼
            # 곱해져서 조용히 행이 배로 늘어납니다.
            if identity in seen:
                raise ValueError(f"차량 대장에 중복 차종이 있습니다: {identity}")
            seen.add(identity)
            rows.append(row)

        return sorted(rows, key=lambda r: (r["vendor"], r["make_key"], r["model_key"]))

    @staticmethod
    def _build_spec_index(specs: list[dict]) -> tuple[dict, dict]:
        """(make, model) 과 (make, base_model) 두 벌의 대표 제원 색인을 만듭니다."""
        if not specs:
            raise ValueError("차량 제원 Silver 가 비어 있습니다.")

        by_model: dict[tuple[str, str], dict] = {}
        by_base_model: dict[tuple[str, str], dict] = {}
        for spec in specs:
            make_key = spec.get("make_key")
            if not make_key:
                continue
            model_key = spec.get("model_key")
            if model_key:
                key = (make_key, model_key)
                by_model[key] = _better_spec(by_model.get(key), spec)
            base_model_key = spec.get("base_model_key")
            if base_model_key:
                key = (make_key, base_model_key)
                by_base_model[key] = _better_spec(by_base_model.get(key), spec)

        return by_model, by_base_model

    @staticmethod
    def _resolve_spec(
        identity: tuple[str, str], by_model: dict, by_base_model: dict
    ) -> tuple[Optional[dict], str]:
        spec = by_model.get(identity)
        if spec is not None:
            return spec, MATCH_MODEL
        # 대장은 "OUTLANDER SPORT", 제원은 "OUTLANDER SPORT 4WD" 처럼 구동방식이
        # 붙어 안 붙는 경우가 있습니다. 접미사를 뗀 키로 한 번 더 봅니다.
        spec = by_base_model.get(identity)
        if spec is not None:
            return spec, MATCH_BASE_MODEL
        return None, MATCH_NONE

    @staticmethod
    def _eligibility_index(uber: list[dict], lyft: list[dict]) -> dict:
        """도시 -> 차종 -> 자격 목록. 두 플랫폼을 platform 값으로 구분해 합칩니다."""
        index: dict[str, dict[tuple[str, str], list[dict]]] = {}
        seen: set[tuple] = set()

        for platform, rows in ((PLATFORM_UBER, uber), (PLATFORM_LYFT, lyft)):
            if not rows:
                raise ValueError(f"{platform} 배차 자격 Silver 가 비어 있습니다.")
            for row in rows:
                city = str(row.get("city") or "").strip()
                make_key = str(row.get("make_key") or "").strip()
                model_key = str(row.get("model_key") or "").strip()
                product = str(row.get("product") or "").strip()
                if not city or not make_key or not model_key or not product:
                    raise ValueError(f"{platform} 자격 목록에 빈 값이 있습니다: {row}")

                # Uber 와 Lyft 는 상품명이 겹칩니다("Black"). platform 을 넣지
                # 않으면 서로 다른 상품이 중복으로 잡힙니다.
                identity = (city, platform, make_key, model_key, product)
                if identity in seen:
                    raise ValueError(f"자격 목록에 중복 행이 있습니다: {identity}")
                seen.add(identity)

                index.setdefault(city, {}).setdefault((make_key, model_key), []).append(
                    {
                        "platform": platform,
                        "product": product,
                        "min_year": row.get("min_year"),
                        "bronze_path": row.get("bronze_path"),
                    }
                )

        for city_index in index.values():
            for products in city_index.values():
                products.sort(key=lambda p: (p["platform"], p["product"]))
        return index

    @staticmethod
    def _row(
        city: str,
        vehicle: dict,
        spec: Optional[dict],
        match_level: str,
        product: Optional[dict],
    ) -> dict:
        spec = spec or {}
        product = product or {}
        return {
            "city": city,
            "vendor": vehicle["vendor"],
            "make_key": vehicle["make_key"],
            "model_key": vehicle["model_key"],
            "platform": product.get("platform"),
            "product": product.get("product"),
            "min_year": product.get("min_year"),
            "weekly_price_usd": vehicle.get("weekly_price_usd"),
            "spec_year": spec.get("year"),
            "combined_mpg": spec.get("combined_mpg"),
            "combined_kwh_per_100mi": spec.get("combined_kwh_per_100mi"),
            "range_miles": spec.get("range_miles"),
            "atv_type": spec.get("atv_type"),
            # 제원을 못 붙였으면 연료 구분도 비웁니다. GAS 로 채우면 Gold 가
            # 없는 연비로 에너지비를 계산하려 듭니다.
            "fuel_type": _fuel_type(spec.get("atv_type")) if spec else None,
            "spec_match_level": match_level,
            # 계보 — 원천 Silver 가 물고 온 Bronze 경로를 그대로 옮깁니다.
            "catalog_bronze_path": vehicle.get("bronze_path"),
            "specs_bronze_path": spec.get("bronze_path"),
            "eligibility_bronze_path": product.get("bronze_path"),
        }
