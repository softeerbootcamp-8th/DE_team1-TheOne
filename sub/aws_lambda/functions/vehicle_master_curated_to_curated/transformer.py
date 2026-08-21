"""네 개 원천 Curated 를 차량 마스터 한 장으로 합칩니다.

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

제원은 **대표 1건이 아니라 후보 트림의 범위**로 내보냅니다. 대장에는 트림이 없고
(`SPORTAGE`) 제원에는 트림마다 행이 있어(`SPORTAGE FWD` 28mpg / `SPORTAGE HYBRID
FWD` 41mpg) 어느 쪽이 맞는지 알 수 없기 때문입니다. 하나를 골라 적으면 그 값이
그대로 Gold 의 에너지비가 되고, 틀려도 아무도 모릅니다(#320).
"""

import logging
from datetime import date
from typing import Optional

from pipeline_core.transformer import Transformer

from schema.source import VEHICLE_MASTER_REQUIRED_NON_NULL

from .extractor import SourceTables

logger = logging.getLogger(__name__)

PLATFORM_UBER = "uber"
PLATFORM_LYFT = "lyft"

# 제원을 어떻게 붙였는지. 조인이 헐거워지는 순간을 Gold 가 알아야 합니다.
MATCH_MODEL = "MODEL"  # model_key 로 정확히 붙음
MATCH_DRIVETRAIN = "DRIVETRAIN"  # 구동방식 접미사만 다른 행에 붙음
MATCH_NONE = "NONE"  # 제원을 못 찾음 (연비/전비 전부 NULL)

# 대장의 차종명 뒤에 이 토큰만 더 붙어 있으면 **같은 차**로 봅니다.
#
# 대장은 "SPORTAGE" 처럼 트림 없이 적히는데 제원은 트림마다 행이 따로입니다.
# 실측 접미사 분포에서 같은 차인 것과 아닌 것이 뚜렷하게 갈렸습니다.
#
#     같은 차   2WD 144 · 4WD 144 · AWD 107 · FWD 89
#     다른 차   WAGON 40 · SOLARA 34 · SPORT 33 · KOUP 27 · HYBRID 22
#
# `HYBRID` 를 넣지 않는 이유가 중요합니다. `SPORTAGE`(내연 24~28mpg)와
# `SPORTAGE HYBRID`(41mpg)를 한 후보로 묶으면 대장에 트림 정보가 없는 상태에서
# 연비를 최대 40% 과대평가하게 됩니다(#320). 하이브리드를 취급하게 되면 대장
# 표기부터 구분돼야 하고, 그때 이 목록이 아니라 대장 파싱을 고쳐야 합니다.
DRIVETRAIN_SUFFIXES = frozenset({"2WD", "4WD", "AWD", "FWD", "RWD"})

# 후보로 삼을 연식 범위 (기준일 연도 대비). 제원 원본에는 1984년식부터 들어 있고
# 미출시 연식도 미리 올라옵니다 — 실제로 2026-08 수집분에 2027년식이 있었습니다.
# 리스 대장은 최근 연식 차량이라 그 바깥은 후보에서 뺍니다.
SPEC_YEAR_LOOKBACK = 3
SPEC_YEAR_LOOKAHEAD = 1

# atv_type -> 연료 구분. 에너지 단가를 어느 쪽(휘발유 $/gal, 전기 $/kWh)으로
# 곱할지가 여기서 갈립니다. 디젤·CNG 는 별도 단가를 수집하지 않아 GAS 로 둡니다
# (뉴욕 리스 대장에는 아직 없습니다. 생기면 단가 수집부터 늘려야 합니다).
FUEL_TYPE_EV = "EV"
FUEL_TYPE_PHEV = "PHEV"
FUEL_TYPE_HYBRID = "HYBRID"
FUEL_TYPE_GAS = "GAS"
# 후보 트림의 연료가 갈릴 때. Gold 가 어느 단가를 곱할지 정할 수 없으므로
# 임의로 하나를 고르지 않고 그 사실을 그대로 넘깁니다.
FUEL_TYPE_MIXED = "MIXED"


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


def _minmax(specs: list[dict], column: str) -> tuple[Optional[float], Optional[float]]:
    """후보 트림에서 관측된 값의 범위. 값이 하나도 없으면 (None, None)."""
    values = [spec[column] for spec in specs if spec.get(column) is not None]
    if not values:
        return None, None
    return min(values), max(values)


class VehicleMasterCuratedTransformer(Transformer):
    """대장 × 자격 곱집합에 제원을 붙여 차량 마스터 행을 만듭니다."""

    def transform(self, data: SourceTables) -> list[dict]:
        catalog = self._catalog_rows(data.catalog)
        spec_index = self._build_spec_index(data.specs, data.as_of)
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
                specs, match_level = self._resolve_specs(identity, spec_index)
                if match_level == MATCH_NONE:
                    unmatched_specs += 1

                products = city_eligibility.get(identity, [])
                # 자격이 하나도 없으면 플랫폼·상품이 빈 행 하나를 남깁니다.
                for product in products or [None]:
                    rows.append(
                        self._row(city, vehicle, specs, match_level, product)
                    )

        if not rows:
            raise ValueError("차량 마스터로 만들 행이 없습니다.")

        self._require_non_null(rows)

        logger.info(
            "vehicle_master_transform done cities=%d vehicles=%d rows=%d spec_unmatched=%d",
            len(cities),
            len(catalog),
            len(rows),
            unmatched_specs,
        )
        return rows

    @staticmethod
    def _require_non_null(rows: list[dict]) -> None:
        """계약상 항상 값이 있어야 할 컬럼이 비지 않았는지 봅니다.

        상류 Curated 의 컬럼명이 바뀌면 `_row` 의 `.get()` 이 예외 없이 None 을 돌려주고,
        그 컬럼만 통째로 빈 채 적재까지 성공합니다. Airflow 검증도 스키마 이름·타입만
        보므로 nullable 컬럼은 전 행이 NULL 이어도 통과합니다 (#567).

        Lambda 단독 실행 경로라 GX 로는 못 막습니다 — `great-expectations` 는
        `main/airflow` 에만 선언돼 있습니다.
        """
        for column in sorted(VEHICLE_MASTER_REQUIRED_NON_NULL):
            missing = sum(1 for row in rows if row.get(column) is None)
            if missing:
                raise ValueError(
                    f"{column} 이 {missing}/{len(rows)} 행에서 비었습니다. "
                    "상류 Curated 의 컬럼명이 바뀌지 않았는지 확인하세요."
                )

    @staticmethod
    def _catalog_rows(catalog: list[dict]) -> list[dict]:
        """대장을 검증하고 (업체, 차종) 순으로 정렬합니다."""
        if not catalog:
            raise ValueError("차량 대장 Curated 가 비어 있습니다.")

        seen: set[tuple[str, str, str]] = set()
        rows: list[dict] = []
        for row in catalog:
            vendor = str(row.get("vendor") or "").strip()
            make_key = str(row.get("make_key") or "").strip()
            model_key = str(row.get("model_key") or "").strip()
            if not vendor or not make_key or not model_key:
                raise ValueError(f"차량 대장에 조인 키가 없는 행이 있습니다: {row}")

            identity = (vendor, make_key, model_key)
            # 대장 Curated 단계에서 이미 걸러지지만, 여기서 통과시키면 자격 수만큼
            # 곱해져서 조용히 행이 배로 늘어납니다.
            if identity in seen:
                raise ValueError(f"차량 대장에 중복 차종이 있습니다: {identity}")
            seen.add(identity)
            rows.append(row)

        return sorted(rows, key=lambda r: (r["vendor"], r["make_key"], r["model_key"]))

    @staticmethod
    def _build_spec_index(
        specs: list[dict], as_of: Optional[date]
    ) -> dict[tuple[str, str], list[dict]]:
        """(make, model) -> 그 표기의 제원 행 전체.

        대표 1건을 여기서 고르지 않습니다. 대장에 트림 정보가 없어 어느 행이
        맞는지 알 수 없기 때문입니다. 후보를 통째로 넘기고 범위로 내보냅니다.

        `base_model_key` 는 쓰지 않습니다. 원본의 `baseModel` 이 뭉툭해서
        `OUTLANDER` 아래에 `OUTLANDER SPORT 2WD` 까지 들어옵니다 — 다른 차입니다.
        """
        if not specs:
            raise ValueError("차량 제원 Curated 가 비어 있습니다.")

        min_year, max_year = None, None
        if as_of is not None:
            min_year = as_of.year - SPEC_YEAR_LOOKBACK
            max_year = as_of.year + SPEC_YEAR_LOOKAHEAD

        index: dict[tuple[str, str], list[dict]] = {}
        for spec in specs:
            make_key, model_key = spec.get("make_key"), spec.get("model_key")
            if not make_key or not model_key:
                continue
            year = spec.get("year")
            if min_year is not None and not (min_year <= (year or 0) <= max_year):
                continue
            index.setdefault((make_key, model_key), []).append(spec)

        return index

    @staticmethod
    def _resolve_specs(
        identity: tuple[str, str], index: dict[tuple[str, str], list[dict]]
    ) -> tuple[list[dict], str]:
        exact = index.get(identity)
        if exact:
            return exact, MATCH_MODEL

        # 대장은 "SPORTAGE", 제원은 "SPORTAGE FWD" / "SPORTAGE AWD" 처럼 구동방식이
        # 붙어 정확히 안 붙습니다. 남는 토큰이 전부 구동방식일 때만 같은 차로 봅니다 —
        # "SPORTAGE HYBRID FWD" 나 "OUTLANDER SPORT 4WD" 는 여기서 걸러집니다.
        make_key, model_key = identity
        prefix = f"{model_key} "
        variants = [
            spec
            for (spec_make, spec_model), rows in index.items()
            if spec_make == make_key and spec_model.startswith(prefix)
            for spec in rows
            if set(spec_model[len(prefix):].split()) <= DRIVETRAIN_SUFFIXES
        ]
        if variants:
            return variants, MATCH_DRIVETRAIN
        return [], MATCH_NONE

    @staticmethod
    def _eligibility_index(uber: list[dict], lyft: list[dict]) -> dict:
        """도시 -> 차종 -> 자격 목록. 두 플랫폼을 platform 값으로 구분해 합칩니다."""
        index: dict[str, dict[tuple[str, str], list[dict]]] = {}
        seen: set[tuple] = set()

        for platform, rows in ((PLATFORM_UBER, uber), (PLATFORM_LYFT, lyft)):
            if not rows:
                raise ValueError(f"{platform} 배차 자격 Curated 가 비어 있습니다.")
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
        specs: list[dict],
        match_level: str,
        product: Optional[dict],
    ) -> dict:
        product = product or {}
        mpg_min, mpg_max = _minmax(specs, "combined_mpg")
        kwh_min, kwh_max = _minmax(specs, "combined_kwh_per_100mi")
        range_min, _ = _minmax(specs, "range_miles")
        year_min, year_max = _minmax(specs, "year")

        fuel_types = {_fuel_type(spec.get("atv_type")) for spec in specs}
        if not fuel_types:
            # 제원을 못 붙였으면 연료 구분도 비웁니다. GAS 로 채우면 Gold 가
            # 없는 연비로 에너지비를 계산하려 듭니다.
            fuel_type = None
        elif len(fuel_types) == 1:
            fuel_type = fuel_types.pop()
        else:
            fuel_type = FUEL_TYPE_MIXED

        return {
            "city": city,
            "vendor": vehicle["vendor"],
            "make_key": vehicle["make_key"],
            "model_key": vehicle["model_key"],
            "platform": product.get("platform"),
            "product": product.get("product"),
            "min_year": product.get("min_year"),
            "weekly_lease_fee": vehicle.get("weekly_lease_fee"),
            "image_url": vehicle.get("image_url"),
            "spec_match_level": match_level,
            # 후보 트림 수. 1 이면 값이 확정이고, 여러 개면 아래 범위만큼 불확실합니다.
            "spec_trim_count": len(specs),
            "spec_year_min": year_min,
            "spec_year_max": year_max,
            # 대표 1건이 아니라 범위입니다. 대장에 트림이 없어 어느 값이 맞는지
            # 모르므로, 고르는 것은 Gold 가 합니다 (보수적으로 가려면 min).
            "combined_mpg_min": mpg_min,
            "combined_mpg_max": mpg_max,
            "combined_kwh_per_100mi_min": kwh_min,
            "combined_kwh_per_100mi_max": kwh_max,
            "range_miles_min": range_min,
            "fuel_type": fuel_type,
            # 계보 — 원천 Curated 가 물고 온 bronze_path 를 그대로 옮깁니다.
            # 후보가 여러 개여도 같은 스냅샷에서 왔으므로 첫 행이면 충분합니다.
            "catalog_bronze_path": vehicle.get("bronze_path"),
            "specs_bronze_path": specs[0].get("bronze_path") if specs else None,
            "eligibility_bronze_path": product.get("bronze_path"),
        }
