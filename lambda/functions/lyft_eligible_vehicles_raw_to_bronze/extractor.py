"""Lyft 뉴욕시 Premium Eligible Vehicles 수집(extract).

수집 대상: https://www.lyft.com/driver/eligible-premium-vehicles

차량 목록은 화면 HTML이 아니라 Next.js의 ``__NEXT_DATA__`` JSON 안에 있습니다.
제조사별 원문은 ``MODEL - 최소 연식 (등급)`` 형태이며, Uber 수집기처럼
최소 연식/등급 묶음 하나를 한 행으로 펼칩니다. 원문에 연식이 없는 XXL
차량은 값을 추측하지 않고 ``min_year=None``으로 남깁니다.

일부 제조사는 지역별 목록이 다르므로 New York City 지역 코드인 ``NYC``
목록을 선택합니다. Standard 차량은 공식 Premium 목록에 없으므로 수집하지
않습니다.
"""

import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

PAGE_URL = "https://www.lyft.com/driver/eligible-premium-vehicles"
CITY_SLUG = "new-york"
REGION_CODE = "NYC"
VEHICLE_FAQ_NAME = "Driver > HVM Eligible Vehicle List"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

MODEL_LINE_RE = re.compile(r"^__(?P<model>.+?)__\s*-\s*(?P<eligibility>.*?)\s*$")
YEAR_GROUP_RE = re.compile(
    r"^(?P<year>\d{4})\s*\((?P<ride_types>.*?)(?:\))?\s*$"
)
NO_YEAR_GROUP_RE = re.compile(r"^\((?P<ride_types>.*?)\)\s*$")
SUP_TAG_RE = re.compile(r"<sup\b[^>]*>.*?</sup>", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")

# 긴 이름을 먼저 확인해 Black SUV -> Black, XXL -> XL로 잘못 읽는 것을 막습니다.
RIDE_TYPE_NAMES = ("Extra Comfort", "Black SUV", "Black", "XXL", "XL")


def fetch(timeout: int = 30) -> dict:
    """공식 페이지에서 차량 목록이 든 Next.js 페이지 데이터를 받습니다."""
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=timeout)
    response.raise_for_status()

    if not response.text.strip():
        raise RuntimeError("Lyft 차량 페이지 응답이 비어 있습니다")

    script = BeautifulSoup(response.text, "lxml").select_one("script#__NEXT_DATA__")
    if script is None or not script.string:
        raise RuntimeError(
            "__NEXT_DATA__를 찾지 못했습니다 (페이지 구조 변경 의심)"
        )

    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("__NEXT_DATA__ JSON 파싱에 실패했습니다") from exc

    page_data = payload.get("props", {}).get("pageProps", {}).get("brandPageData")
    if not isinstance(page_data, dict) or not page_data:
        raise RuntimeError(
            "응답에 차량 페이지 데이터가 없습니다 (페이지 구조 변경 의심)"
        )

    logger.info("Lyft 차량 페이지 수신 완료: bytes=%d", len(response.content))
    return page_data


def _walk_components(value: object) -> Iterator[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_components(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_components(child)


def _vehicle_entries(page_data: dict, region_code: str) -> list[dict]:
    faqs = [
        component
        for component in _walk_components(page_data)
        if component.get("componentType") == "FAQ"
        and component.get("displayName") == VEHICLE_FAQ_NAME
    ]
    if len(faqs) != 1:
        raise RuntimeError(
            f"차량 FAQ를 정확히 하나 찾지 못했습니다: count={len(faqs)} "
            "(페이지 구조 변경 의심)"
        )

    raw_entries = faqs[0].get("entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise RuntimeError("차량 FAQ가 비어 있습니다 (페이지 구조 변경 의심)")

    selected: list[dict] = []
    for entry in raw_entries:
        component_type = entry.get("componentType")
        if component_type == "FAQEntry":
            selected.append(entry)
            continue
        if component_type != "VisibilityToggleList":
            raise RuntimeError(f"알 수 없는 차량 FAQ 구성요소: {component_type!r}")

        toggles = entry.get("listOfVisibilityToggles")
        if not isinstance(toggles, list) or not toggles:
            raise RuntimeError(
                "지역별 차량 목록이 비어 있습니다 (페이지 구조 변경 의심)"
            )

        matches = [toggle for toggle in toggles if region_code in toggle.get("regions", [])]
        if not matches:
            matches = [toggle for toggle in toggles if not toggle.get("regions")]
        if len(matches) != 1:
            raise RuntimeError(
                f"지역별 차량 목록을 정확히 하나 선택하지 못했습니다: "
                f"region={region_code} count={len(matches)}"
            )

        components = matches[0].get("componentList")
        regional_entries = [
            component
            for component in components or []
            if component.get("componentType") == "FAQEntry"
        ]
        if not regional_entries:
            raise RuntimeError(
                f"선택한 지역 차량 목록이 비어 있습니다: region={region_code}"
            )
        selected.extend(regional_entries)

    return selected


def _clean_eligibility(raw: str) -> str:
    # 각주 번호는 등급 정보가 아니므로 태그 내용까지 제거합니다.
    return HTML_TAG_RE.sub("", SUP_TAG_RE.sub("", raw)).strip()


def _normalize_ride_types(raw: str) -> list[str]:
    ride_types: list[str] = []
    for token in raw.split(","):
        matched = next((name for name in RIDE_TYPE_NAMES if name in token), None)
        if matched and matched not in ride_types:
            ride_types.append(matched)
    return ride_types


def parse(page_data: dict, city_slug: str, collected_at: datetime) -> list[dict]:
    """NYC 제조사 목록을 최소 연식/등급 묶음 단위의 행으로 펼칩니다."""
    if city_slug != CITY_SLUG:
        raise ValueError(f"지원하지 않는 Lyft 도시입니다: {city_slug!r}")

    rows: list[dict] = []
    skipped = 0

    for entry in _vehicle_entries(page_data, REGION_CODE):
        make = str(entry.get("question") or "").strip()
        answer = entry.get("answer")
        if not make or not isinstance(answer, str) or not answer.strip():
            skipped += 1
            logger.warning("제조사 차량 목록 파싱 실패: make=%r", make)
            continue

        for raw_line in answer.splitlines():
            raw_line = raw_line.strip()
            if not raw_line or raw_line.startswith("*"):
                continue

            model_match = MODEL_LINE_RE.match(raw_line)
            if not model_match:
                skipped += 1
                logger.warning("차량 행 파싱 실패: make=%s raw=%r", make, raw_line)
                continue

            model = model_match.group("model").strip()
            raw_eligibility = model_match.group("eligibility").strip()
            eligibility = _clean_eligibility(raw_eligibility)

            for chunk in eligibility.split(" / "):
                chunk = chunk.strip()
                year_match = YEAR_GROUP_RE.match(chunk)
                no_year_match = NO_YEAR_GROUP_RE.match(chunk)

                if year_match:
                    min_year = int(year_match.group("year"))
                    raw_ride_types = year_match.group("ride_types")
                elif no_year_match:
                    min_year = None
                    raw_ride_types = no_year_match.group("ride_types")
                else:
                    skipped += 1
                    logger.warning(
                        "등급 묶음 파싱 실패: make=%s model=%s raw=%r",
                        make,
                        model,
                        chunk,
                    )
                    continue

                ride_types = _normalize_ride_types(raw_ride_types)
                if not ride_types:
                    skipped += 1
                    logger.warning(
                        "알 수 없는 Lyft 등급: make=%s model=%s raw=%r",
                        make,
                        model,
                        raw_ride_types,
                    )
                    continue

                rows.append(
                    {
                        "city_slug": city_slug,
                        "make": make,
                        "model": model,
                        "min_year": min_year,
                        "ride_types": ride_types,
                        "raw_eligibility": raw_eligibility,
                        "source_url": PAGE_URL,
                        "collected_at": collected_at,
                    }
                )

    if not rows:
        raise RuntimeError(
            "Lyft 차량 파싱 결과가 0건입니다 (페이지 구조 변경 의심)"
        )

    logger.info(
        "Lyft 차량 파싱 완료: city=%s region=%s rows=%d skipped=%d",
        city_slug,
        REGION_CODE,
        len(rows),
        skipped,
    )
    return rows


def extract(
    city_slug: str,
    collected_at: datetime,
    timeout: int = 30,
) -> list[dict]:
    """수집 진입점 — 공식 페이지 호출과 NYC 차량 파싱을 수행합니다."""
    logger.info("Lyft 차량 수집 시작: city=%s url=%s", city_slug, PAGE_URL)
    return parse(fetch(timeout), city_slug, collected_at)


class LyftEligibleVehiclesExtractor(Extractor):
    """Lyft 공식 페이지에서 NYC Premium 대상 차량을 수집합니다."""

    name = "lyft_eligible_vehicles"

    def __init__(
        self,
        city_slug: str,
        collected_at: datetime,
        timeout: int = 30,
    ):
        self._city_slug = city_slug
        self._collected_at = collected_at
        self._timeout = timeout

    def extract(self) -> list[dict]:
        return extract(self._city_slug, self._collected_at, self._timeout)
