"""Lyft 공식 페이지에서 NYC Premium 대상 차량 원문을 수집합니다.

페이지 전체 HTML은 저장하지 않고 차량 FAQ에 있는 제조사·차량 행만 가져옵니다.
등급명은 선별하지 않지만, ``min_year``는 해당 상품의 최소 허용 연식이므로
4자리 연식이 명시되지 않은 묶음은 Uber 수집기와 동일하게 건너뜁니다.
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
    r"^(?P<year>\d{4})\s*\((?P<products>.+)\)\s*$"
)
SUP_TAG_RE = re.compile(r"<sup\b[^>]*>.*?</sup>", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")


def fetch(timeout: int = 30) -> dict:
    """공식 페이지에서 차량 목록이 든 Next.js 데이터를 받습니다."""
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=timeout)
    response.raise_for_status()

    script = BeautifulSoup(response.text, "lxml").select_one("script#__NEXT_DATA__")
    if script is None or not script.string:
        raise RuntimeError("Lyft 차량 페이지에서 __NEXT_DATA__를 찾지 못했습니다")

    try:
        payload = json.loads(script.string)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Lyft __NEXT_DATA__ JSON을 읽지 못했습니다") from exc

    page_data = payload.get("props", {}).get("pageProps", {}).get("brandPageData")
    if not isinstance(page_data, dict):
        raise RuntimeError("Lyft 차량 페이지 데이터를 찾지 못했습니다")
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
    """차량 FAQ 중 대상 지역에 표시되는 차량 항목만 선택합니다."""
    faq = next(
        (
            component
            for component in _walk_components(page_data)
            if component.get("componentType") == "FAQ"
            and component.get("displayName") == VEHICLE_FAQ_NAME
        ),
        None,
    )
    if faq is None:
        return []

    selected: list[dict] = []
    for entry in faq.get("entries") or []:
        if not isinstance(entry, dict):
            continue
        if entry.get("componentType") == "FAQEntry":
            selected.append(entry)
            continue
        if entry.get("componentType") != "VisibilityToggleList":
            continue

        toggles = [
            toggle
            for toggle in entry.get("listOfVisibilityToggles") or []
            if isinstance(toggle, dict)
        ]
        regional = next(
            (toggle for toggle in toggles if region_code in toggle.get("regions", [])),
            None,
        )
        fallback = next((toggle for toggle in toggles if not toggle.get("regions")), None)
        chosen = regional or fallback
        if chosen is None:
            continue

        selected.extend(
            component
            for component in chosen.get("componentList") or []
            if isinstance(component, dict)
            and component.get("componentType") == "FAQEntry"
        )

    return selected


def _clean_text(raw: str) -> str:
    return HTML_TAG_RE.sub("", SUP_TAG_RE.sub("", raw)).strip()


def _products(raw: str) -> list[str]:
    """공식 페이지의 등급 표기를 선별하지 않고 쉼표 단위로만 나눕니다."""
    return [product.strip() for product in raw.split(",") if product.strip()]


def _row(
    *,
    city_slug: str,
    make: str | None,
    model: str | None,
    min_year: int,
    products: list[str],
    raw_eligibility: str,
    raw_vehicle: str,
    collected_at: datetime,
) -> dict:
    return {
        "city_slug": city_slug,
        "make": make,
        "model": model,
        "min_year": min_year,
        "products": products,
        "raw_eligibility": raw_eligibility,
        "raw_vehicle": raw_vehicle,
        "source_url": PAGE_URL,
        "collected_at": collected_at,
    }


def parse(page_data: dict, city_slug: str, collected_at: datetime) -> list[dict]:
    """NYC 차량 행을 연식/상품 묶음 단위로 펼칩니다."""
    if city_slug != CITY_SLUG:
        raise ValueError(f"지원하지 않는 Lyft 도시입니다: {city_slug!r}")

    rows: list[dict] = []
    skipped = 0
    for entry in _vehicle_entries(page_data, REGION_CODE):
        make = str(entry.get("question") or "").strip() or None
        answer = entry.get("answer")
        raw_lines = answer.splitlines() if isinstance(answer, str) else [str(answer)]

        for raw_vehicle in raw_lines:
            raw_vehicle = raw_vehicle.strip()
            if not raw_vehicle:
                continue

            matched = MODEL_LINE_RE.match(raw_vehicle)
            if not matched:
                skipped += 1
                logger.warning(
                    "lyft_parse skipped reason=vehicle_format make=%r raw=%r",
                    make,
                    raw_vehicle,
                )
                continue

            model = matched.group("model").strip() or None
            raw_eligibility = matched.group("eligibility").strip()
            chunks = _clean_text(raw_eligibility).split(" / ")

            for chunk in chunks:
                chunk = chunk.strip()
                year_match = YEAR_GROUP_RE.match(chunk)
                if not year_match:
                    skipped += 1
                    logger.warning(
                        "lyft_parse skipped reason=missing_min_year "
                        "make=%r model=%r raw=%r",
                        make,
                        model,
                        chunk,
                    )
                    continue

                min_year = int(year_match.group("year"))
                products = _products(year_match.group("products"))

                rows.append(
                    _row(
                        city_slug=city_slug,
                        make=make,
                        model=model,
                        min_year=min_year,
                        products=products,
                        raw_eligibility=raw_eligibility,
                        raw_vehicle=raw_vehicle,
                        collected_at=collected_at,
                    )
                )

    if not rows:
        raise RuntimeError("Lyft 차량 파싱 결과가 0건입니다")

    logger.info("lyft_extract done rows=%d skipped=%d", len(rows), skipped)
    return rows


class LyftEligibleVehiclesExtractor(Extractor):
    """Lyft 공식 페이지에서 NYC Premium 대상 차량 행을 수집합니다."""

    name = "lyft_eligible_vehicles"

    def __init__(self, city_slug: str, collected_at: datetime, timeout: int = 30):
        self._city_slug = city_slug
        self._collected_at = collected_at
        self._timeout = timeout

    def extract(self) -> list[dict]:
        return parse(fetch(self._timeout), self._city_slug, self._collected_at)
