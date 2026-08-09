"""AAA New York 정규 휘발유 평균 가격 Raw 수집."""

import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

PAGE_URL = "https://gasprices.aaa.com/?state=NY"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CARD_SELECTOR = "main#maincontent .map-badges .average-price--blue"
PRICE_RE = re.compile(r"\$\s*(\d+(?:\.\d+)?)")
DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2})\b")

# AAA New York 주별 가격 페이지 HTML을 받습니다.
def fetch(timeout: int = 30) -> str:
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text

# New York 평균 가격 카드에서 정규 휘발유 가격과 기준일을 읽습니다.
def parse(html: str) -> dict:
    card = BeautifulSoup(html, "lxml").select_one(CARD_SELECTOR)
    price_element = card.select_one("p.numb") if card else None
    date_element = card.select_one("span") if card else None

    price_match = PRICE_RE.search(price_element.get_text(" ", strip=True)) if price_element else None
    date_match = DATE_RE.search(date_element.get_text(" ", strip=True)) if date_element else None
    if not price_match or not date_match:
        raise RuntimeError("New York 가격 또는 기준일을 찾지 못했습니다 (페이지 구조 변경 의심)")

    return {
        "state": "NY",
        "fuel_type": "regular",
        "price_usd_per_gallon": float(price_match.group(1)),
        "price_date": datetime.strptime(date_match.group(1), "%m/%d/%y").date(),
        "source_url": PAGE_URL,
    }


def extract(timeout: int = 30) -> dict:
    """수집 진입점 — 페이지 요청과 파싱 결과를 반환합니다."""
    return parse(fetch(timeout))
