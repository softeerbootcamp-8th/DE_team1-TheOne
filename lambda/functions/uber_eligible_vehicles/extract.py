"""Uber Eligible Vehicles 수집(extract).

수집 대상 페이지: https://www.uber.com/us/en/eligible-vehicles/?city=new-york
페이지 HTML 에는 차량 목록이 없고, 클라이언트가 아래 RPC 를 호출해 채웁니다.
그래서 브라우저 없이 이 엔드포인트를 그대로 호출합니다.

응답 형태:
    {"status": "success",
     "data": {"Acura": {"ZDX": "1990 (Courier, ...) / 2018 (Comfort, ...)"}}}

값 문자열은 " / " 로 끊긴 `연식 (상품들)` 묶음입니다. 연식은 "그 상품에 허용되는
가장 오래된 차량 연식" 이라 한 모델이 연식별로 여러 줄을 갖습니다.
(ZDX = 2010 UberX / 2018 Comfort). 원천 적재가 목적이라 전부 남깁니다.
"""

import logging
import re
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

API_URL = "https://www.uber.com/api/getEligibleVehiclesForCity"
PAGE_URL = "https://www.uber.com/us/en/eligible-vehicles/"
# 봇 차단(406)이 걸려서 브라우저와 같은 헤더를 보냅니다. x-csrf-token 은 값 검증을
# 하지 않고 존재 여부만 보기 때문에 프런트엔드와 동일하게 "x" 로 둡니다.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "*/*",
    "x-csrf-token": "x",
    "Origin": "https://www.uber.com",
}

# "2018 (Business Comfort, Comfort Electric, Comfort)"
YEAR_GROUP_RE = re.compile(r"^(\d{4})\s*\((.+)\)$")


def fetch(city_slug: str, timeout: int = 30) -> dict:
    """도시별 차량 목록 원본 JSON 을 받아옵니다."""
    response = requests.post(
        API_URL,
        params={"localeCode": "en"},
        headers={**HEADERS, "Referer": f"{PAGE_URL}?city={city_slug}"},
        json={
            "citySlug": city_slug,
            # 페이지 UI 는 Courier/Shuttle 등을 걸러서 보여주지만
            # 원천 적재가 목적이라 아무것도 제외하지 않습니다.
            "makesToExclude": "",
            "modelsToExclude": "",
            "productsToExclude": "",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("status") != "success":
        raise RuntimeError(f"예상치 못한 응답 status: {payload.get('status')!r}")
    data = payload.get("data")
    if not isinstance(data, dict) or not data:
        raise RuntimeError("응답에 차량 데이터가 비어 있습니다 (API 스펙 변경 의심)")
    return data


def parse(data: dict, city_slug: str, collected_at: datetime) -> list[dict]:
    """{제조사: {모델: 원문}} 을 연식 그룹 단위 행으로 펼칩니다."""
    rows: list[dict] = []
    skipped = 0

    for make, models in data.items():
        for model, raw in models.items():
            for chunk in raw.split(" / "):
                matched = YEAR_GROUP_RE.match(chunk.strip())
                if not matched:
                    # 형식이 바뀌면 여기서 새기 시작하므로 개수를 남깁니다.
                    skipped += 1
                    logger.warning("파싱 실패: %s %s -> %r", make, model, chunk)
                    continue
                year, products = matched.groups()
                rows.append(
                    {
                        "city_slug": city_slug,
                        "make": make,
                        "model": model,
                        "min_year": int(year),
                        "products": [p.strip() for p in products.split(",")],
                        "raw_eligibility": raw,
                        "collected_at": collected_at,
                    }
                )

    if not rows:
        raise RuntimeError("파싱 결과가 0건입니다 (페이지 구조 변경 의심)")
    logger.info("파싱 완료: %d행 (실패 %d건)", len(rows), skipped)
    return rows


def extract(city_slug: str, collected_at: datetime) -> list[dict]:
    """수집 진입점 — 호출 + 파싱을 묶어 행 목록을 돌려줍니다."""
    logger.info("수집 시작: city=%s", city_slug)
    return parse(fetch(city_slug), city_slug, collected_at)
