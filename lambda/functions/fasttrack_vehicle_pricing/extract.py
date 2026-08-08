"""Fast Track Leasing 렌탈 차량 수집(extract).

수집 대상 페이지: https://fasttrackleasingllc.com/vehicles-pricing/
WordPress(Elementor) 로 만든 정적 페이지라 HTML 을 그대로 파싱합니다.

차량 카드 한 장은 이미지 + "Book Now!" 버튼이 전부이고, **차종과 가격이 모두
카드 이미지 안에 그려져 있습니다.** HTML 에는 텍스트로 존재하지 않아 OCR 로 읽습니다.
(브라우저에서 개발자도구를 열면 가격 텍스트가 보이는데, 그건 크롬의
"이미지에서 텍스트 복사" 기능이 만든 오버레이라 페이지 DOM 이 아닙니다.)

이미지 레이아웃은 12장 모두 동일합니다.
    상단 25%  : 모델명(대문자) + 제조사      예) "OUTLANDER SPORT" / "Mitsubishi"
    하단 30%  : "STARTING FROM" + 가격      예) "$554.00"

제조사/모델은 HTML 의 img alt 보다 이미지 쪽이 정확해서 이미지를 우선합니다.
alt 는 "Toyota Outlander Sport"(제조사 오기) 나 빈 값(Voyager)이 섞여 있는데,
이미지는 "Mitsubishi" / "Chrysler" 로 정확합니다. 다만 원문 추적을 위해
alt 값은 raw_name 으로 함께 남깁니다.

모델명은 카드 디자인이 전부 대문자라 "RAV4" 처럼 대문자로 들어옵니다.
Uber 쪽 데이터와 조인할 때의 표기 정규화는 실버 단계에서 합니다.
"""

import io
import logging
import re
from datetime import datetime
from urllib.parse import urljoin

import pytesseract
import requests
from bs4 import BeautifulSoup
from PIL import Image

logger = logging.getLogger(__name__)

VENDOR = "fasttrack"
PAGE_URL = "https://fasttrackleasingllc.com/vehicles-pricing/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# 차량 카드 이미지만 골라냅니다. 로고/국기 등은 uploads 경로가 아니거나 main 밖입니다.
CARD_IMG_SELECTOR = "main .elementor-widget-image img[src*='/wp-content/uploads/']"

# 카드 이미지에서 잘라낼 영역 (세로 비율). 12장 모두 같은 템플릿입니다.
# 모델명/제조사는 한 줄씩 따로 자릅니다. 두 줄을 한 번에 넣으면 tesseract 가
# 짧은 줄("Kia")을 통째로 버리는데, 이미지 크기에 따라 결과가 달라져
# (900px 는 되고 768/1080px 는 실패) 재현이 안 됩니다. 줄 단위로 자른 뒤
# 단일 행 모드(psm 7)로 읽으면 세그멘테이션이 개입하지 않아 안정적입니다.
MODEL_BOX = (0.02, 0.15)  # 모델명 (대문자)
MAKE_BOX = (0.15, 0.25)  # 제조사
PRICE_BOX = (0.70, 1.00)  # "STARTING FROM" + 가격 (두 줄이라 자동 모드)

PSM_SINGLE_LINE = "--psm 7"

PRICE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d{2})?)")

# 흑백 이진화 임계값. 카드가 회색 그라데이션 배경이라 그냥 넣으면 짧은 단어를
# 놓칩니다("Kia" 미인식). 흑백으로 눌러주면 12장 모두 안정적으로 읽힙니다.
BINARY_THRESHOLD = 140


def fetch(timeout: int = 30) -> str:
    """차량/가격 페이지 HTML 을 받아옵니다."""
    response = requests.get(PAGE_URL, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_cards(html: str) -> list[dict]:
    """HTML 에서 카드별 이미지/예약 링크와 alt 원문을 뽑습니다."""
    soup = BeautifulSoup(html, "lxml")
    cards: list[dict] = []

    for img in soup.select(CARD_IMG_SELECTOR):
        column = img.find_parent(class_="elementor-column")
        booking = column.select_one("a.elementor-button") if column else None
        cards.append(
            {
                "image_url": urljoin(PAGE_URL, img["src"]),
                "booking_url": urljoin(PAGE_URL, booking["href"]) if booking else None,
                "raw_name": (img.get("alt") or "").strip() or None,
            }
        )

    if not cards:
        raise RuntimeError("차량 카드를 찾지 못했습니다 (페이지 구조 변경 의심)")
    return cards


def _read_box(image: Image.Image, box: tuple[float, float], config: str = "") -> str:
    """이미지의 세로 구간 하나를 잘라 흑백으로 눌러 OCR 합니다."""
    width, height = image.size
    top, bottom = box
    crop = image.crop((0, int(height * top), width, int(height * bottom)))
    binarized = crop.convert("L").point(lambda v: 0 if v < BINARY_THRESHOLD else 255)
    return pytesseract.image_to_string(binarized, config=config).strip()


def ocr_card(image_bytes: bytes) -> dict:
    """카드 이미지에서 제조사/모델/가격을 읽습니다. 못 읽은 값은 None 입니다."""
    image = Image.open(io.BytesIO(image_bytes))

    model = _read_box(image, MODEL_BOX, PSM_SINGLE_LINE)
    make = _read_box(image, MAKE_BOX, PSM_SINGLE_LINE)
    matched = PRICE_RE.search(_read_box(image, PRICE_BOX))

    return {
        "make": make or None,
        "model": model or None,
        "price_usd": float(matched.group(1).replace(",", "")) if matched else None,
    }


def extract(collected_at: datetime, timeout: int = 30) -> list[dict]:
    """수집 진입점 — 페이지 파싱 후 카드 이미지를 OCR 해 행 목록을 만듭니다."""
    logger.info("수집 시작: %s", PAGE_URL)
    cards = parse_cards(fetch(timeout))
    rows: list[dict] = []

    for card in cards:
        image_bytes = requests.get(card["image_url"], headers=HEADERS, timeout=timeout).content
        try:
            read = ocr_card(image_bytes)
        except pytesseract.TesseractNotFoundError as exc:
            raise RuntimeError(
                "tesseract 바이너리를 찾을 수 없습니다. "
                "설치 방법은 docs/GETTING_STARTED.md 를 참고하세요 (macOS: brew install tesseract)."
            ) from exc

        # 한 장이 안 읽혀도 나머지는 살립니다. 값은 None 으로 남습니다.
        if read["price_usd"] is None:
            logger.warning("가격 OCR 실패: %s", card["image_url"])
        if read["model"] is None:
            logger.warning("차종 OCR 실패: %s", card["image_url"])

        rows.append(
            {
                "vendor": VENDOR,
                **read,
                "raw_name": card["raw_name"],  # HTML img alt 원문
                "price_period": "week",  # 페이지 제목의 "Weekly ... Pricing Guide" 기준
                "image_url": card["image_url"],  # 가격이 바뀌면 이 URL 이 바뀜
                "booking_url": card["booking_url"],
                "source_url": PAGE_URL,
                "collected_at": collected_at,
            }
        )

    priced = sum(1 for r in rows if r["price_usd"] is not None)
    logger.info("파싱 완료: %d대 (가격 확보 %d대)", len(rows), priced)
    return rows
