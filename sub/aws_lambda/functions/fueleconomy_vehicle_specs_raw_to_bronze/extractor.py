"""fueleconomy.gov 차종별 제원 수집(extract).

수집 대상: https://www.fueleconomy.gov/feg/epadata/vehicles.csv
미국 EPA/DOE 가 공개하는 전 차종 제원 벌크 CSV 입니다. 1984~현재 약 50,000행,
84개 컬럼이고 별도의 인증이나 API 키가 없습니다.

차종당 한 번씩 API 를 호출하는 방식(ws/rest/vehicle/...)도 있지만, 전 차종을
받으려면 수만 번 호출해야 합니다. 한 달에 한 번 전량 스냅샷을 뜨는 용도라
벌크 CSV 를 그대로 받는 편이 훨씬 쌉니다.

원본 컬럼을 하나도 버리지 않습니다. 이 CSV 는 계속 갱신되는 파일이라
과거 스냅샷이 어디에도 남지 않습니다. 지금 안 쓰는 컬럼을 빼두면 나중에
필요해져도 그 시점 값을 되살릴 방법이 없습니다. 그래서 컬럼명도 원본
그대로 두고 값도 문자열로 싣습니다. 타입 변환과 컬럼 선별은 실버 단계에서
합니다.

이번 이슈에서 쓸 필드의 의미(전량 중 일부):
    year, make, model    조인 키
    baseModel            구동방식 접미사가 빠진 모델명 (Equinox AWD -> Equinox)
    comb08               연비 — MPG. 전기차는 MPGe(휘발유 환산) 라 전비와 단위가 다릅니다.
    combE                전비 — kWh/100mi. 내연기관은 0 으로 채워져 옵니다.
    range                주행거리 — mile. 전기차/PHEV 만 값이 있고 내연기관은 0 입니다.
    atvType              EV / Plug-in Hybrid / Hybrid ...
"""

import csv
import io
import logging
from datetime import datetime

import requests
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

SOURCE = "fueleconomy.gov"
CSV_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,*/*",
}
MAX_ATTEMPTS = 3

# 이게 없으면 이 데이터를 쓰는 의미가 없는 컬럼들. 사라지면 실패시킵니다.
REQUIRED_COLUMNS = ("id", "year", "make", "model", "baseModel", "comb08", "combE", "range")

# 수집 시 값 대신 붙이는 메타 컬럼. 원본에 같은 이름이 생기면 덮어쓰게 되므로
# 아래 parse 에서 충돌을 확인합니다.
META_COLUMNS = ("source", "collected_at")


def fetch(timeout: int = 120) -> str:
    """벌크 CSV 를 받아 문자열로 돌려줍니다.

    20MB 를 통째로 메모리에 올리고, 이후 dict 5만 개로 펼쳐지면서 수백 MB 를 씁니다.
    한 달에 한 번 도는 작업이라 그대로 뒀습니다. 메모리가 빠듯한 곳에서
    돌릴 일이 생기면 청크 처리로 바꿔야 합니다.
    """
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(CSV_URL, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            response.encoding = "utf-8"
            text = response.text
            logger.info("CSV 수신: %.1fMB", len(text) / 1024 / 1024)
            return text
        except requests.RequestException as exc:
            # 20MB 라 중간에 끊기는 일이 잦습니다. 끊기면 처음부터 다시 받습니다.
            last_error = exc
            logger.warning("CSV 수신 실패 (%d/%d): %s", attempt, MAX_ATTEMPTS, exc)

    raise RuntimeError(f"CSV 를 {MAX_ATTEMPTS}회 시도했지만 받지 못했습니다") from last_error


def parse(text: str, collected_at: datetime) -> list[dict]:
    """CSV 를 원본 컬럼 그대로 행 목록으로 만듭니다. 값은 전부 문자열입니다."""
    reader = csv.DictReader(io.StringIO(text))
    columns = list(reader.fieldnames or [])

    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise RuntimeError(f"원본 CSV 에 필수 컬럼이 없습니다: {missing} (스키마 변경 의심)")

    collides = [c for c in META_COLUMNS if c in columns]
    if collides:
        raise RuntimeError(f"원본 컬럼과 메타 컬럼 이름이 충돌합니다: {collides}")

    rows = [
        # 빈 칸은 None. 그 외에는 원본 문자열을 그대로 둡니다.
        {c: (raw.get(c) or "").strip() or None for c in columns}
        | {"source": SOURCE, "collected_at": collected_at}
        for raw in reader
    ]

    if not rows:
        raise RuntimeError("파싱 결과가 0건입니다 (원본 구조 변경 의심)")
    logger.info("파싱 완료: %d행 / 원본 %d컬럼", len(rows), len(columns))
    return rows


class VehicleSpecsExtractor(Extractor):
    """fueleconomy.gov 벌크 CSV 를 받아 원본 컬럼 그대로 행 목록을 만듭니다."""

    name = "fueleconomy_vehicle_specs"

    def __init__(self, collected_at: datetime, timeout: int = 120):
        self._collected_at = collected_at
        self._timeout = timeout

    def extract(self) -> list[dict]:
        logger.info("수집 시작: %s", CSV_URL)
        return parse(fetch(self._timeout), self._collected_at)
