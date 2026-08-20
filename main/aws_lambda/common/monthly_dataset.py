"""월별 원천 API의 Parquet 한 종을 Bronze에 보존합니다."""

import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult

from shared.aws_lambda.common.atomic_write import atomic_write


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
DATASET_URL_PATTERN = re.compile(
    r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$"
)


def requested_year_month(event: dict) -> str | None:
    year, month = event.get("year"), event.get("month")
    if bool(year) != bool(month):
        raise ValueError("year와 month는 함께 지정해야 합니다")
    if not year:
        return None
    value = f"{str(year).strip()}-{str(month).strip().zfill(2)}"
    if not YEAR_MONTH_PATTERN.fullmatch(value):
        raise ValueError("year와 month가 유효한 YYYY-MM 형식이 아닙니다")
    try:
        datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ValueError("year와 month가 유효한 YYYY-MM 형식이 아닙니다") from exc
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MonthlyParquetAPIExtractor(Extractor):
    """월별 API에서 요청한 데이터셋의 Parquet 파일만 내려받습니다."""

    name = "monthly_parquet_api"

    def __init__(
        self,
        api_base_url: str,
        dataset: str,
        year_month: str | None,
        *,
        timeout: int = 180,
    ):
        self._api_base_url = api_base_url.rstrip("/")
        self._dataset = dataset
        self._year_month = year_month
        self._timeout = timeout

    def extract(self) -> dict:
        year_month = self._year_month or "latest"
        endpoint = f"v1/data/{year_month}/datasets/{self._dataset}"
        response = requests.get(
            urljoin(f"{self._api_base_url}/", endpoint), timeout=self._timeout
        )
        response.raise_for_status()
        collected_at = (
            _utc_now()
            .astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
        return {
            "year_month": self._response_year_month(response.url),
            "dataset": self._dataset,
            "collected_at": collected_at,
            "content": response.content,
        }

    def _response_year_month(self, response_url: str) -> str:
        base, target = urlsplit(self._api_base_url), urlsplit(response_url)
        if (target.scheme, target.netloc) != (base.scheme, base.netloc):
            raise ValueError("데이터셋 응답 URL은 API와 같은 host여야 합니다")
        match = DATASET_URL_PATTERN.fullmatch(target.path.rstrip("/"))
        if not match or match.group(2) != self._dataset:
            raise ValueError(f"데이터셋 응답 URL이 올바르지 않습니다: {response_url}")
        year_month = match.group(1)
        try:
            datetime.strptime(year_month, "%Y-%m")
        except ValueError as exc:
            raise ValueError("데이터셋 응답 URL의 월이 유효하지 않습니다") from exc
        if self._year_month and year_month != self._year_month:
            raise ValueError(
                f"요청 월과 응답 월이 다릅니다: {self._year_month} != {year_month}"
            )
        return year_month


class MonthlyParquetBronzeLoader(Loader):
    """읽을 수 있는 월별 원본 Parquet을 수집 시각 파일로 보존합니다."""

    def __init__(self, base_dir: str, dataset: str, dataset_dir: str):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self.payload: dict = {}
        self.path: Path | None = None

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload.get("content")
        if not isinstance(content, bytes) or not content:
            raise ValueError(f"다운로드한 {self._dataset} 파일이 비어 있습니다")
        try:
            parquet = pq.ParquetFile(pa.BufferReader(content))
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(f"{self._dataset} 원본이 읽을 수 있는 Parquet이 아닙니다") from exc

        self.payload = payload
        self.path = self._data_path(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(self.path, lambda temporary: temporary.write_bytes(content))
        return WriteResult(str(self.path), parquet.metadata.num_rows)

    def _data_path(self, payload: dict) -> Path:
        collected_at = payload.get("collected_at")
        if not isinstance(collected_at, str):
            raise ValueError("collected_at이 누락되었습니다")
        try:
            timestamp = datetime.strptime(
                collected_at, "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError("collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
        return (
            self._base_dir
            / self._dataset_dir
            / f"year_month={payload['year_month']}"
            / f"{timestamp:%Y%m%dT%H%M%S%fZ}.parquet"
        )
