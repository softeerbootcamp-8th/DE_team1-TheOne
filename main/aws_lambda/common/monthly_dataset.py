"""월별 원천 API의 Parquet 한 종을 Bronze에 보존합니다."""

import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult

from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object


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


class SyntheticDatasetExtractor(Extractor):
    """월별 API에서 요청한 데이터셋의 Parquet 파일만 내려받습니다."""

    name = "synthetic_dataset"

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
        return {
            "year_month": self._response_year_month(response.url),
            "dataset": self._dataset,
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


def _read_parquet(dataset: str, content) -> pq.ParquetFile:
    """content 가 읽을 수 있는 Parquet bytes 인지 확인하고 엽니다."""
    if not isinstance(content, bytes) or not content:
        raise ValueError(f"다운로드한 {dataset} 파일이 비어 있습니다")
    try:
        return pq.ParquetFile(pa.BufferReader(content))
    except (OSError, pa.ArrowInvalid) as exc:
        raise ValueError(f"{dataset} 원본이 읽을 수 있는 Parquet이 아닙니다") from exc


class SyntheticDatasetLoader(Loader):
    """읽을 수 있는 원본 Parquet을 데이터셋별 월 파티션에 로컬로 적재합니다."""

    def __init__(self, base_dir: str, dataset: str, dataset_dir: str):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self.payload: dict = {}
        self.path: Path | None = None

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload["content"]
        parquet = _read_parquet(self._dataset, content)

        self.payload = payload
        self.path = self._data_path(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(self.path, lambda temporary: temporary.write_bytes(content))
        return WriteResult(str(self.path), parquet.metadata.num_rows)

    def _data_path(self, payload: dict) -> Path:
        return (
            self._base_dir
            / self._dataset_dir
            / f"year_month={payload['year_month']}"
            / "data.parquet"
        )


class S3SyntheticDatasetLoader(Loader):
    """읽을 수 있는 원본 Parquet을 데이터셋별 월 파티션에 S3로 적재합니다."""

    def __init__(self, dataset: str, dataset_dir: str, bucket: str | None = None):
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self._bucket = bucket
        self.payload: dict = {}

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload["content"]
        parquet = _read_parquet(self._dataset, content)

        self.payload = payload
        key = self._data_key(payload)
        return S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=content, row_count=parquet.metadata.num_rows)
        )

    def _data_key(self, payload: dict) -> str:
        return f"bronze/{self._dataset_dir}/year_month={payload['year_month']}/data.parquet"


def build_bronze_loader(
    storage: str,
    base_dir: str,
    dataset: str,
    dataset_dir: str,
    bucket: str | None = None,
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return SyntheticDatasetLoader(base_dir, dataset, dataset_dir)
    if storage == "s3":
        return S3SyntheticDatasetLoader(dataset, dataset_dir, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
