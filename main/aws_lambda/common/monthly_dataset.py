"""월별 원천 API의 Parquet 한 종을 Bronze 수집 이력으로 보존합니다."""

import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult

from shared.aws_lambda.common.atomic_write import atomic_write
from shared.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object
from shared.common.s3_reader import get_object_bytes, list_keys


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
DATASET_URL_PATTERN = re.compile(
    r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$"
)
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")


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


def _read_parquet(dataset: str, content) -> pq.ParquetFile:
    if not isinstance(content, bytes) or not content:
        raise ValueError(f"다운로드한 {dataset} 파일이 비어 있습니다")
    try:
        return pq.ParquetFile(pa.BufferReader(content))
    except (OSError, pa.ArrowInvalid) as exc:
        raise ValueError(f"{dataset} 원본이 읽을 수 있는 Parquet이 아닙니다") from exc


def _parse_collected_at(payload: dict) -> datetime:
    collected_at = payload.get("collected_at")
    if not isinstance(collected_at, str):
        raise ValueError("collected_at이 누락되었습니다")
    try:
        return datetime.strptime(collected_at, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("collected_at이 UTC 수집 시각 형식이 아닙니다") from exc


def _collected_at_from_name(name: str) -> str:
    timestamp = datetime.strptime(
        PurePosixPath(name).stem, "%Y%m%dT%H%M%S%fZ"
    ).replace(tzinfo=timezone.utc)
    return timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _timestamp_file_name(payload: dict) -> str:
    return f"{_parse_collected_at(payload):%Y%m%dT%H%M%S%fZ}.parquet"


def _same_bytes(left: bytes, right: bytes) -> bool:
    return hashlib.sha256(left).digest() == hashlib.sha256(right).digest()


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
    """로컬 월 파티션에 변경된 원본만 수집 시각 파일로 보존합니다."""

    def __init__(
        self,
        base_dir: str,
        dataset: str,
        dataset_dir: str,
        *,
        dry_run: bool = False,
    ):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self._dry_run = dry_run
        self.payload: dict = {}
        self.path: Path | None = None
        self.source_changed = True

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload.get("content")
        parquet = _read_parquet(self._dataset, content)

        self.source_changed = True
        self.payload = payload
        self.path = self._data_path(payload)
        latest = self._latest_data_path(self.path.parent)
        if latest is not None and self._same_content(latest, content):
            self.source_changed = False
            self.path = latest
            self.payload = {
                **payload,
                "collected_at": _collected_at_from_name(latest.name),
            }
            return WriteResult(str(latest), parquet.metadata.num_rows)

        if self._dry_run:
            if latest is None:
                raise FileNotFoundError(
                    "dry_run은 기존 Bronze 수집본이 있어야 합니다: "
                    f"{self.path.parent}"
                )
            raise ValueError(
                "dry_run 원본이 기존 Bronze와 다릅니다. 변경 원본은 적재 없이 "
                "하류 태스크에 전달할 수 없으므로 정상 실행으로 확인하세요."
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(self.path, lambda temporary: temporary.write_bytes(content))
        return WriteResult(str(self.path), parquet.metadata.num_rows)

    @staticmethod
    def _latest_data_path(partition_dir: Path) -> Path | None:
        return max(
            (
                path
                for path in partition_dir.glob("*.parquet")
                if TIMESTAMP_FILE_PATTERN.fullmatch(path.name)
            ),
            key=lambda path: path.name,
            default=None,
        )

    @staticmethod
    def _same_content(path: Path, content: bytes) -> bool:
        with path.open("rb") as source:
            existing_hash = hashlib.file_digest(source, "sha256").digest()
        return existing_hash == hashlib.sha256(content).digest()

    def _data_path(self, payload: dict) -> Path:
        return (
            self._base_dir
            / self._dataset_dir
            / f"year_month={payload['year_month']}"
            / _timestamp_file_name(payload)
        )


class S3MonthlyParquetBronzeLoader(Loader):
    """S3 월 파티션에 변경된 원본만 수집 시각 객체로 보존합니다."""

    def __init__(
        self,
        dataset: str,
        dataset_dir: str,
        bucket: str | None = None,
        *,
        dry_run: bool = False,
    ):
        load_local_env()
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._dry_run = dry_run
        self.payload: dict = {}
        self.source_changed = True

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload.get("content")
        parquet = _read_parquet(self._dataset, content)

        self.source_changed = True
        self.payload = payload
        prefix = self._partition_prefix(payload)
        latest = self._latest_key(prefix)
        latest_content = (
            get_object_bytes(self._bucket, latest) if latest is not None else None
        )
        if latest is not None and _same_bytes(latest_content, content):
            self.source_changed = False
            self.payload = {
                **payload,
                "collected_at": _collected_at_from_name(latest),
            }
            return WriteResult(
                f"s3://{self._bucket}/{latest}", parquet.metadata.num_rows
            )

        if self._dry_run:
            if latest is None or latest_content is None:
                raise FileNotFoundError(
                    "dry_run은 기존 Bronze 수집본이 있어야 합니다: "
                    f"s3://{self._bucket}/{prefix}"
                )
            raise ValueError(
                "dry_run 원본이 기존 Bronze와 다릅니다. 변경 원본은 적재 없이 "
                "하류 태스크에 전달할 수 없으므로 정상 실행으로 확인하세요."
            )

        key = f"{prefix}{_timestamp_file_name(payload)}"
        return S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=content, row_count=parquet.metadata.num_rows)
        )

    def _partition_prefix(self, payload: dict) -> str:
        return f"bronze/{self._dataset_dir}/year_month={payload['year_month']}/"

    def _latest_key(self, prefix: str) -> str | None:
        return max(
            (
                key
                for key in list_keys(self._bucket, prefix)
                if TIMESTAMP_FILE_PATTERN.fullmatch(PurePosixPath(key).name)
            ),
            default=None,
        )


def build_bronze_loader(
    storage: str,
    base_dir: str,
    dataset: str,
    dataset_dir: str,
    bucket: str | None = None,
    *,
    dry_run: bool = False,
) -> Loader:
    if storage == "local":
        return MonthlyParquetBronzeLoader(
            base_dir,
            dataset,
            dataset_dir,
            dry_run=dry_run,
        )
    if storage == "s3":
        return S3MonthlyParquetBronzeLoader(
            dataset,
            dataset_dir,
            bucket=bucket,
            dry_run=dry_run,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
