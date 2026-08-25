"""월별 원천 API의 Parquet 한 종을 Bronze 수집 이력으로 보존합니다."""

import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult

from shared.aws_lambda.common.atomic_write import atomic_write, invalidate_success_marker
from shared.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object
from shared.common.bronze_manifest import (
    MANIFEST_FILE_NAME,
    bronze_manifest_bytes,
    build_bronze_manifest,
)
from shared.common.s3_reader import get_object_bytes, list_keys
from shared.common.success_marker import data_key_is_complete, data_path_is_complete
BRONZE_DATA_FILE_NAME = "data.parquet"
COLLECTED_AT_DIR_PATTERN = re.compile(r"^collected_at=(\d{8}T\d{12}Z)$")
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")

YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
DATASET_URL_PATTERN = re.compile(
    r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$"
)
SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def service_area_segment(service_area: str) -> str:
    if not SERVICE_AREA_PATTERN.fullmatch(service_area):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return f"service_area={service_area}"


def join_segments(*segments: str | None) -> str:
    return "/".join(segment for segment in segments if segment)


def service_area_root(root: str | Path, service_area: str) -> Path:
    return Path(root) / service_area_segment(service_area)


def service_area_prefix(*head: str, service_area: str) -> str:
    return join_segments(*head, service_area_segment(service_area))


def collected_at_token(value: str) -> str:
    try:
        timestamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
    return f"{timestamp:%Y%m%dT%H%M%S%fZ}"


def collected_at_from_token(token: str) -> str:
    try:
        timestamp = datetime.strptime(token, "%Y%m%dT%H%M%S%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("Bronze 경로의 수집 시각이 올바르지 않습니다") from exc
    return timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")


def bronze_collection_token(path) -> str | None:
    if TIMESTAMP_FILE_PATTERN.fullmatch(path.name):
        return Path(path.name).stem
    if path.name != BRONZE_DATA_FILE_NAME:
        return None
    match = COLLECTED_AT_DIR_PATTERN.fullmatch(path.parent.name)
    return match.group(1) if match else None


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


def _collected_at_dir_name(payload: dict) -> str:
    return f"collected_at={collected_at_token(payload.get('collected_at'))}"


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
        service_area: str | None = None,
        timeout: int = 180,
    ):
        self._api_base_url = api_base_url.rstrip("/")
        self._dataset = dataset
        self._year_month = year_month
        self._service_area = service_area
        service_area_segment(service_area)
        self._timeout = timeout

    def extract(self) -> dict:
        year_month = self._year_month or "latest"
        endpoint = f"v1/data/{year_month}/datasets/{self._dataset}"
        params = {"service_area": self._service_area} if self._service_area else None
        response = requests.get(
            urljoin(f"{self._api_base_url}/", endpoint),
            params=params,
            timeout=self._timeout,
        )
        response.raise_for_status()
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        if not etag or not last_modified:
            raise ValueError(
                f"{self._dataset} GET 응답에 ETag 또는 Last-Modified가 없습니다"
            )
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
            "sha256": hashlib.sha256(response.content).hexdigest(),
            "api_base_url": self._api_base_url,
            "source_etag": etag,
            "source_last_modified": last_modified,
        }

    def _response_year_month(self, response_url: str) -> str:
        base, target = urlsplit(self._api_base_url), urlsplit(response_url)
        if (target.scheme, target.netloc) != (base.scheme, base.netloc):
            raise ValueError("데이터셋 응답 URL은 API와 같은 host여야 합니다")
        match = DATASET_URL_PATTERN.fullmatch(target.path.rstrip("/"))
        if not match or match.group(2) != self._dataset:
            raise ValueError(f"데이터셋 응답 URL이 올바르지 않습니다: {response_url}")
        if self._service_area:
            areas = parse_qs(target.query).get("service_area")
            if areas != [self._service_area]:
                raise ValueError(
                    "데이터셋 응답 URL의 service_area가 요청과 다릅니다: "
                    f"{response_url}"
                )
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
    """로컬 월 파티션에 변경된 원본만 수집 시각 디렉터리로 보존합니다."""

    def __init__(
        self,
        base_dir: str,
        dataset: str,
        dataset_dir: str,
        service_area: str,
    ):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self._service_area = service_area
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
        latest = self._latest_data_path(self.path.parent.parent)
        if (
            latest is not None
            and latest.name == BRONZE_DATA_FILE_NAME
            and self._same_content(latest, content)
        ):
            self.source_changed = False
            self.path = latest
            self.payload = {
                **payload,
                "collected_at": collected_at_from_token(
                    bronze_collection_token(latest)
                ),
            }
            self._write_manifest(parquet.metadata.num_rows)
            return WriteResult(str(latest), parquet.metadata.num_rows)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        invalidate_success_marker(self.path.parent)
        atomic_write(self.path, lambda temporary: temporary.write_bytes(content))
        self._write_manifest(parquet.metadata.num_rows)
        return WriteResult(str(self.path), parquet.metadata.num_rows)

    def _write_manifest(self, row_count: int) -> None:
        manifest = build_bronze_manifest(
            self.payload,
            service_area=self._service_area,
            row_count=row_count,
        )
        target = self.path.parent / MANIFEST_FILE_NAME
        body = bronze_manifest_bytes(manifest)
        atomic_write(target, lambda temporary: temporary.write_bytes(body))

    @staticmethod
    def _latest_data_path(partition_dir: Path) -> Path | None:
        candidates = (
            *partition_dir.glob("*.parquet"),
            *partition_dir.glob("collected_at=*/data.parquet"),
        )
        return max(
            (
                path
                for path in candidates
                if bronze_collection_token(path) and data_path_is_complete(path)
            ),
            key=bronze_collection_token,
            default=None,
        )

    @staticmethod
    def _same_content(path: Path, content: bytes) -> bool:
        with path.open("rb") as source:
            existing_hash = hashlib.file_digest(source, "sha256").digest()
        return existing_hash == hashlib.sha256(content).digest()

    def _data_path(self, payload: dict) -> Path:
        dataset_root = self._base_dir / self._dataset_dir
        area = service_area_segment(self._service_area)
        return (
            (dataset_root / area)
            / f"year_month={payload['year_month']}"
            / _collected_at_dir_name(payload)
            / BRONZE_DATA_FILE_NAME
        )


class S3MonthlyParquetBronzeLoader(Loader):
    """S3 월 파티션에 변경된 원본만 수집 시각 객체로 보존합니다."""

    def __init__(
        self,
        dataset: str,
        dataset_dir: str,
        service_area: str,
        bucket: str | None = None,
    ):
        load_local_env()
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._service_area = service_area
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
        if (
            latest is not None
            and PurePosixPath(latest).name == BRONZE_DATA_FILE_NAME
            and _same_bytes(latest_content, content)
        ):
            self.source_changed = False
            self.payload = {
                **payload,
                "collected_at": collected_at_from_token(
                    bronze_collection_token(PurePosixPath(latest))
                ),
            }
            self._write_manifest(latest, parquet.metadata.num_rows)
            return WriteResult(
                f"s3://{self._bucket}/{latest}", parquet.metadata.num_rows
            )

        key = f"{prefix}{_collected_at_dir_name(payload)}/{BRONZE_DATA_FILE_NAME}"
        result = S3Loader(
            key=key,
            bucket=self._bucket,
            invalidate_parent_success=True,
        ).write(
            S3Object(body=content, row_count=parquet.metadata.num_rows)
        )
        self._write_manifest(key, parquet.metadata.num_rows)
        return result

    def _write_manifest(self, data_key: str, row_count: int) -> None:
        manifest = build_bronze_manifest(
            self.payload,
            service_area=self._service_area,
            row_count=row_count,
        )
        key = str(PurePosixPath(data_key).with_name(MANIFEST_FILE_NAME))
        S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=bronze_manifest_bytes(manifest))
        )

    def _partition_prefix(self, payload: dict) -> str:
        return (
            join_segments(
                "bronze",
                self._dataset_dir,
                service_area_segment(self._service_area),
                f"year_month={payload['year_month']}",
            )
            + "/"
        )

    def _latest_key(self, prefix: str) -> str | None:
        keys = list_keys(self._bucket, prefix)
        key_set = set(keys)
        candidates = (
            (key, bronze_collection_token(PurePosixPath(key)))
            for key in keys
            if data_key_is_complete(key, key_set)
        )
        return max(
            ((key, token) for key, token in candidates if token),
            key=lambda item: item[1],
            default=(None, None),
        )[0]


def build_bronze_loader(
    storage: str,
    base_dir: str,
    dataset: str,
    dataset_dir: str,
    service_area: str,
    bucket: str | None = None,
) -> Loader:
    if storage == "local":
        return MonthlyParquetBronzeLoader(
            base_dir,
            dataset,
            dataset_dir,
            service_area=service_area,
        )
    if storage == "s3":
        return S3MonthlyParquetBronzeLoader(
            dataset,
            dataset_dir,
            bucket=bucket,
            service_area=service_area,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
