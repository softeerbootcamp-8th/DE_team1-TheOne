"""가짜 기사·운행·보유 차량 월별 릴리스를 내려주는 HTTP API.

`SOURCE_API_ENV=local`(기본값)은 생성 DAG(`synthetic_driver_trip_source`)가 쓰는
`year_month=YYYY-MM/manifest.json` 월 우선 레이아웃을 그대로 읽습니다. `prod`는
S3에서 `<prefix>/<dataset>/year_month=YYYY-MM/data.parquet` 고정 키를 직접 읽습니다
(manifest 없음 — 공개 계약은 이미 Parquet 파일만 내려주는 것으로 단순화됨, #547).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import BinaryIO
from urllib.parse import parse_qs, urlsplit

import boto3
from botocore.exceptions import ClientError

from shared.common.env import load_local_env
from shared.common.s3_reader import get_object_stream, list_keys
from shared.common.source_published_layout import (
    PUBLISHED_DATASETS,
    PUBLISHED_SERVICE_AREA,
    S3_PUBLISHED_ROOT,
)

DEFAULT_CHUNK_SIZE = 1024 * 1024

DATASETS = PUBLISHED_DATASETS
DATASET_PATTERN = re.compile(r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$")
LATEST_DATASET_PATTERN = re.compile(r"^/v1/data/latest/datasets/([a-z_]+)$")
SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _validate_service_area(service_area: str) -> str:
    if not SERVICE_AREA_PATTERN.fullmatch(service_area):
        raise ValueError(f"invalid service_area: {service_area!r}")
    return service_area


class DatasetStorageError(Exception):
    """릴리스는 있지만 읽을 수 없는 상태(예: manifest 손상)."""


@dataclass(frozen=True)
class DatasetMetadata:
    content_length: int
    etag: str
    last_modified: datetime


class DatasetStorage:
    """service_area+dataset+year_month 로 parquet 스트림을 찾는 인터페이스."""

    def open(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> tuple[BinaryIO, int] | None:
        """(스트림, content_length) 를 돌려줍니다. 다 쓰면 스트림을 `.close()` 해야 합니다.

        전체를 메모리에 올리지 않기 위한 것이라, 호출부는 `.read(size)`로 청크 단위로
        읽어 즉시 흘려보내야 합니다 — 500MB짜리 원본을 통째로 들고 있지 않습니다.
        """
        raise NotImplementedError

    def latest_year_month(
        self, dataset: str, service_area: str = PUBLISHED_SERVICE_AREA
    ) -> str | None:
        raise NotImplementedError

    def metadata(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> DatasetMetadata | None:
        raise NotImplementedError


class LocalDatasetStorage(DatasetStorage):
    """`<root>/year_month=YYYY-MM/manifest.json` 릴리스 레이아웃을 그대로 읽습니다."""

    def __init__(self, root: str | Path):
        self._root = Path(root).resolve()

    def _area_root(self, service_area: str) -> Path:
        _validate_service_area(service_area)
        if service_area == PUBLISHED_SERVICE_AREA:
            return self._root
        return self._root / f"service_area={service_area}"

    def _file(
        self, dataset: str, year_month: str, service_area: str
    ) -> tuple[Path, dict] | None:
        area_root = self._area_root(service_area)
        release = area_root / f"year_month={year_month}"
        manifest_path = release / "manifest.json"
        if not manifest_path.is_file():
            return None
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DatasetStorageError(f"invalid manifest: {manifest_path}") from exc
        metadata = manifest.get("datasets", {}).get(dataset, {})
        path = release / str(metadata.get("file", ""))
        # release 디렉터리 밖을 가리키면(경로 탈출) 내보내지 않습니다.
        if not path.is_file() or path.parent.parent != area_root:
            return None
        return path, metadata

    def open(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> tuple[BinaryIO, int] | None:
        found = self._file(dataset, year_month, service_area)
        if found is None:
            return None
        path, _ = found
        return path.open("rb"), path.stat().st_size

    def latest_year_month(
        self, dataset: str, service_area: str = PUBLISHED_SERVICE_AREA
    ) -> str | None:
        releases = sorted(self._area_root(service_area).glob("year_month=????-??"))
        return releases[-1].name.removeprefix("year_month=") if releases else None

    def metadata(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> DatasetMetadata | None:
        found = self._file(dataset, year_month, service_area)
        if found is None:
            return None
        path, manifest_metadata = found
        sha256 = manifest_metadata.get("sha256")
        if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise DatasetStorageError(f"invalid sha256 metadata: {path}")
        stat = path.stat()
        return DatasetMetadata(
            content_length=stat.st_size,
            etag=f'"{sha256}"',
            last_modified=datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        )


class S3DatasetStorage(DatasetStorage):
    """S3의 `<prefix>/<service_area>/<dataset>/year_month=YYYY-MM/data.parquet`을 읽습니다."""

    def __init__(self, bucket: str, prefix: str = S3_PUBLISHED_ROOT):
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> str:
        area = _validate_service_area(service_area)
        return f"{self._prefix}/{area}/{dataset}/year_month={year_month}/data.parquet"

    def open(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> tuple[BinaryIO, int] | None:
        try:
            return get_object_stream(
                self._bucket, self._key(dataset, year_month, service_area)
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None
            raise DatasetStorageError(str(exc)) from exc

    def latest_year_month(
        self, dataset: str, service_area: str = PUBLISHED_SERVICE_AREA
    ) -> str | None:
        area = _validate_service_area(service_area)
        prefix = f"{self._prefix}/{area}/{dataset}/year_month="
        months = sorted(
            key[len(prefix) :].split("/", 1)[0]
            for key in list_keys(self._bucket, prefix)
            if key.startswith(prefix)
        )
        return months[-1] if months else None

    def metadata(
        self,
        dataset: str,
        year_month: str,
        service_area: str = PUBLISHED_SERVICE_AREA,
    ) -> DatasetMetadata | None:
        try:
            response = boto3.client("s3").head_object(
                Bucket=self._bucket,
                Key=self._key(dataset, year_month, service_area),
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None
            raise DatasetStorageError(str(exc)) from exc
        return DatasetMetadata(
            content_length=response["ContentLength"],
            etag=response["ETag"],
            last_modified=response["LastModified"],
        )


class ReleaseRequestHandler(BaseHTTPRequestHandler):
    storage: DatasetStorage  # create_server 가 서브클래스에 주입합니다.

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        self._route(head_only=False)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
        self._route(head_only=True)

    def _route(self, *, head_only: bool) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/health":
            self._send_json({"status": "ok"}, head_only=head_only)
            return
        area_request = self._service_area(parsed.query)
        if area_request is None:
            return
        service_area, explicit_area = area_request
        if match := LATEST_DATASET_PATTERN.fullmatch(path):
            self._send_latest(
                match.group(1),
                service_area,
                explicit_area=explicit_area,
                head_only=head_only,
            )
            return
        if match := DATASET_PATTERN.fullmatch(path):
            self._send_dataset(
                match.group(1),
                match.group(2),
                service_area,
                head_only=head_only,
            )
            return
        self.send_error(404, "endpoint not found")

    def _service_area(self, query: str) -> tuple[str, bool] | None:
        values = parse_qs(query, keep_blank_values=True).get("service_area")
        if values is None:
            return PUBLISHED_SERVICE_AREA, False
        if len(values) != 1 or not SERVICE_AREA_PATTERN.fullmatch(values[0]):
            self.send_error(400, "invalid service_area")
            return None
        return values[0], True

    def _send_latest(
        self,
        dataset: str,
        service_area: str,
        *,
        explicit_area: bool,
        head_only: bool,
    ) -> None:
        if dataset not in DATASETS:
            self.send_error(404, "dataset not found")
            return
        try:
            year_month = self.storage.latest_year_month(
                dataset, service_area=service_area
            )
        except DatasetStorageError:
            self.send_error(500, "invalid release")
            return
        if year_month is None:
            self.send_error(404, "data not found")
            return
        self.send_response(307)
        location = f"/v1/data/{year_month}/datasets/{dataset}"
        if explicit_area:
            location += f"?service_area={service_area}"
        self.send_header("Location", location)
        self.end_headers()

    def _send_dataset(
        self,
        year_month: str,
        dataset: str,
        service_area: str,
        *,
        head_only: bool,
    ) -> None:
        if dataset not in DATASETS:
            self.send_error(404, "dataset not found")
            return
        try:
            metadata = self.storage.metadata(
                dataset, year_month, service_area=service_area
            )
        except DatasetStorageError:
            self.send_error(500, "invalid release")
            return
        if metadata is None:
            self.send_error(404, "dataset file not found")
            return
        if self._not_modified(metadata):
            self.send_response(304)
            self._send_validator_headers(metadata)
            self.end_headers()
            return

        stream = None
        if not head_only:
            try:
                opened = self.storage.open(
                    dataset, year_month, service_area=service_area
                )
            except DatasetStorageError:
                self.send_error(500, "invalid release")
                return
            if opened is None:
                self.send_error(404, "dataset file not found")
                return
            stream, _ = opened

        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.parquet")
        self.send_header("Content-Length", str(metadata.content_length))
        self.send_header(
            "Content-Disposition", f'attachment; filename="{dataset}.parquet"'
        )
        self._send_validator_headers(metadata)
        self.end_headers()
        if head_only:
            return

        try:
            chunk_size = int(os.getenv("SOURCE_API_CHUNK_SIZE", DEFAULT_CHUNK_SIZE))
            while chunk := stream.read(chunk_size):
                self.wfile.write(chunk)
        finally:
            stream.close()

    def _not_modified(self, metadata: DatasetMetadata) -> bool:
        if_none_match = self.headers.get("If-None-Match")
        if if_none_match is not None:
            tags = {tag.strip().removeprefix("W/") for tag in if_none_match.split(",")}
            return "*" in tags or metadata.etag in tags

        if_modified_since = self.headers.get("If-Modified-Since")
        if if_modified_since is None:
            return False
        try:
            since = parsedate_to_datetime(if_modified_since)
        except (TypeError, ValueError, OverflowError):
            return False
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        modified = metadata.last_modified.astimezone(timezone.utc).replace(microsecond=0)
        return modified <= since.astimezone(timezone.utc)

    def _send_validator_headers(self, metadata: DatasetMetadata) -> None:
        self.send_header("ETag", metadata.etag)
        self.send_header(
            "Last-Modified",
            format_datetime(metadata.last_modified.astimezone(timezone.utc), usegmt=True),
        )

    def _send_json(self, value: dict, *, head_only: bool) -> None:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        print(f"source-api: {format % args}")


def storage_from_env() -> DatasetStorage:
    """`SOURCE_API_ENV`(local|prod)에 따라 저장소 구현을 고릅니다."""
    env = os.getenv("SOURCE_API_ENV", "local")
    if env == "local":
        root = os.getenv("SOURCE_API_LOCAL_ROOT", "data/source/synthetic_driver_trip_api")
        return LocalDatasetStorage(root)
    if env == "prod":
        bucket = os.environ["SOURCE_API_S3_BUCKET"]
        prefix = os.getenv("SOURCE_API_S3_PREFIX") or S3_PUBLISHED_ROOT
        return S3DatasetStorage(bucket, prefix)
    raise ValueError(f"알 수 없는 SOURCE_API_ENV: {env!r} (local 또는 prod)")


def create_server(
    storage: DatasetStorage,
    host: str = "127.0.0.1",
    port: int = 8091,
) -> ThreadingHTTPServer:
    # ponytail: 팀 내부 가짜 원천용 stdlib 서버.
    handler = type(
        "ConfiguredReleaseRequestHandler",
        (ReleaseRequestHandler,),
        {"storage": storage},
    )
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    load_local_env()
    storage = storage_from_env()
    host = os.getenv("SOURCE_API_HOST", "127.0.0.1")
    port = int(os.getenv("SOURCE_API_PORT", "8091"))
    server = create_server(storage, host, port)
    print(f"source API ({os.getenv('SOURCE_API_ENV', 'local')}): http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
