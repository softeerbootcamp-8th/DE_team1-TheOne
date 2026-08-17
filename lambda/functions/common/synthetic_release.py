"""월별 제공 데이터의 Parquet 한 종을 검증해 Bronze에 보존합니다."""

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult

from .atomic_write import atomic_write


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
RELEASE_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class SyntheticReleaseDatasetExtractor(Extractor):
    """한 release manifest에서 요청한 데이터셋 하나만 내려받습니다."""

    name = "synthetic_release_dataset"

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
        endpoint = (
            f"v1/releases/{self._year_month}"
            if self._year_month
            else "v1/releases/latest"
        )
        response = requests.get(
            urljoin(f"{self._api_base_url}/", endpoint), timeout=30
        )
        response.raise_for_status()
        manifest = self._validate_manifest(response.json())
        metadata = manifest["datasets"][self._dataset]
        response = requests.get(
            self._dataset_url(metadata["download_url"]), timeout=self._timeout
        )
        response.raise_for_status()
        return {
            "release_id": manifest["release_id"],
            "year_month": manifest["year_month"],
            "dataset": self._dataset,
            "metadata": metadata,
            "content": response.content,
        }

    def _validate_manifest(self, manifest: object) -> dict:
        if not isinstance(manifest, dict):
            raise ValueError("데이터 release manifest는 JSON 객체여야 합니다")
        release_id = manifest.get("release_id")
        year_month = manifest.get("year_month")
        if not isinstance(release_id, str) or not RELEASE_ID_PATTERN.fullmatch(
            release_id
        ):
            raise ValueError("manifest release_id 형식이 올바르지 않습니다")
        if not isinstance(year_month, str) or not YEAR_MONTH_PATTERN.fullmatch(
            year_month
        ):
            raise ValueError("manifest year_month 형식이 YYYY-MM이 아닙니다")
        try:
            datetime.strptime(year_month, "%Y-%m")
        except ValueError as exc:
            raise ValueError("manifest year_month가 유효한 월이 아닙니다") from exc
        if self._year_month and year_month != self._year_month:
            raise ValueError(
                f"요청 월과 manifest 월이 다릅니다: {self._year_month} != {year_month}"
            )
        datasets = manifest.get("datasets")
        metadata = datasets.get(self._dataset) if isinstance(datasets, dict) else None
        if not isinstance(metadata, dict):
            raise ValueError(f"manifest 필수 dataset이 없습니다: {self._dataset}")
        if not isinstance(metadata.get("download_url"), str):
            raise ValueError(f"manifest download_url이 없습니다: {self._dataset}")
        if not isinstance(metadata.get("row_count"), int) or metadata["row_count"] <= 0:
            raise ValueError(f"manifest row_count가 올바르지 않습니다: {self._dataset}")
        if not isinstance(metadata.get("sha256"), str) or not SHA256_PATTERN.fullmatch(
            metadata["sha256"]
        ):
            raise ValueError(f"manifest sha256이 올바르지 않습니다: {self._dataset}")
        return manifest

    def _dataset_url(self, download_url: str) -> str:
        resolved = urljoin(f"{self._api_base_url}/", download_url)
        base, target = urlsplit(self._api_base_url), urlsplit(resolved)
        if (target.scheme, target.netloc) != (base.scheme, base.netloc):
            raise ValueError("dataset download_url은 manifest API와 같은 host여야 합니다")
        return resolved


class SyntheticReleaseDatasetLoader(Loader):
    """검증된 release 파일을 데이터셋별 월 파티션에 멱등 적재합니다."""

    def __init__(self, base_dir: str, dataset: str, dataset_dir: str):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self.release: dict = {}
        self.path: Path | None = None
        self.marker_path: Path | None = None
        self.already_collected = False

    def write(self, release: dict) -> WriteResult:
        if release.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {release.get('dataset')}")
        content = release.get("content")
        metadata = release.get("metadata", {})
        if not isinstance(content, bytes) or _sha256_bytes(content) != metadata.get(
            "sha256"
        ):
            raise ValueError(f"다운로드한 {self._dataset} checksum이 manifest와 다릅니다")
        try:
            parquet = pq.ParquetFile(pa.BufferReader(content))
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(f"{self._dataset} 원본이 읽을 수 있는 Parquet이 아닙니다") from exc
        if parquet.metadata.num_rows != metadata.get("row_count"):
            raise ValueError(f"{self._dataset} 행 수가 manifest와 다릅니다")

        self.release = release
        self.path = self._data_path(release)
        self.marker_path = self.path.with_suffix(".json")
        expected_marker = self._marker(release)
        if self.marker_path.is_file():
            self._validate_existing(expected_marker)
            self.already_collected = True
            return WriteResult(str(self.path), metadata["row_count"])

        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(self.path, lambda temporary: temporary.write_bytes(content))
        atomic_write(
            self.marker_path,
            lambda temporary: temporary.write_text(
                json.dumps(expected_marker, ensure_ascii=False, sort_keys=True),
                encoding="utf-8",
            ),
        )
        return WriteResult(str(self.path), metadata["row_count"])

    def _data_path(self, release: dict) -> Path:
        return (
            self._base_dir
            / self._dataset_dir
            / f"year_month={release['year_month']}"
            / f"{release['release_id']}.parquet"
        )

    def _marker(self, release: dict) -> dict:
        return {
            "release_id": release["release_id"],
            "year_month": release["year_month"],
            "dataset": self._dataset,
            "row_count": release["metadata"]["row_count"],
            "sha256": release["metadata"]["sha256"],
        }

    def _validate_existing(self, expected_marker: dict) -> None:
        assert self.marker_path is not None and self.path is not None
        try:
            stored = json.loads(self.marker_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Bronze release marker를 읽지 못했습니다: {self.marker_path}") from exc
        if stored != expected_marker:
            raise ValueError("같은 release_id의 기존 marker가 API 응답과 다릅니다")
        if not self.path.is_file() or _sha256_file(self.path) != expected_marker["sha256"]:
            raise ValueError(f"완료된 Bronze release 파일이 없거나 checksum이 다릅니다: {self.path}")
