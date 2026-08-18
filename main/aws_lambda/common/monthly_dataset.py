"""월별 제공 데이터의 Parquet 한 종을 검증해 Bronze에 보존합니다."""

import hashlib
import json
import logging
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


YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
logger = logging.getLogger(__name__)


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


class SyntheticDatasetExtractor(Extractor):
    """월별 manifest에서 요청한 데이터셋 하나만 내려받습니다."""

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
        endpoint = (
            f"v1/data/{self._year_month}"
            if self._year_month
            else "v1/data/latest"
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
            "year_month": manifest["year_month"],
            "dataset": self._dataset,
            "metadata": metadata,
            "content": response.content,
        }

    def _validate_manifest(self, manifest: object) -> dict:
        if not isinstance(manifest, dict):
            raise ValueError("데이터 manifest는 JSON 객체여야 합니다")
        year_month = manifest.get("year_month")
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
            raise ValueError("dataset download_url은 manifest와 같은 host여야 합니다")
        return resolved


class SyntheticDatasetLoader(Loader):
    """검증된 원본 파일을 데이터셋별 월 파티션에 멱등 적재합니다."""

    def __init__(self, base_dir: str, dataset: str, dataset_dir: str):
        self._base_dir = Path(base_dir)
        self._dataset = dataset
        self._dataset_dir = dataset_dir
        self.payload: dict = {}
        self.path: Path | None = None
        self.marker_path: Path | None = None
        self.already_collected = False

    def write(self, payload: dict) -> WriteResult:
        if payload.get("dataset") != self._dataset:
            raise ValueError(f"수집 dataset이 다릅니다: {payload.get('dataset')}")
        content = payload.get("content")
        metadata = payload.get("metadata", {})
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

        self.payload = payload
        self.path = self._data_path(payload)
        self.marker_path = self.path.with_suffix(".json")
        expected_marker = self._marker(payload)
        if self.marker_path.is_file():
            stored = self._read_marker()
            if any(
                stored.get(key) != expected_marker[key]
                for key in ("year_month", "dataset")
            ):
                raise ValueError("기존 Bronze marker의 월 또는 dataset이 다릅니다")
            if stored.get("sha256") == expected_marker["sha256"]:
                self._validate_existing(expected_marker, stored)
                self.already_collected = True
                return WriteResult(str(self.path), metadata["row_count"])
            # 원천이 같은 달의 내용을 고쳐 다시 제공하면 새 내용으로 교체합니다.
            # 직전 checksum과 행 수는 marker에 남겨 교체 이력을 추적합니다.
            logger.warning(
                "%s %s 원천 파일 교체: sha256 %s -> %s (행 수 %s -> %s)",
                self._dataset,
                payload["year_month"],
                stored.get("sha256"),
                expected_marker["sha256"],
                stored.get("row_count"),
                expected_marker["row_count"],
            )
            expected_marker = {
                **expected_marker,
                "previous_sha256": stored.get("sha256"),
                "previous_row_count": stored.get("row_count"),
            }

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

    def _data_path(self, payload: dict) -> Path:
        return (
            self._base_dir
            / self._dataset_dir
            / f"year_month={payload['year_month']}"
            / "data.parquet"
        )

    def _marker(self, payload: dict) -> dict:
        return {
            "year_month": payload["year_month"],
            "dataset": self._dataset,
            "row_count": payload["metadata"]["row_count"],
            "sha256": payload["metadata"]["sha256"],
        }

    def _read_marker(self) -> dict:
        assert self.marker_path is not None
        try:
            marker = json.loads(self.marker_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Bronze marker를 읽지 못했습니다: {self.marker_path}") from exc
        if not isinstance(marker, dict):
            raise ValueError(f"Bronze marker는 JSON 객체여야 합니다: {self.marker_path}")
        return marker

    def _validate_existing(self, expected_marker: dict, stored: dict) -> None:
        """내용이 같은 재수집입니다. 계약값이 어긋나면 마커가 손상된 것으로 봅니다."""
        assert self.marker_path is not None and self.path is not None
        # 교체 이력(previous_*)은 비교에서 뺍니다 — 이전 수집에서만 붙는 값이라
        # 그대로 비교하면 정정 이후 모든 재수집이 손상으로 잡힙니다.
        comparable = {
            key: value
            for key, value in stored.items()
            if not key.startswith("previous_")
        }
        if comparable != expected_marker:
            raise ValueError("같은 월의 기존 marker가 수집 응답과 다릅니다")
        if not self.path.is_file() or _sha256_file(self.path) != expected_marker["sha256"]:
            raise ValueError(f"완료된 Bronze 파일이 없거나 checksum이 다릅니다: {self.path}")
