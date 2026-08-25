"""Source API Bronze 원본과 함께 저장하는 수집 manifest 계약."""

import json
import re


MANIFEST_FILE_NAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = 1
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
COLLECTED_AT_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"
)


def build_bronze_manifest(
    payload: dict,
    *,
    service_area: str,
    row_count: int,
) -> dict:
    """다운로드한 원본의 내용 식별자와 HTTP validator를 고정합니다."""
    return validate_bronze_manifest(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "dataset": payload.get("dataset"),
            "service_area": service_area,
            "year_month": payload.get("year_month"),
            "collected_at": payload.get("collected_at"),
            "data_file": "data.parquet",
            "file_size_bytes": len(payload.get("content") or b""),
            "row_count": row_count,
            "sha256": payload.get("sha256"),
            "api_base_url": payload.get("api_base_url"),
            "source_etag": payload.get("source_etag"),
            "source_last_modified": payload.get("source_last_modified"),
        }
    )


def validate_bronze_manifest(value: object) -> dict:
    """manifest가 refresh 판단에 쓸 수 있는 완전한 계약인지 확인합니다."""
    if not isinstance(value, dict):
        raise ValueError("Bronze manifest는 JSON object여야 합니다")
    manifest = dict(value)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError("Bronze manifest schema_version이 올바르지 않습니다")
    for field in (
        "dataset",
        "service_area",
        "api_base_url",
        "source_etag",
        "source_last_modified",
    ):
        if not isinstance(manifest.get(field), str) or not manifest[field].strip():
            raise ValueError(f"Bronze manifest {field}가 비어 있습니다")
    if not YEAR_MONTH_PATTERN.fullmatch(str(manifest.get("year_month", ""))):
        raise ValueError("Bronze manifest year_month가 올바르지 않습니다")
    if not COLLECTED_AT_PATTERN.fullmatch(str(manifest.get("collected_at", ""))):
        raise ValueError("Bronze manifest collected_at이 올바르지 않습니다")
    if manifest.get("data_file") != "data.parquet":
        raise ValueError("Bronze manifest data_file이 올바르지 않습니다")
    if not SHA256_PATTERN.fullmatch(str(manifest.get("sha256", ""))):
        raise ValueError("Bronze manifest sha256이 올바르지 않습니다")
    for field in ("file_size_bytes", "row_count"):
        if not isinstance(manifest.get(field), int) or manifest[field] < 0:
            raise ValueError(f"Bronze manifest {field}가 올바르지 않습니다")
    if manifest["file_size_bytes"] == 0:
        raise ValueError("Bronze manifest file_size_bytes가 0입니다")
    return manifest


def bronze_manifest_bytes(manifest: dict) -> bytes:
    return json.dumps(
        validate_bronze_manifest(manifest),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")


def parse_bronze_manifest(body: bytes) -> dict:
    try:
        value = json.loads(body)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Bronze manifest가 올바른 JSON이 아닙니다") from exc
    return validate_bronze_manifest(value)
