"""월별 가짜 기사-운행 원천 생성 DAG의 수집·입력·출력 검증."""

import hashlib
import json
import logging
import shutil
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError

import pyarrow.parquet as pq
from airflow.sdk import task
from airflow.sdk.exceptions import AirflowSkipException

from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_REQUIRED_NON_NULL as SNAPSHOT_REQUIRED_COLUMNS,
    LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL as INVENTORY_REQUIRED_COLUMNS,
)
from shared.airflow.common.project_paths import PROJECT_ROOT
from sub.generators.synthetic_company_snapshot.generate import (
    resolve_vehicle_master_path,
)

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SOURCE_ROOT = ROOT / "data" / "source"
DEFAULT_PATHS = {
    "source_input_dir": str(SOURCE_ROOT / "synthetic_driver_trip_inputs"),
    "vehicle_master_dir": str(ROOT / "data" / "silver" / "vehicle_master"),
    "state_output_dir": str(SOURCE_ROOT / "synthetic_driver_trip_state"),
    "release_output_dir": str(SOURCE_ROOT / "synthetic_driver_trip_api"),
}
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
HVFHV_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year_month}.parquet"
MAX_MONTH_LOOKBACK = 6
RELEASE_DATASETS = {
    "hvfhv_taxi_trips": {"pickup_datetime", "taxi_id"},
    "driver_vehicle_monthly_snapshot": SNAPSHOT_REQUIRED_COLUMNS,
    "lease_vehicle_inventory": INVENTORY_REQUIRED_COLUMNS,
}


def _test_scoped_root(path: str | Path, test_row_limit: int) -> Path:
    if test_row_limit < 0:
        raise ValueError("test_row_limit는 0 이상이어야 합니다")
    root = Path(path)
    if test_row_limit == 0:
        return root
    return root / "_temporary" / f"test_row_limit={test_row_limit}"


def _manual_year_month(params: dict) -> str | None:
    year, month = params.get("year"), params.get("month")
    if not (year and month):
        return None
    value = f"{str(year).strip()}-{str(month).strip().zfill(2)}"
    datetime.strptime(value, "%Y-%m")
    return value


def _source_input_file(source_input_dir: str | Path, year_month: str) -> Path:
    return Path(source_input_dir) / "hvfhv" / f"year_month={year_month}" / "hvfhv.parquet"


def tlc_is_available(year: str, month: str) -> bool:
    request = urllib.request.Request(
        HVFHV_URL.format(year_month=f"{year}-{month}"),
        method="HEAD",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status == 200
    except HTTPError as exc:
        if exc.code in (403, 404):
            return False
        raise


def fetch_tlc_hvfhv(
    year: str,
    month: str,
    source_input_dir: str | Path,
) -> Path:
    """월별 Parquet을 메모리에 올리지 않고 임시 파일로 받아 원자적으로 공개합니다."""
    year_month = f"{year}-{month}"
    final = _source_input_file(source_input_dir, year_month)
    if final.is_file():
        pq.read_schema(final)
        return final

    final.parent.mkdir(parents=True, exist_ok=True)
    temporary = final.parent / f".hvfhv-{uuid.uuid4().hex}.parquet"
    url = HVFHV_URL.format(year_month=year_month)
    try:
        with (
            urllib.request.urlopen(url, timeout=180) as response,
            temporary.open("wb") as output,
        ):
            shutil.copyfileobj(response, output, length=1024 * 1024)
        pq.read_schema(temporary)
        temporary.replace(final)
        return final
    finally:
        temporary.unlink(missing_ok=True)


def resolve_source_year_month(
    logical_date: datetime,
    params: dict,
    *,
    is_available,
) -> str | None:
    """수동 월 또는 아직 릴리스하지 않은 최신 TLC 공개 월을 고릅니다."""
    if manual := _manual_year_month(params):
        return manual
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)

    release_root = Path(params["release_output_dir"])
    cursor = logical_date.replace(day=1)
    for _ in range(MAX_MONTH_LOOKBACK):
        cursor = (cursor - timedelta(days=1)).replace(day=1)
        year_month = cursor.strftime("%Y-%m")
        if (release_root / f"year_month={year_month}" / "manifest.json").is_file():
            continue
        if _source_input_file(params["source_input_dir"], year_month).is_file():
            return year_month
        if is_available(cursor.strftime("%Y"), cursor.strftime("%m")):
            return year_month
    return None


def _zone_lookup(source_input_dir: str | Path) -> Path:
    path = Path(source_input_dir) / "taxi_zone_lookup.csv"
    if path.is_file():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}-{uuid.uuid4().hex}")
    try:
        with urllib.request.urlopen(ZONE_LOOKUP_URL, timeout=30) as response:
            temporary.write_bytes(response.read())
        if temporary.stat().st_size == 0:
            raise ValueError("taxi zone lookup 응답이 비어 있습니다")
        temporary.replace(path)
        return path
    finally:
        temporary.unlink(missing_ok=True)


@task(task_id="collect_source_input")
def collect_source_input_task(**context) -> dict:
    params = context["params"]
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    year_month = resolve_source_year_month(
        logical_date,
        params,
        is_available=tlc_is_available,
    )
    if year_month is None:
        raise AirflowSkipException(
            "새로 공개됐고 아직 발행하지 않은 HVFHV 월이 없습니다"
        )
    year, month = year_month.split("-")
    path = _source_input_file(params["source_input_dir"], year_month)
    if path.is_file():
        pq.read_schema(path)
    else:
        path = fetch_tlc_hvfhv(
            year,
            month,
            params["source_input_dir"],
        )
    zone_lookup = _zone_lookup(params["source_input_dir"])
    result = {
        "year_month": year_month,
        "hvfhv_input_path": str(path),
        "zone_lookup_path": str(zone_lookup),
    }
    logger.info("가짜 기사-운행 원천 입력 수집 완료: %s", result)
    return result


def validate_source_inputs(source_result: dict, params: dict) -> dict:
    """대상 월의 수집된 HVFHV 입력을 확정합니다.

    기사·차량 상태는 여기서 확인하지 않습니다. event sourcing 이후
    `prepare_monthly_state()`가 이전 체크포인트를 이어받거나(계속월) 스스로
    부트스트랩하므로(첫 달), 사전에 어떤 스냅샷이 존재해야 한다는 전제 자체가
    없습니다 — 예전(계약 기반 legacy) 아키텍처가 남긴 검사였습니다.
    """
    year_month = str(source_result["year_month"])
    datetime.strptime(year_month, "%Y-%m")
    target_date = date.fromisoformat(f"{year_month}-01")

    hvfhv_input = Path(source_result["hvfhv_input_path"])
    zone_lookup = Path(source_result["zone_lookup_path"])
    for name, path in (
        ("hvfhv_input_path", hvfhv_input),
        ("zone_lookup_path", zone_lookup),
    ):
        if not path.is_file():
            raise FileNotFoundError(f"기사-운행 입력 파일이 없습니다: {name}={path}")

    vehicle_master = resolve_vehicle_master_path(params["vehicle_master_dir"])
    return {
        "year_month": year_month,
        "snapshot_date": target_date.isoformat(),
        "hvfhv_input_path": str(hvfhv_input),
        "zone_lookup_path": str(zone_lookup),
        "vehicle_master_path": str(vehicle_master),
    }


@task(task_id="validate_inputs")
def validate_inputs_task(source_result: dict, **context) -> dict:
    result = validate_source_inputs(source_result, context["params"])
    logger.info("가짜 기사-운행 원천 입력 검증 통과: %s", result)
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_release(output_dir: str | Path, year_month: str, seed: int | None) -> None:
    """release로 공개할 manifest·단일 Parquet·행 수·checksum을 확인합니다."""
    release = Path(output_dir) / f"year_month={year_month}"
    manifest_path = release / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"원천 릴리스 manifest가 없습니다: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("year_month") != year_month:
        raise ValueError(f"원천 릴리스 계보가 요청과 다릅니다: {manifest}")
    # 계보 필드를 여기서 다시 해싱하지 않습니다 — 설정을 두 곳에서 읽으면 그 둘이
    # 갈릴 수 있습니다. 대신 존재와 내부 정합성만 봅니다. 어느 설정으로 만들었는지는
    # run_id·config_hash 가 답하고, 그 값이 config 와 맞는지는 발행 쪽
    # (`source_job._existing_release`) 이 이미 판정합니다.
    run_id, config_hash = manifest.get("run_id"), manifest.get("config_hash")
    if not run_id or not config_hash:
        raise ValueError(
            f"원천 릴리스 manifest에 run_id/config_hash가 없습니다: {manifest_path}. "
            f"설정 통합 이전 릴리스라면 해당 파티션을 지우고 다시 발행하세요: rm -rf {release}"
        )
    if run_id != f"{year_month}_{config_hash}":
        raise ValueError(
            f"run_id가 year_month·config_hash와 어긋납니다: run_id={run_id!r}, "
            f"year_month={year_month!r}, config_hash={config_hash!r}"
        )
    # seed 를 명시해 돌린 실행만 비교합니다. 비웠으면 config 의 global_seed 를 쓴
    # 것이므로 여기서 맞춰 볼 요청값이 없습니다.
    if seed is not None and manifest.get("seed") != seed:
        raise ValueError(f"원천 릴리스 seed가 요청과 다릅니다: {manifest.get('seed')} != {seed}")

    for dataset, required_columns in RELEASE_DATASETS.items():
        metadata = manifest.get("datasets", {}).get(dataset, {})
        path = release / str(metadata.get("file", ""))
        if not path.is_file():
            raise ValueError(f"원천 릴리스 Parquet이 없습니다: {path}")
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows <= 0 or parquet.metadata.num_rows != metadata.get(
            "row_count"
        ):
            raise ValueError(f"{dataset} 행 수가 manifest와 다릅니다")
        if _sha256(path) != metadata.get("sha256"):
            raise ValueError(f"{dataset} checksum이 manifest와 다릅니다")
        missing = required_columns - set(parquet.schema_arrow.names)
        if missing:
            raise ValueError(f"{dataset} 필수 컬럼 누락: {sorted(missing)}")

    # coverage/ceiling/saturation/탈락 사유/클리핑 — 진단용이라 manifest 계보와
    # 분리돼 있습니다(#608). 존재만 확인하고 내용은 로그로 남겨 운영자가 봅니다.
    quality_report_path = release / "quality_report.json"
    if not quality_report_path.is_file():
        raise ValueError(f"원천 릴리스 품질 리포트가 없습니다: {quality_report_path}")
    logger.info("원천 릴리스 품질 리포트: %s", quality_report_path.read_text(encoding="utf-8"))


@task(task_id="validate_release")
def validate_release_task(**context) -> None:
    result = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    params = context["params"]
    validate_release(
        _test_scoped_root(
            params["release_output_dir"], int(params.get("test_row_limit", 0))
        ),
        result["year_month"],
        params["seed"],
    )
