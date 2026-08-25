"""원천 API를 본문 다운로드 없이 검사하고 정상 Bronze manifest와 비교합니다."""

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urljoin, urlsplit

import requests
from airflow.sdk import task
from airflow.task.trigger_rule import TriggerRule

from main.airflow.common import assets
from main.airflow.common.gold_staleness import (
    evaluate_staleness,
    resolve_stale_sla_days,
)
from main.airflow.common.monthly_bronze import (
    SILVER_PART_PATTERN,
    bronze_collection_token,
    collected_at_token,
    latest_local_silver_version,
)
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_stale_alert_callback
from shared.common.bronze_manifest import MANIFEST_FILE_NAME, parse_bronze_manifest
from shared.common.s3_reader import get_object_bytes, list_keys
from shared.common.success_marker import data_key_is_complete, data_path_is_complete


logger = logging.getLogger(__name__)

DATASET_URL_PATTERN = re.compile(
    r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$"
)
BRONZE_DATASET_DIRS = {
    "monthly_taxi_trip": "monthly_taxi_trip",
    "driver_vehicle_monthly_snapshot": "driver_vehicle_monthly_snapshot",
    "lease_vehicle_inventory": "lease_vehicle_inventory",
}
SILVER_DIR_ENVS = {
    "monthly_taxi_trip": "SILVER_DIR",
    "driver_vehicle_monthly_snapshot": "DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER_DIR",
    "lease_vehicle_inventory": "LEASE_VEHICLE_INVENTORY_SILVER_DIR",
}


def _latest_bronze_state(
    dataset: str, year_month: str, service_area: str
) -> tuple[str | None, dict | None]:
    """최신 `_SUCCESS` Bronze token과 검증 가능한 manifest를 읽습니다.

    manifest가 없거나 깨졌으면 token은 유지하고 상태만 None으로 돌려 다음 수집이
    기존 원본을 재검증하며 manifest를 복구하게 합니다.
    """
    dataset_dir = BRONZE_DATASET_DIRS[dataset]
    storage = os.getenv("BRONZE_STORAGE", "local")
    if storage == "local":
        root = Path(
            os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
        )
        partition = (
            root
            / dataset_dir
            / f"service_area={service_area}"
            / f"year_month={year_month}"
        )
        candidates = (
            *partition.glob("*.parquet"),
            *partition.glob("collected_at=*/data.parquet"),
        )
        latest = max(
            (
                (token, path)
                for path in candidates
                if data_path_is_complete(path)
                and (token := bronze_collection_token(path))
            ),
            default=(None, None),
            key=lambda item: item[0],
        )
        token, path = latest
        if path is None or path.name != "data.parquet":
            return token, None
        manifest_path = path.parent / MANIFEST_FILE_NAME
        try:
            manifest = _manifest_for_token(manifest_path.read_bytes(), token)
        except (OSError, ValueError):
            logger.warning("Bronze manifest를 읽지 못해 재수집합니다: %s", manifest_path)
            return token, None
        return token, manifest
    if storage == "s3":
        bucket = os.environ["DATA_LAKE_S3_BUCKET"]
        prefix = (
            f"bronze/{dataset_dir}/service_area={service_area}/"
            f"year_month={year_month}/"
        )
        keys = list_keys(bucket, prefix)
        key_set = set(keys)
        token, key = max(
            (
                (token, key)
                for key in keys
                if data_key_is_complete(key, key_set)
                and (token := bronze_collection_token(PurePosixPath(key)))
            ),
            default=(None, None),
            key=lambda item: item[0],
        )
        if key is None or PurePosixPath(key).name != "data.parquet":
            return token, None
        manifest_key = str(PurePosixPath(key).with_name(MANIFEST_FILE_NAME))
        if manifest_key not in key_set:
            logger.warning(
                "Bronze manifest가 없어 재수집합니다: s3://%s/%s",
                bucket,
                manifest_key,
            )
            return token, None
        try:
            manifest = _manifest_for_token(
                get_object_bytes(bucket, manifest_key), token
            )
        except (OSError, ValueError):
            logger.warning(
                "Bronze manifest를 읽지 못해 재수집합니다: s3://%s/%s",
                bucket,
                manifest_key,
            )
            return token, None
        return token, manifest
    raise ValueError(f"알 수 없는 BRONZE_STORAGE: {storage!r} (local 또는 s3)")


def _manifest_for_token(body: bytes, token: str) -> dict:
    manifest = parse_bronze_manifest(body)
    if collected_at_token(manifest["collected_at"]) != token:
        raise ValueError("Bronze manifest collected_at이 경로 token과 다릅니다")
    return manifest


def _manifest_previous(
    manifest: dict | None,
    *,
    api_base_url: str,
    dataset: str,
    year_month: str,
    service_area: str,
) -> dict | None:
    if not manifest:
        return None
    expected = {
        "api_base_url": api_base_url.rstrip("/"),
        "dataset": dataset,
        "year_month": year_month,
        "service_area": service_area,
    }
    if any(manifest.get(field) != value for field, value in expected.items()):
        logger.warning("Bronze manifest 계보가 요청과 달라 재수집합니다: %s", expected)
        return None
    return {
        "api_base_url": expected["api_base_url"],
        "year_month": year_month,
        "etag": manifest["source_etag"],
        "last_modified": manifest["source_last_modified"],
    }


def _silver_version_exists(
    dataset: str,
    year_month: str,
    service_area: str,
    collection_token: str,
) -> bool:
    storage = os.getenv("BRONZE_STORAGE", "local")
    version_name = f"source_collected_at={collection_token}"
    if storage == "local":
        default_root = PROJECT_ROOT / "data" / "silver" / dataset
        root = Path(os.getenv(SILVER_DIR_ENVS[dataset], str(default_root)))
        partition = root / f"service_area={service_area}" / f"year_month={year_month}"
        latest = latest_local_silver_version(partition)
        return latest is not None and (
            latest.name == version_name or latest.stem == collection_token
        )
    if storage == "s3":
        bucket = os.environ["DATA_LAKE_S3_BUCKET"]
        partition_prefix = (
            f"silver/{dataset}/service_area={service_area}/"
            f"year_month={year_month}/"
        )
        keys = list_keys(bucket, partition_prefix)
        version_prefix = f"{partition_prefix}{version_name}/"
        names = {
            key.removeprefix(version_prefix)
            for key in keys
            if key.startswith(version_prefix)
            and "/" not in key.removeprefix(version_prefix)
        }
        has_data = "data.parquet" in names or any(
            SILVER_PART_PATTERN.fullmatch(name) for name in names
        )
        return "_SUCCESS" in names and has_data
    raise ValueError(f"알 수 없는 BRONZE_STORAGE: {storage!r} (local 또는 s3)")


def _requested_year_month(year, month) -> str | None:
    if bool(year) != bool(month):
        raise ValueError("year와 month는 함께 지정해야 합니다")
    if not year:
        return None
    value = f"{str(year).strip()}-{str(month).strip().zfill(2)}"
    try:
        datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ValueError("year와 month가 유효한 YYYY-MM 형식이 아닙니다") from exc
    return value


def _validate_dataset_url(
    api_base_url: str, url: str, dataset: str, service_area: str
) -> str:
    base, target = urlsplit(api_base_url), urlsplit(url)
    if (target.scheme, target.netloc) != (base.scheme, base.netloc):
        raise ValueError("데이터셋 응답 URL은 API와 같은 host여야 합니다")
    match = DATASET_URL_PATTERN.fullmatch(target.path.rstrip("/"))
    if not match or match.group(2) != dataset:
        raise ValueError(f"데이터셋 응답 URL이 올바르지 않습니다: {url}")
    areas = parse_qs(target.query).get("service_area")
    if service_area == "NYC":
        valid_area = areas in (None, ["NYC"])
    else:
        valid_area = areas == [service_area]
    if not valid_area:
        raise ValueError(f"데이터셋 응답 URL의 service_area가 다릅니다: {url}")
    return match.group(1)


def _target_url(
    api_base_url: str,
    dataset: str,
    requested_month: str | None,
    service_area: str,
    *,
    timeout: int,
) -> tuple[str, str]:
    if requested_month:
        url = urljoin(
            f"{api_base_url.rstrip('/')}/",
            f"v1/data/{requested_month}/datasets/{dataset}",
        )
        return url, requested_month

    latest_url = urljoin(
        f"{api_base_url.rstrip('/')}/",
        f"v1/data/latest/datasets/{dataset}",
    )
    response = requests.head(
        latest_url,
        params={"service_area": service_area},
        timeout=timeout,
        allow_redirects=False,
    )
    response.raise_for_status()
    location = response.headers.get("Location")
    if response.status_code not in (301, 302, 303, 307, 308) or not location:
        raise ValueError(
            f"latest 응답에 데이터셋 월 redirect가 없습니다: {latest_url}"
        )
    target_url = urljoin(response.url, location)
    return target_url, _validate_dataset_url(
        api_base_url, target_url, dataset, service_area
    )


def inspect_source(
    api_base_url: str,
    dataset: str,
    *,
    service_area: str = "NYC",
    year=None,
    month=None,
    previous: dict | None = None,
    timeout: int = 30,
) -> dict:
    """대상 월의 원천 validator를 조건부 HEAD로 확인합니다."""
    api_base_url = api_base_url.rstrip("/")
    requested_month = _requested_year_month(year, month)
    target_url, year_month = _target_url(
        api_base_url,
        dataset,
        requested_month,
        service_area,
        timeout=timeout,
    )
    return _inspect_target_source(
        api_base_url,
        dataset,
        target_url,
        year_month,
        service_area=service_area,
        previous=previous,
        timeout=timeout,
    )


def _inspect_target_source(
    api_base_url: str,
    dataset: str,
    target_url: str,
    year_month: str,
    *,
    service_area: str,
    previous: dict | None,
    timeout: int,
) -> dict:
    """확정된 월 URL을 이전 정상 Bronze validator로 검사합니다."""

    headers = {}
    same_source = (
        isinstance(previous, dict)
        and previous.get("api_base_url") == api_base_url
        and previous.get("year_month") == year_month
    )
    if same_source:
        if previous.get("etag"):
            headers["If-None-Match"] = previous["etag"]
        if previous.get("last_modified"):
            headers["If-Modified-Since"] = previous["last_modified"]

    target_params = (
        None
        if parse_qs(urlsplit(target_url).query).get("service_area")
        else {"service_area": service_area}
    )
    response = requests.head(
        target_url,
        params=target_params,
        headers=headers,
        timeout=timeout,
        allow_redirects=False,
    )
    if response.status_code not in (200, 304):
        response.raise_for_status()
        raise ValueError(f"예상하지 못한 HEAD 응답: {response.status_code}")

    etag = response.headers.get("ETag") or (previous or {}).get("etag")
    last_modified = response.headers.get("Last-Modified") or (previous or {}).get(
        "last_modified"
    )
    if not etag or not last_modified:
        raise ValueError(f"{dataset} HEAD 응답에 ETag 또는 Last-Modified가 없습니다")

    version = hashlib.sha256(
        f"{year_month}\0{etag}\0{last_modified}".encode()
    ).hexdigest()[:16]
    return {
        "dataset": dataset,
        "year_month": year_month,
        "year": year_month[:4],
        "month": year_month[5:],
        "etag": etag,
        "last_modified": last_modified,
        "changed": response.status_code == 200,
        "version": version,
        "api_base_url": api_base_url,
        "service_area": service_area,
    }


@task.short_circuit(
    task_id="check_and_should_refresh",
    ignore_downstream_trigger_rules=False,
)
def check_and_should_refresh_task(dataset: str, **context) -> dict | bool:
    params = context["params"]
    service_area = assets.resolve_service_area(params)
    api_base_url = params["api_base_url"].rstrip("/")
    requested_month = _requested_year_month(
        params.get("year"), params.get("month")
    )
    target_url, year_month = _target_url(
        api_base_url,
        dataset,
        requested_month,
        service_area,
        timeout=params["request_timeout"],
    )
    collection_token, manifest = _latest_bronze_state(
        dataset, year_month, service_area
    )
    previous = _manifest_previous(
        manifest,
        api_base_url=api_base_url,
        dataset=dataset,
        year_month=year_month,
        service_area=service_area,
    )
    result = _inspect_target_source(
        api_base_url,
        dataset,
        target_url,
        year_month,
        service_area=service_area,
        previous=previous,
        timeout=params["request_timeout"],
    )
    bronze_exists = collection_token is not None and manifest is not None
    silver_exists = bool(collection_token) and _silver_version_exists(
        dataset,
        result["year_month"],
        service_area,
        collection_token,
    )
    result["refresh_required"] = (
        result["changed"] or not bronze_exists or not silver_exists
    )
    logger.info(
        "원천 HEAD 검사: dataset=%s service_area=%s year_month=%s changed=%s "
        "bronze_exists=%s silver_exists=%s refresh_required=%s etag=%s",
        dataset,
        service_area,
        result["year_month"],
        result["changed"],
        bronze_exists,
        silver_exists,
        result["refresh_required"],
        result["etag"],
    )
    return result if result["refresh_required"] else False


@task(
    task_id="publish_api_refresh_ready",
    outlets=[assets.API_SILVER_REFRESH_READY],
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
def publish_api_refresh_ready_task(check_task_ids: list[str], **context) -> None:
    results = context["task_instance"].xcom_pull(task_ids=check_task_ids)
    year_months = {
        result["year_month"]
        for result in results
        if isinstance(result, dict)
        and result.get("refresh_required", result.get("changed"))
    }
    service_area = assets.resolve_service_area(context.get("params", {}))
    for year_month in year_months:
        assets.publish_month_partition(
            context.get("outlet_events"),
            assets.API_SILVER_REFRESH_READY,
            year_month,
            service_area,
        )


@task(task_id="check_gold_staleness")
def check_gold_staleness_task(**context) -> None:
    """매일 지역별 Gold 성공 상태를 확인해 Asset 무발행 blind spot을 감시합니다."""
    service_area = assets.resolve_service_area(context["params"])
    stale_days = resolve_stale_sla_days(context["params"])
    now = datetime.now(timezone.utc)
    days_since, state = evaluate_staleness(service_area, now)
    if days_since is None or days_since <= stale_days:
        return
    logger.warning(
        "Gold staleness SLA 초과: service_area=%s days=%s threshold=%s",
        service_area,
        days_since,
        stale_days,
    )
    try:
        slack_stale_alert_callback(
            {
                **context,
                "partition_key": state.get("partition_key"),
                "days_since_success": days_since,
                "stale_days": stale_days,
            }
        )
    except Exception:
        logger.warning("Slack staleness 알림 전송에 실패했습니다", exc_info=True)
