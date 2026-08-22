"""원천 API를 본문 다운로드 없이 독립적으로 검사하고 처리 상태를 기록합니다."""

import hashlib
import logging
import re
from datetime import datetime
from urllib.parse import urljoin, urlsplit

import requests
from airflow.sdk import Variable, task
from airflow.task.trigger_rule import TriggerRule

from main.airflow.common import assets


logger = logging.getLogger(__name__)

DATASET_URL_PATTERN = re.compile(
    r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$"
)
STATE_KEY_PREFIX = "source_api_processed__"


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


def _validate_dataset_url(api_base_url: str, url: str, dataset: str) -> str:
    base, target = urlsplit(api_base_url), urlsplit(url)
    if (target.scheme, target.netloc) != (base.scheme, base.netloc):
        raise ValueError("데이터셋 응답 URL은 API와 같은 host여야 합니다")
    match = DATASET_URL_PATTERN.fullmatch(target.path.rstrip("/"))
    if not match or match.group(2) != dataset:
        raise ValueError(f"데이터셋 응답 URL이 올바르지 않습니다: {url}")
    return match.group(1)


def _target_url(
    api_base_url: str,
    dataset: str,
    requested_month: str | None,
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
    response = requests.head(latest_url, timeout=timeout, allow_redirects=False)
    response.raise_for_status()
    location = response.headers.get("Location")
    if response.status_code not in (301, 302, 303, 307, 308) or not location:
        raise ValueError(
            f"latest 응답에 데이터셋 월 redirect가 없습니다: {latest_url}"
        )
    target_url = urljoin(response.url, location)
    return target_url, _validate_dataset_url(api_base_url, target_url, dataset)


def inspect_source(
    api_base_url: str,
    dataset: str,
    *,
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
        timeout=timeout,
    )

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

    response = requests.head(
        target_url,
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
    }


@task.short_circuit(
    task_id="check_and_should_refresh",
    ignore_downstream_trigger_rules=False,
)
def check_and_should_refresh_task(dataset: str, **context) -> dict | bool:
    params = context["params"]
    previous = Variable.get(
        f"{STATE_KEY_PREFIX}{dataset}",
        default=None,
        deserialize_json=True,
    )
    result = inspect_source(
        params["api_base_url"],
        dataset,
        year=params.get("year"),
        month=params.get("month"),
        previous=previous,
        timeout=params["request_timeout"],
    )
    logger.info(
        "원천 HEAD 검사: dataset=%s year_month=%s changed=%s etag=%s",
        dataset,
        result["year_month"],
        result["changed"],
        result["etag"],
    )
    return result if result["changed"] else False


@task(task_id="mark_processed")
def mark_processed_task(result: dict) -> None:
    Variable.set(
        f"{STATE_KEY_PREFIX}{result['dataset']}",
        {
            "api_base_url": result["api_base_url"],
            "year_month": result["year_month"],
            "etag": result["etag"],
            "last_modified": result["last_modified"],
        },
        serialize_json=True,
    )


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
        if isinstance(result, dict) and result.get("changed")
    }
    for year_month in year_months:
        assets.publish_month_partition(
            context.get("outlet_events"),
            assets.API_SILVER_REFRESH_READY,
            year_month,
        )
