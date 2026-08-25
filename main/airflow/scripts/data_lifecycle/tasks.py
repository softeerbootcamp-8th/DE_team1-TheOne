"""S3와 RDS에서 90일이 지난 구버전을 정리합니다."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import PurePosixPath
import re

import boto3
import psycopg2
from airflow.sdk import task

from shared.common.success_marker import QUARANTINE_FILE, SUCCESS_FILE


logger = logging.getLogger(__name__)

DEFAULT_RETENTION_DAYS = 90
SCAN_PREFIXES = ("bronze/", "silver/")
DELETE_BATCH_SIZE = 1000
GOLD_TABLES = ("driver_aggregation", "driver_car_suggestion")
GOLD_VERSION_TABLE = "gold_load_versions"
VERSION_SEGMENT_PATTERN = re.compile(
    r"^(?:source_)?collected_at=(?P<token>\d{8}T\d{12}Z)$"
)


def _version_time(token: str) -> datetime:
    return datetime.strptime(token, "%Y%m%dT%H%M%S%fZ").replace(
        tzinfo=timezone.utc
    )


def _version_from_key(key: str) -> tuple[str, str, datetime] | None:
    parts = PurePosixPath(key).parts
    version_like_parts = [
        part
        for part in parts
        if part.startswith(("collected_at=", "source_collected_at="))
    ]
    matches = [
        (index, match)
        for index, part in enumerate(parts)
        if (match := VERSION_SEGMENT_PATTERN.fullmatch(part))
    ]
    if version_like_parts and len(matches) != len(version_like_parts):
        raise ValueError(f"버전 경로 형식을 해석할 수 없습니다: {key}")
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError(f"버전 경로가 중첩되어 있습니다: {key}")
    index, match = matches[0]
    version_prefix = "/".join(parts[: index + 1])
    partition_prefix = "/".join(parts[:index])
    return version_prefix, partition_prefix, _version_time(match.group("token"))


def _listed_versions(client, bucket: str) -> dict[str, dict]:
    versions: dict[str, dict] = {}
    for root in SCAN_PREFIXES:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=root):
            for item in page.get("Contents", []):
                key = item["Key"]
                parsed = _version_from_key(key)
                if parsed is None:
                    continue
                version_prefix, partition_prefix, created_at = parsed
                state = versions.setdefault(
                    version_prefix,
                    {
                        "partition_prefix": partition_prefix,
                        "created_at": created_at,
                        "keys": [],
                        "markers": set(),
                    },
                )
                state["keys"].append(key)
                relative = key.removeprefix(f"{version_prefix}/")
                if "/" not in relative and relative in {
                    SUCCESS_FILE,
                    QUARANTINE_FILE,
                }:
                    state["markers"].add(relative)
    return versions


def _quarantine_failed_at(client, bucket: str, marker_key: str) -> datetime:
    body = client.get_object(Bucket=bucket, Key=marker_key)["Body"]
    try:
        payload = json.loads(body.read())
        raw = payload["failed_at"]
        failed_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"격리 marker의 failed_at을 읽을 수 없습니다: {marker_key}") from exc
    finally:
        body.close()
    if failed_at.tzinfo is None:
        raise ValueError(f"격리 marker의 failed_at에 시간대가 없습니다: {marker_key}")
    return failed_at.astimezone(timezone.utc)


def _cleanup_plan(client, bucket: str, cutoff: datetime) -> list[tuple[str, list[str]]]:
    versions = _listed_versions(client, bucket)
    completed_by_partition: dict[str, list[tuple[datetime, str]]] = defaultdict(list)

    for prefix, state in versions.items():
        markers = state["markers"]
        if markers == {SUCCESS_FILE, QUARANTINE_FILE}:
            raise ValueError(
                f"성공 marker와 격리 marker가 동시에 존재합니다: {prefix}"
            )
        if SUCCESS_FILE in markers:
            completed_by_partition[state["partition_prefix"]].append(
                (state["created_at"], prefix)
            )

    protected = {
        max(completed_versions)[1]
        for completed_versions in completed_by_partition.values()
    }
    candidates: list[tuple[str, list[str]]] = []
    for prefix, state in versions.items():
        markers = state["markers"]
        expired_success = (
            SUCCESS_FILE in markers
            and prefix not in protected
            and state["created_at"] <= cutoff
        )
        expired_quarantine = False
        if QUARANTINE_FILE in markers:
            marker_key = f"{prefix}/{QUARANTINE_FILE}"
            expired_quarantine = (
                _quarantine_failed_at(client, bucket, marker_key) <= cutoff
            )
        if expired_success or expired_quarantine:
            candidates.append((prefix, sorted(state["keys"])))
    return sorted(candidates)


def _delete_keys(client, bucket: str, keys: list[str]) -> int:
    deleted = 0
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[start : start + DELETE_BATCH_SIZE]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": key} for key in batch],
                "Quiet": True,
            },
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(
                "S3 객체 일부를 삭제하지 못했습니다: "
                + json.dumps(errors, ensure_ascii=False, sort_keys=True)
            )
        deleted += len(batch)
    return deleted


def _retention_cutoff(now: datetime | None, retention_days: int) -> datetime:
    if retention_days < 1:
        raise ValueError("retention_days는 1 이상이어야 합니다")
    reference = now or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        raise ValueError("now에는 시간대가 필요합니다")
    return reference.astimezone(timezone.utc) - timedelta(days=retention_days)


def cleanup_expired_versions(
    bucket: str,
    *,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    dry_run: bool = False,
    now: datetime | None = None,
    client=None,
) -> dict:
    """파티션별 최신 정상본은 남기고 만료된 버전 폴더를 삭제합니다."""
    cutoff = _retention_cutoff(now, retention_days)
    s3 = client or boto3.client("s3")
    plan = _cleanup_plan(s3, bucket, cutoff)
    candidate_prefixes = [prefix for prefix, _ in plan]
    candidate_object_count = sum(len(keys) for _, keys in plan)

    logger.info(
        "데이터 보존 기간 정리 계획: bucket=%s cutoff=%s dry_run=%s "
        "versions=%d objects=%d",
        bucket,
        cutoff.isoformat(),
        dry_run,
        len(plan),
        candidate_object_count,
    )
    if dry_run:
        for prefix in candidate_prefixes:
            logger.info("dry-run 삭제 후보: s3://%s/%s/", bucket, prefix)
        return {
            "candidate_version_prefixes": candidate_prefixes,
            "deleted_version_prefixes": [],
            "candidate_object_count": candidate_object_count,
            "deleted_object_count": 0,
        }

    deleted_objects = sum(
        _delete_keys(s3, bucket, keys) for _, keys in plan
    )
    return {
        "candidate_version_prefixes": candidate_prefixes,
        "deleted_version_prefixes": candidate_prefixes,
        "candidate_object_count": candidate_object_count,
        "deleted_object_count": deleted_objects,
    }


def cleanup_expired_gold_versions(
    dsn: str,
    *,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    dry_run: bool = False,
    now: datetime | None = None,
    connect=psycopg2.connect,
) -> dict:
    """RDS Gold의 지역·월별 최신본을 제외한 만료 버전을 삭제합니다."""
    cutoff = _retention_cutoff(now, retention_days)
    connection = connect(dsn)
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT service_area, year_month, version
                        FROM {GOLD_TABLES[0]}
                        UNION
                        SELECT service_area, year_month, version
                        FROM {GOLD_TABLES[1]}
                    ) data_versions
                    LEFT JOIN {GOLD_VERSION_TABLE} history
                    USING (service_area, year_month, version)
                    WHERE history.version IS NULL
                    """,
                    (),
                )
                missing_metadata = cursor.fetchone()[0]
                if missing_metadata:
                    raise RuntimeError(
                        "Gold 버전 생성 시각이 누락되었습니다. "
                        "RDS 메타데이터 마이그레이션을 먼저 실행해야 합니다: "
                        f"missing_versions={missing_metadata}"
                    )
                cursor.execute(
                    f"""
                    SELECT service_area, year_month, version
                    FROM (
                        SELECT service_area, year_month, version, created_at,
                               MAX(version) OVER (
                                   PARTITION BY service_area, year_month
                               ) AS latest_version
                        FROM {GOLD_VERSION_TABLE}
                    ) history
                    WHERE version < latest_version AND created_at <= %s
                    ORDER BY service_area, year_month, version
                    """,
                    (cutoff,),
                )
                candidates = [tuple(row) for row in cursor.fetchall()]
                logger.info(
                    "Gold 보존 기간 정리 계획: cutoff=%s dry_run=%s versions=%d",
                    cutoff.isoformat(),
                    dry_run,
                    len(candidates),
                )
                if dry_run:
                    for area, month, version in candidates:
                        logger.info(
                            "dry-run Gold 삭제 후보: service_area=%s "
                            "year_month=%s version=%s",
                            area,
                            month,
                            version,
                        )
                    return {
                        "candidate_versions": candidates,
                        "deleted_versions": [],
                        "deleted_row_count": 0,
                    }

                deleted_rows = 0
                for candidate in candidates:
                    for table in GOLD_TABLES:
                        cursor.execute(
                            f"DELETE FROM {table} "
                            "WHERE service_area = %s AND year_month = %s "
                            "AND version = %s",
                            candidate,
                        )
                        deleted_rows += cursor.rowcount
                    cursor.execute(
                        f"DELETE FROM {GOLD_VERSION_TABLE} "
                        "WHERE service_area = %s AND year_month = %s "
                        "AND version = %s",
                        candidate,
                    )
                return {
                    "candidate_versions": candidates,
                    "deleted_versions": candidates,
                    "deleted_row_count": deleted_rows,
                }
    finally:
        connection.close()


@task(task_id="cleanup_expired_s3_versions")
def cleanup_expired_s3_versions_task(**context) -> dict:
    bucket = os.getenv("DATA_LAKE_S3_BUCKET")
    if not bucket:
        raise ValueError("DATA_LAKE_S3_BUCKET 환경변수가 필요합니다")
    params = context["params"]
    result = cleanup_expired_versions(
        bucket,
        retention_days=params["retention_days"],
        dry_run=params["dry_run"],
    )
    return {
        "candidate_version_count": len(result["candidate_version_prefixes"]),
        "candidate_object_count": result["candidate_object_count"],
        "deleted_version_count": len(result["deleted_version_prefixes"]),
        "deleted_object_count": result["deleted_object_count"],
    }


@task(task_id="cleanup_expired_gold_versions")
def cleanup_expired_gold_versions_task(**context) -> dict:
    dsn = os.getenv("GOLD_DATABASE_URL")
    if not dsn:
        raise ValueError("GOLD_DATABASE_URL 환경변수가 필요합니다")
    params = context["params"]
    result = cleanup_expired_gold_versions(
        dsn,
        retention_days=params["retention_days"],
        dry_run=params["dry_run"],
    )
    return {
        "candidate_version_count": len(result["candidate_versions"]),
        "deleted_version_count": len(result["deleted_versions"]),
        "deleted_row_count": result["deleted_row_count"],
    }
