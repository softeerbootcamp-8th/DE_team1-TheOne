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
GOLD_TABLES = (
    "driver_aggregation",
    "driver_car_suggestion",
    "silver_lineage",
)
GOLD_VERSION_TABLE = "gold_load_versions"
VERSION_SEGMENT_PATTERN = re.compile(
    r"^(?:source_)?collected_at=(?P<token>\d{8}T\d{12}Z)$"
)


def _audit(layer: str, event: str, *, level=logging.INFO, **fields) -> None:
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    logger.log(level, "%s_lifecycle %s %s", layer, event, details)


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
            _audit(
                "s3", "decision", level=logging.ERROR, decision="error",
                reason="conflicting_markers", prefix=f"s3://{bucket}/{prefix}/",
                markers=",".join(sorted(markers)),
            )
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
    _audit(
        "s3", "scan_complete", bucket=bucket, versions=len(versions),
        partitions=len({state["partition_prefix"] for state in versions.values()}),
        protected_latest=len(protected),
    )
    candidates: list[tuple[str, list[str]]] = []
    for prefix, state in versions.items():
        markers = state["markers"]
        decision = "keep"
        reason = "no_marker"
        age_reference = state["created_at"]
        is_candidate = False
        if prefix in protected:
            decision = "protect"
            reason = "latest_success"
        elif SUCCESS_FILE in markers and state["created_at"] <= cutoff:
            decision = "delete_candidate"
            reason = "expired_success"
            is_candidate = True
        elif SUCCESS_FILE in markers:
            reason = "retention_active_success"
        elif QUARANTINE_FILE in markers:
            marker_key = f"{prefix}/{QUARANTINE_FILE}"
            age_reference = _quarantine_failed_at(client, bucket, marker_key)
            if age_reference <= cutoff:
                decision = "delete_candidate"
                reason = "expired_quarantine"
                is_candidate = True
            else:
                reason = "retention_active_quarantine"
        _audit(
            "s3", "decision", decision=decision, reason=reason,
            prefix=f"s3://{bucket}/{prefix}/",
            age_reference=age_reference.isoformat(),
            markers=",".join(sorted(markers)) or "none", objects=len(state["keys"]),
        )
        if is_candidate:
            candidates.append((prefix, sorted(state["keys"])))
    return sorted(candidates)


def _delete_keys(
    client,
    bucket: str,
    version_prefix: str,
    keys: list[str],
) -> int:
    deleted = 0
    batch_count = (len(keys) + DELETE_BATCH_SIZE - 1) // DELETE_BATCH_SIZE
    for start in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[start : start + DELETE_BATCH_SIZE]
        batch_number = start // DELETE_BATCH_SIZE + 1
        _audit(
            "s3", "delete_batch_start",
            prefix=f"s3://{bucket}/{version_prefix}/",
            batch=f"{batch_number}/{batch_count}", objects=len(batch),
        )
        response = client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": key} for key in batch],
                "Quiet": True,
            },
        )
        errors = response.get("Errors", [])
        if errors:
            _audit(
                "s3", "delete_batch_failed", level=logging.ERROR,
                prefix=f"s3://{bucket}/{version_prefix}/",
                batch=f"{batch_number}/{batch_count}",
                errors=json.dumps(errors, ensure_ascii=False, sort_keys=True),
            )
            raise RuntimeError(
                "S3 객체 일부를 삭제하지 못했습니다: "
                + json.dumps(errors, ensure_ascii=False, sort_keys=True)
            )
        deleted += len(batch)
        _audit(
            "s3", "delete_batch_complete",
            prefix=f"s3://{bucket}/{version_prefix}/",
            batch=f"{batch_number}/{batch_count}", deleted_objects=len(batch),
        )
    return deleted


def _retention_cutoff(now: datetime | None, retention_days: int) -> datetime:
    if retention_days < 0:
        raise ValueError("retention_days는 0 이상이어야 합니다")
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
    _audit(
        "s3", "start", bucket=bucket, retention_days=retention_days,
        cutoff=cutoff.isoformat(), dry_run=dry_run,
        scan_prefixes=",".join(SCAN_PREFIXES),
    )
    plan = _cleanup_plan(s3, bucket, cutoff)
    candidate_prefixes = [prefix for prefix, _ in plan]
    candidate_object_count = sum(len(keys) for _, keys in plan)

    for prefix, keys in plan:
        _audit(
            "s3", "plan", prefix=f"s3://{bucket}/{prefix}/",
            objects=len(keys), dry_run=dry_run,
        )
    if dry_run:
        result = {
            "candidate_version_prefixes": candidate_prefixes,
            "deleted_version_prefixes": [],
            "candidate_object_count": candidate_object_count,
            "deleted_object_count": 0,
        }
    else:
        deleted_objects = 0
        for prefix, keys in plan:
            _audit(
                "s3", "delete_start", prefix=f"s3://{bucket}/{prefix}/",
                objects=len(keys),
            )
            deleted_for_version = _delete_keys(s3, bucket, prefix, keys)
            deleted_objects += deleted_for_version
            _audit(
                "s3", "delete_complete", prefix=f"s3://{bucket}/{prefix}/",
                deleted_objects=deleted_for_version,
            )
        result = {
            "candidate_version_prefixes": candidate_prefixes,
            "deleted_version_prefixes": candidate_prefixes,
            "candidate_object_count": candidate_object_count,
            "deleted_object_count": deleted_objects,
        }
    _audit(
        "s3", "complete",
        candidate_versions=len(result["candidate_version_prefixes"]),
        candidate_objects=result["candidate_object_count"],
        deleted_versions=len(result["deleted_version_prefixes"]),
        deleted_objects=result["deleted_object_count"], dry_run=dry_run,
    )
    return result


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
    _audit(
        "gold", "start", retention_days=retention_days,
        cutoff=cutoff.isoformat(), dry_run=dry_run, tables=",".join(GOLD_TABLES),
    )
    connection = connect(dsn)
    try:
        with connection:
            with connection.cursor() as cursor:
                data_versions_sql = "\nUNION\n".join(
                    "SELECT service_area, year_month, version "
                    f"FROM {table}"
                    for table in GOLD_TABLES
                )
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        {data_versions_sql}
                    ) data_versions
                    LEFT JOIN {GOLD_VERSION_TABLE} history
                    USING (service_area, year_month, version)
                    WHERE history.version IS NULL
                    """,
                    (),
                )
                missing_metadata = cursor.fetchone()[0]
                if missing_metadata:
                    _audit(
                        "gold", "decision", level=logging.ERROR, decision="error",
                        reason="missing_metadata", missing_versions=missing_metadata,
                    )
                    raise RuntimeError(
                        "Gold 버전 생성 시각이 누락되었습니다. "
                        "RDS 메타데이터 마이그레이션을 먼저 실행해야 합니다: "
                        f"missing_versions={missing_metadata}"
                    )
                cursor.execute(
                    f"""
                    SELECT service_area, year_month, version, created_at,
                           latest_version,
                           version < latest_version AND created_at <= %s
                               AS is_delete_candidate
                    FROM (
                        SELECT service_area, year_month, version, created_at,
                               MAX(version) OVER (
                                   PARTITION BY service_area, year_month
                               ) AS latest_version
                        FROM {GOLD_VERSION_TABLE}
                    ) history
                    ORDER BY service_area, year_month, version
                    """,
                    (cutoff,),
                )
                history_rows = [tuple(row) for row in cursor.fetchall()]
                candidates = []
                for area, month, version, created_at, latest_version, expired in history_rows:
                    if version == latest_version:
                        decision = "protect"
                        reason = "latest_version"
                    elif expired:
                        decision = "delete_candidate"
                        reason = "expired_old_version"
                        candidates.append((area, month, version))
                    else:
                        decision = "keep"
                        reason = "retention_active"
                    _audit(
                        "gold", "decision", decision=decision, reason=reason,
                        service_area=area, year_month=month, version=version,
                        created_at=created_at.isoformat(), latest_version=latest_version,
                    )
                _audit(
                    "gold", "scan_complete", history_versions=len(history_rows),
                    candidate_versions=len(candidates),
                )
                for area, month, version in candidates:
                    _audit(
                        "gold", "plan", service_area=area, year_month=month,
                        version=version, dry_run=dry_run,
                    )
                if dry_run:
                    result = {
                        "candidate_versions": candidates,
                        "deleted_versions": [],
                        "deleted_row_count": 0,
                    }
                else:
                    deleted_rows = 0
                    for candidate in candidates:
                        area, month, version = candidate
                        _audit(
                            "gold", "delete_start", service_area=area,
                            year_month=month, version=version,
                        )
                        candidate_deleted_rows = 0
                        for table in GOLD_TABLES:
                            cursor.execute(
                                f"DELETE FROM {table} "
                                "WHERE service_area = %s AND year_month = %s "
                                "AND version = %s",
                                candidate,
                            )
                            candidate_deleted_rows += cursor.rowcount
                            _audit(
                                "gold", "delete_table", service_area=area,
                                year_month=month, version=version, table=table,
                                deleted_rows=cursor.rowcount,
                            )
                        cursor.execute(
                            f"DELETE FROM {GOLD_VERSION_TABLE} "
                            "WHERE service_area = %s AND year_month = %s "
                            "AND version = %s",
                            candidate,
                        )
                        _audit(
                            "gold", "delete_metadata", service_area=area,
                            year_month=month, version=version,
                            table=GOLD_VERSION_TABLE, deleted_rows=cursor.rowcount,
                        )
                        deleted_rows += candidate_deleted_rows
                        _audit(
                            "gold", "delete_complete", service_area=area,
                            year_month=month, version=version,
                            deleted_rows=candidate_deleted_rows,
                        )
                    result = {
                        "candidate_versions": candidates,
                        "deleted_versions": candidates,
                        "deleted_row_count": deleted_rows,
                    }
        _audit(
            "gold", "complete", candidate_versions=len(result["candidate_versions"]),
            deleted_versions=len(result["deleted_versions"]),
            deleted_rows=result["deleted_row_count"], dry_run=dry_run,
        )
        return result
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
