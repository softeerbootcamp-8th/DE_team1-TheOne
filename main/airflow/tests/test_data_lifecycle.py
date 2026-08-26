"""구버전과 격리 버전의 S3·Gold 정리 계약.

1. 최신 정상본 보호와 만료된 과거 정상본 삭제
2. 격리 시각 기준 만료와 버전 폴더 전체 삭제
3. dry-run 후보 계산과 실제 삭제 금지
4. 잘못된 marker·버전 경로의 명시적 실패
5. S3 배치 삭제·재실행·부분 실패 처리
6. Gold 최신본 보호·메타데이터·트랜잭션 처리
7. 보존기간 0일의 즉시 만료와 음수 거부
8. S3·Gold 판정과 실제 삭제 대상의 INFO 감사 로그
9. DAG 실행 및 파라미터 계약
"""

from datetime import datetime, timezone
from io import BytesIO
import json
import logging

import pytest

from main.airflow.scripts.data_lifecycle.tasks import (
    _retention_cutoff,
    cleanup_expired_gold_versions,
    cleanup_expired_versions,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 25, tzinfo=UTC)


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, delete_errors=None):
        self.objects = dict(objects)
        self.delete_errors = delete_errors or []
        self.delete_calls: list[list[str]] = []

    def get_paginator(self, operation_name):
        assert operation_name == "list_objects_v2"
        client = self

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                del Bucket
                keys = sorted(key for key in client.objects if key.startswith(Prefix))
                midpoint = len(keys) // 2
                for page_keys in (keys[:midpoint], keys[midpoint:]):
                    yield {"Contents": [{"Key": key} for key in page_keys]}

        return Paginator()

    def get_object(self, *, Bucket, Key):
        del Bucket
        return {"Body": BytesIO(self.objects[Key])}

    def delete_objects(self, *, Bucket, Delete):
        del Bucket
        keys = [item["Key"] for item in Delete["Objects"]]
        self.delete_calls.append(keys)
        for key in keys:
            self.objects.pop(key, None)
        return {"Errors": self.delete_errors}


class FakeGoldCursor:
    def __init__(self, candidates, *, fail_table=None, missing_metadata=0):
        self.candidates = candidates
        self.fail_table = fail_table
        self.missing_metadata = missing_metadata
        self.executions = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, parameters):
        self.executions.append((" ".join(sql.split()), parameters))
        if self.fail_table and sql.startswith(f"DELETE FROM {self.fail_table}"):
            raise RuntimeError("delete failed")
        self.rowcount = 1 if sql.startswith("DELETE FROM") else 0

    def fetchall(self):
        return self.candidates

    def fetchone(self):
        return (self.missing_metadata,)


class FakeGoldConnection:
    def __init__(self, candidates, *, fail_table=None, missing_metadata=0):
        self.cursor_instance = FakeGoldCursor(
            candidates,
            fail_table=fail_table,
            missing_metadata=missing_metadata,
        )
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.committed = exc_type is None
        self.rolled_back = exc_type is not None
        return False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def version(layer, dataset, area, month, kind, token):
    return (
        f"{layer}/{dataset}/service_area={area}/year_month={month}/"
        f"{kind}={token}"
    )


def quarantine(failed_at):
    return json.dumps({"failed_at": failed_at}).encode()


def gold_history(
    version_number=1,
    *,
    latest_version=2,
    is_delete_candidate=True,
    created_at=datetime(2026, 1, 1, tzinfo=UTC),
):
    return (
        "NYC",
        "2026-01",
        version_number,
        created_at,
        latest_version,
        is_delete_candidate,
    )


def test_파티션별_최신_정상본은_90일이_지나도_보존하고_구버전만_삭제한다():
    old = version("bronze", "trips", "NYC", "2025-01", "collected_at", "20260527T000000000000Z")
    latest = version("bronze", "trips", "NYC", "2025-01", "collected_at", "20260528T000000000000Z")
    other_area = version("bronze", "trips", "TX", "2025-01", "collected_at", "20250101T000000000000Z")
    client = FakeS3(
        {
            f"{old}/data.parquet": b"old",
            f"{old}/_SUCCESS": b"",
            f"{latest}/data.parquet": b"latest",
            f"{latest}/_SUCCESS": b"",
            f"{other_area}/data.parquet": b"only",
            f"{other_area}/_SUCCESS": b"",
        }
    )

    result = cleanup_expired_versions("lake", client=client, now=NOW)

    assert result["deleted_version_prefixes"] == [old]
    assert set(client.objects) == {
        f"{latest}/data.parquet",
        f"{latest}/_SUCCESS",
        f"{other_area}/data.parquet",
        f"{other_area}/_SUCCESS",
    }


def test_보존기간_0일은_최신_정상본만_남기고_과거_S3_버전을_즉시_삭제한다(
    caplog,
):
    old = version(
        "silver", "monthly_taxi_trip", "NYC", "2026-01",
        "source_collected_at", "20260824T143445353239Z",
    )
    latest = version(
        "silver", "monthly_taxi_trip", "NYC", "2026-01",
        "source_collected_at", "20260824T153445353239Z",
    )
    client = FakeS3(
        {
            f"{old}/part-00000.parquet": b"old",
            f"{old}/_SUCCESS": b"",
            f"{latest}/part-00000.parquet": b"latest",
            f"{latest}/_SUCCESS": b"",
        }
    )

    with caplog.at_level(
        logging.INFO,
        logger="main.airflow.scripts.data_lifecycle.tasks",
    ):
        result = cleanup_expired_versions(
            "lake",
            retention_days=0,
            client=client,
            now=NOW,
        )

    assert result["deleted_version_prefixes"] == [old]
    assert all(not key.startswith(f"{old}/") for key in client.objects)
    assert f"{latest}/_SUCCESS" in client.objects
    assert "retention_days=0" in caplog.text
    assert f"decision=delete_candidate reason=expired_success prefix=s3://lake/{old}/" in caplog.text
    assert f"decision=protect reason=latest_success prefix=s3://lake/{latest}/" in caplog.text
    assert f"delete_start prefix=s3://lake/{old}/ objects=2" in caplog.text
    assert f"delete_complete prefix=s3://lake/{old}/ deleted_objects=2" in caplog.text
    assert "complete candidate_versions=1 candidate_objects=2 deleted_versions=1 deleted_objects=2" in caplog.text


def test_보존기간은_0을_허용하고_음수는_거부한다():
    assert _retention_cutoff(NOW, 0) == NOW

    with pytest.raises(ValueError, match="0 이상"):
        _retention_cutoff(NOW, -1)


def test_경계일을_포함한_오래된_격리_폴더만_전체_삭제한다():
    expired = version("silver", "trips", "NYC", "2026-01", "source_collected_at", "20260101T000000000000Z")
    recent = version("silver", "trips", "NYC", "2026-02", "source_collected_at", "20260201T000000000000Z")
    client = FakeS3(
        {
            f"{expired}/part-00000.parquet": b"bad",
            f"{expired}/nested/debug.txt": b"debug",
            f"{expired}/_QUARANTINED.json": quarantine("2026-05-27T00:00:00+00:00"),
            f"{recent}/data.parquet": b"bad",
            f"{recent}/_QUARANTINED.json": quarantine("2026-05-27T00:00:01+00:00"),
        }
    )

    result = cleanup_expired_versions("lake", client=client, now=NOW)

    assert result["deleted_version_prefixes"] == [expired]
    assert all(not key.startswith(f"{expired}/") for key in client.objects)
    assert f"{recent}/_QUARANTINED.json" in client.objects


def test_dry_run은_후보와_판정로그만_남기고_삭제하지_않는다(caplog):
    expired = version("silver", "trips", "NYC", "2025-01", "source_collected_at", "20250101T000000000000Z")
    client = FakeS3(
        {
            f"{expired}/data.parquet": b"bad",
            f"{expired}/_QUARANTINED.json": quarantine("2025-01-02T00:00:00Z"),
        }
    )

    with caplog.at_level(
        logging.INFO,
        logger="main.airflow.scripts.data_lifecycle.tasks",
    ):
        result = cleanup_expired_versions("lake", client=client, now=NOW, dry_run=True)

    assert result["candidate_version_prefixes"] == [expired]
    assert result["deleted_version_prefixes"] == []
    assert client.delete_calls == []
    assert f"plan prefix=s3://lake/{expired}/ objects=2 dry_run=True" in caplog.text
    assert "delete_start" not in caplog.text
    assert "deleted_versions=0 deleted_objects=0 dry_run=True" in caplog.text


def test_성공과_격리_marker가_함께_있으면_삭제_전에_실패한다():
    invalid = version("bronze", "trips", "NYC", "2025-01", "collected_at", "20250101T000000000000Z")
    client = FakeS3(
        {
            f"{invalid}/data.parquet": b"bad",
            f"{invalid}/_SUCCESS": b"",
            f"{invalid}/_QUARANTINED.json": quarantine("2025-01-02T00:00:00Z"),
        }
    )

    with pytest.raises(ValueError, match="동시에 존재"):
        cleanup_expired_versions("lake", client=client, now=NOW)

    assert client.delete_calls == []


def test_해석할_수_없는_버전_경로는_삭제_전에_실패한다():
    malformed = "bronze/trips/service_area=NYC/year_month=2025-01/collected_at=bad"
    client = FakeS3({f"{malformed}/_SUCCESS": b""})

    with pytest.raises(ValueError, match="버전 경로 형식"):
        cleanup_expired_versions("lake", client=client, now=NOW)

    assert client.delete_calls == []


def test_객체는_1000개씩_삭제하고_재실행은_noop이다():
    expired = version("silver", "trips", "NYC", "2025-01", "source_collected_at", "20250101T000000000000Z")
    objects = {f"{expired}/part-{index:05d}.parquet": b"bad" for index in range(1000)}
    objects[f"{expired}/_QUARANTINED.json"] = quarantine("2025-01-02T00:00:00Z")
    client = FakeS3(objects)

    first = cleanup_expired_versions("lake", client=client, now=NOW)
    second = cleanup_expired_versions("lake", client=client, now=NOW)

    assert list(map(len, client.delete_calls)) == [1000, 1]
    assert first["deleted_object_count"] == 1001
    assert second["candidate_version_prefixes"] == []


def test_S3_부분_삭제_오류를_실패로_처리한다():
    expired = version("silver", "trips", "NYC", "2025-01", "source_collected_at", "20250101T000000000000Z")
    client = FakeS3(
        {
            f"{expired}/data.parquet": b"bad",
            f"{expired}/_QUARANTINED.json": quarantine("2025-01-02T00:00:00Z"),
        },
        delete_errors=[{"Key": f"{expired}/data.parquet", "Code": "AccessDenied"}],
    )

    with pytest.raises(RuntimeError, match="AccessDenied"):
        cleanup_expired_versions("lake", client=client, now=NOW)


def test_Gold는_최신본을_보호하고_0일_만료_버전을_테이블별_로그와_함께_삭제한다(
    caplog,
):
    connection = FakeGoldConnection(
        [
            gold_history(),
            gold_history(
                version_number=2,
                latest_version=2,
                is_delete_candidate=False,
            ),
        ]
    )

    with caplog.at_level(
        logging.INFO,
        logger="main.airflow.scripts.data_lifecycle.tasks",
    ):
        result = cleanup_expired_gold_versions(
            "postgresql://gold",
            retention_days=0,
            now=NOW,
            connect=lambda dsn: connection,
        )

    sql = [statement for statement, _ in connection.cursor_instance.executions]
    assert all(f"FROM {table}" in sql[0] for table in (
        "driver_aggregation",
        "driver_car_suggestion",
        "silver_lineage",
    ))
    assert "MAX(version) OVER" in sql[1]
    assert "PARTITION BY service_area, year_month" in sql[1]
    assert "version < latest_version" in sql[1]
    assert "created_at <= %s" in sql[1]
    assert [statement for statement in sql if statement.startswith("DELETE FROM")] == [
        "DELETE FROM driver_aggregation WHERE service_area = %s AND year_month = %s AND version = %s",
        "DELETE FROM driver_car_suggestion WHERE service_area = %s AND year_month = %s AND version = %s",
        "DELETE FROM silver_lineage WHERE service_area = %s AND year_month = %s AND version = %s",
        "DELETE FROM gold_load_versions WHERE service_area = %s AND year_month = %s AND version = %s",
    ]
    assert result["deleted_versions"] == [("NYC", "2026-01", 1)]
    assert connection.committed is True
    assert connection.closed is True
    candidate = "service_area=NYC year_month=2026-01 version=1"
    assert "gold_lifecycle start retention_days=0" in caplog.text
    assert f"decision=delete_candidate reason=expired_old_version {candidate}" in caplog.text
    assert "decision=protect reason=latest_version service_area=NYC year_month=2026-01 version=2" in caplog.text
    for table in (
        "driver_aggregation",
        "driver_car_suggestion",
        "silver_lineage",
    ):
        assert f"delete_table {candidate} table={table} deleted_rows=1" in caplog.text
    assert f"delete_complete {candidate} deleted_rows=3" in caplog.text
    assert "complete candidate_versions=1 deleted_versions=1 deleted_rows=3" in caplog.text


def test_Gold_기존_버전의_메타데이터가_빠지면_삭제하지_않는다():
    connection = FakeGoldConnection([], missing_metadata=1)

    with pytest.raises(RuntimeError, match="마이그레이션"):
        cleanup_expired_gold_versions(
            "postgresql://gold",
            now=NOW,
            connect=lambda dsn: connection,
        )

    assert connection.rolled_back is True
    assert not any(
        sql.startswith("DELETE FROM")
        for sql, _ in connection.cursor_instance.executions
    )


def test_Gold_dry_run과_재실행은_삭제하지_않는다():
    dry_run_connection = FakeGoldConnection([gold_history()])
    empty_connection = FakeGoldConnection([])

    dry_run = cleanup_expired_gold_versions(
        "postgresql://gold",
        now=NOW,
        dry_run=True,
        connect=lambda dsn: dry_run_connection,
    )
    rerun = cleanup_expired_gold_versions(
        "postgresql://gold",
        now=NOW,
        connect=lambda dsn: empty_connection,
    )

    assert dry_run["candidate_versions"] == [("NYC", "2026-01", 1)]
    assert dry_run["deleted_versions"] == []
    assert rerun["candidate_versions"] == []
    assert not any(
        sql.startswith("DELETE FROM")
        for sql, _ in dry_run_connection.cursor_instance.executions
    )


def test_Gold_마지막_테이블_삭제가_실패하면_트랜잭션을_롤백한다():
    connection = FakeGoldConnection(
        [gold_history()],
        fail_table="silver_lineage",
    )

    with pytest.raises(RuntimeError, match="delete failed"):
        cleanup_expired_gold_versions(
            "postgresql://gold",
            now=NOW,
            connect=lambda dsn: connection,
        )

    assert connection.rolled_back is True
    assert connection.closed is True


def test_DAG는_매일_한번만_동시에_실행한다():
    from dags.data_lifecycle_dag import data_lifecycle_dag

    assert data_lifecycle_dag.schedule == "0 3 * * *"
    assert data_lifecycle_dag.catchup is False
    assert data_lifecycle_dag.max_active_runs == 1
    retention = data_lifecycle_dag.params.get_param("retention_days")
    assert retention.schema["minimum"] == 0
    assert {task.task_id for task in data_lifecycle_dag.tasks} == {
        "cleanup_expired_s3_versions",
        "cleanup_expired_gold_versions",
    }
