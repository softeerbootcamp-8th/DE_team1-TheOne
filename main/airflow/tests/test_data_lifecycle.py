"""90일이 지난 구버전과 격리 버전의 S3 정리 계약."""

from datetime import datetime, timezone
from io import BytesIO
import json

import pytest

from main.airflow.scripts.data_lifecycle.tasks import (
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


def test_dry_run은_후보만_계산하고_삭제하지_않는다():
    expired = version("silver", "trips", "NYC", "2025-01", "source_collected_at", "20250101T000000000000Z")
    client = FakeS3(
        {
            f"{expired}/data.parquet": b"bad",
            f"{expired}/_QUARANTINED.json": quarantine("2025-01-02T00:00:00Z"),
        }
    )

    result = cleanup_expired_versions("lake", client=client, now=NOW, dry_run=True)

    assert result["candidate_version_prefixes"] == [expired]
    assert result["deleted_version_prefixes"] == []
    assert client.delete_calls == []


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


def test_Gold는_지역월별_최신본을_제외한_90일_초과_버전을_두_테이블에서_삭제한다():
    connection = FakeGoldConnection([("NYC", "2026-01", 1)])

    result = cleanup_expired_gold_versions(
        "postgresql://gold",
        now=NOW,
        connect=lambda dsn: connection,
    )

    sql = [statement for statement, _ in connection.cursor_instance.executions]
    assert "MAX(version) OVER" in sql[1]
    assert "PARTITION BY service_area, year_month" in sql[1]
    assert "version < latest_version" in sql[1]
    assert "created_at <= %s" in sql[1]
    assert [statement for statement in sql if statement.startswith("DELETE FROM")] == [
        "DELETE FROM driver_aggregation WHERE service_area = %s AND year_month = %s AND version = %s",
        "DELETE FROM driver_car_suggestion WHERE service_area = %s AND year_month = %s AND version = %s",
        "DELETE FROM gold_load_versions WHERE service_area = %s AND year_month = %s AND version = %s",
    ]
    assert result["deleted_versions"] == [("NYC", "2026-01", 1)]
    assert connection.committed is True
    assert connection.closed is True


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
    dry_run_connection = FakeGoldConnection([("NYC", "2026-01", 1)])
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


def test_Gold_테이블_삭제_중_실패하면_트랜잭션을_롤백한다():
    connection = FakeGoldConnection(
        [("NYC", "2026-01", 1)],
        fail_table="driver_car_suggestion",
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

    assert data_lifecycle_dag.schedule == "@daily"
    assert data_lifecycle_dag.catchup is False
    assert data_lifecycle_dag.max_active_runs == 1
    assert {task.task_id for task in data_lifecycle_dag.tasks} == {
        "cleanup_expired_s3_versions",
        "cleanup_expired_gold_versions",
    }
