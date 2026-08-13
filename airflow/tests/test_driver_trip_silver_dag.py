"""기사 배정 운행 Silver 월간 DAG 시나리오. 이슈 #301.

1. validate_inputs -> build_driver_trip_silver -> validate_silver 순서
2. 직전 달·수동 연월과 snapshot_date 파라미터 전달
3. Spark 명령에 모든 입력·출력·seed 포함
4. 입력 경로 누락, 출력 0행·스키마·키·관계·계약·월 오류 차단
5. 월간 운영 설정과 실패 콜백 적용
"""

from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import hvfhv_driver_trip_silver_dag as module

DAG = module.hvfhv_driver_trip_silver_dag


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _row(**overrides):
    row = {
        "trip_key": "t1", "driver_id": "d1", "customer_id": "c1", "lease_id": "l1",
        "taxi_id": "x1", "pickup_datetime": datetime(2024, 3, 4, 9),
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2025, 1, 1),
        "year_month": "2024-03", "snapshot_date": date(2024, 3, 1),
        "assignment_seed": 42, "assignment_version": "v1",
        "trip_sequence": 1, "deadhead_minutes": 0.0, "preference_score": 0.9,
        "make_key": "Toyota", "model_key": "Camry", "model_year": 2023,
    }
    row.update(overrides)
    return row


def test_DAG_구조와_월간_운영설정이_올바르다():
    assert DAG.dag_id == "hvfhv_driver_trip_silver_pipeline"
    assert set(DAG.task_ids) == {"validate_inputs", "build_driver_trip_silver", "validate_silver"}
    assert DAG.get_task("validate_inputs").downstream_task_ids == {"build_driver_trip_silver"}
    assert DAG.get_task("build_driver_trip_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.catchup is False and DAG.max_active_runs == 1
    assert DAG.schedule == "0 1 12 * *"
    assert all(task.on_failure_callback for task in DAG.tasks)


def test_직전달과_수동연월을_계산한다():
    assert module.resolve_target_year_month(datetime(2024, 1, 12, tzinfo=timezone.utc), {}) == "2023-12"
    assert module.resolve_target_year_month(datetime(2024, 1, 12), {"year": "2030", "month": "3"}) == "2030-03"


def test_Spark_명령에_모든_경로와_실행계보가_들어간다():
    command = DAG.get_task("build_driver_trip_silver").bash_command
    for option in (
        "--trips_path", "--preferences_path", "--customers_path", "--leases_path",
        "--taxis_path", "--travel_times_path", "--output_path", "--year_month",
        "--snapshot_date", "--seed",
    ):
        assert option in command
    assert "xcom_pull(task_ids='validate_inputs')['year_month']" in command


def test_validate_inputs는_경로가_모두_있어야_계보를_반환한다(tmp_path):
    paths = {}
    for name in ("trips", "preferences", "company", "travel_times"):
        path = tmp_path / name
        path.mkdir()
        paths[f"{name}_path"] = str(path)
    (tmp_path / "trips" / "year_month=2024-03").mkdir()
    company = tmp_path / "company" / "snapshot_date=2024-03-01"
    company.mkdir()
    for filename in ("customer.parquet", "lease_contract.parquet", "taxi.parquet"):
        (company / filename).touch()

    result = module.validate_input_paths("2024-03", "2024-03-01", paths)

    assert result == {"year_month": "2024-03", "snapshot_date": "2024-03-01"}
    (tmp_path / "trips" / "year_month=2024-03").rmdir()
    with pytest.raises(FileNotFoundError):
        module.validate_input_paths("2024-03", "2024-03-01", paths)


@pytest.mark.parametrize("violation", ["empty", "missing_column", "duplicate", "null_fk", "contract", "month"])
def test_validate_silver는_잘못된_출력을_거부한다(tmp_path, violation):
    partition = tmp_path / "year_month=2024-03"
    rows = [_row()]
    if violation == "empty":
        rows = []
    elif violation == "missing_column":
        rows[0].pop("assignment_version")
    elif violation == "duplicate":
        rows.append(dict(rows[0]))
    elif violation == "null_fk":
        rows[0]["customer_id"] = None
    elif violation == "contract":
        rows[0]["lease_ended_on"] = date(2024, 3, 4)
    else:
        rows[0]["year_month"] = "2024-02"
    _write(partition / "part.parquet", rows)

    with pytest.raises(ValueError):
        module.validate_silver_partition(tmp_path, "2024-03")


def test_validate_silver는_정상_파티션과_다른월_보존을_확인한다(tmp_path):
    _write(tmp_path / "year_month=2024-02" / "part.parquet", [_row(year_month="2024-02")])
    _write(tmp_path / "year_month=2024-03" / "part.parquet", [_row()])

    module.validate_silver_partition(tmp_path, "2024-03")
