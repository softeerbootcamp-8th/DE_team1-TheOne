"""기사 운행 이력 Silver 월간 DAG 시나리오. 이슈 #301, #456.

1. validate_inputs -> build_driver_trip_silver -> validate_silver 순서
2. 직전 달·수동 연월과 snapshot_date 파라미터 전달
3. Spark 명령에 두 Clean Silver 입력·출력·계보 포함
4. 입력 파티션 누락, 출력 0행·스키마·키·관계·계약·월 오류 차단
5. 월간 운영 설정과 실패 콜백 적용
6. snapshot_date 를 안 주면 실제 존재하는 파티션 중 최신을 고름 — 전에는 대상 월의
   1일(`{year_month}-01`)을 찾아, 대상 월과 무관하게 만들어지는 회사 원천 픽스처와
   어긋나 매번 실패했음
"""

from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import hvfhv_driver_trip_silver_dag as dag_module
from scripts.hvfhv_driver_trip_silver import tasks as task_module

DAG = dag_module.hvfhv_driver_trip_silver_dag


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _row(**overrides):
    row = {
        "trip_key": "t1", "driver_id": "d1", "customer_id": "c1", "lease_id": "l1",
        "taxi_id": "x1", "pickup_datetime": datetime(2024, 3, 4, 9),
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2025, 1, 1),
        "year_month": "2024-03", "snapshot_date": date(2024, 3, 1),
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
    # 리스 Silver 는 `year_month` 파티션으로 읽습니다. snapshot_date 를 파라미터로
    # 두면 아무 경로도 고르지 않으면서 계보 컬럼만 틀리게 찍히고, 실패 없이 통과합니다.
    assert "snapshot_date" not in DAG.params


def test_수동으로_넘긴_연월이_최우선이다():
    assert task_module.resolve_target_year_month(
        datetime(2024, 1, 12), {"year": "2030", "month": "3"}
    ) == "2030-03"


def make_partitions(trips_path, year_months):
    trips_path.mkdir(parents=True, exist_ok=True)
    for year_month in year_months:
        (trips_path / f"year_month={year_month}").mkdir()
    return trips_path


def test_달력이_아니라_있는_파티션_중_최신을_고른다(tmp_path):
    """TLC 가 두 달쯤 늦게 공개해 직전 달 파티션은 존재한 적이 없습니다.

    달력으로 계산하면 매달 같은 자리에서 FileNotFoundError 로 죽습니다.
    """
    trips = make_partitions(tmp_path / "trips", ["2026-04", "2026-05", "2026-06"])

    resolved = task_module.resolve_target_year_month(
        datetime(2026, 8, 12, tzinfo=timezone.utc), {}, str(trips)
    )

    assert resolved == "2026-06"  # 직전 달인 2026-07 이 아님


def test_기준일_직전달을_넘는_파티션은_고르지_않는다(tmp_path):
    """과거로 백필할 때 그때 없던 달이 섞이면 결과를 재현할 수 없습니다."""
    trips = make_partitions(tmp_path / "trips", ["2026-04", "2026-05", "2026-06"])

    resolved = task_module.resolve_target_year_month(
        datetime(2026, 6, 12, tzinfo=timezone.utc), {}, str(trips)
    )

    assert resolved == "2026-05"


def test_쓸_수_있는_파티션이_없으면_무엇이_있는지_알려준다(tmp_path):
    trips = make_partitions(tmp_path / "trips", ["2026-09"])

    with pytest.raises(FileNotFoundError, match=r"2026-09"):
        task_module.resolve_target_year_month(
            datetime(2026, 8, 12, tzinfo=timezone.utc), {}, str(trips)
        )


def test_경로를_안_주면_예전처럼_직전달을_쓴다():
    """`trips_path` 없이 부르는 호출부가 있어도 동작이 바뀌지 않게 둡니다."""
    assert task_module.resolve_target_year_month(
        datetime(2024, 1, 12, tzinfo=timezone.utc), {}
    ) == "2023-12"


def test_Spark_명령에_모든_경로와_실행계보가_들어간다():
    command = DAG.get_task("build_driver_trip_silver").bash_command
    for option in (
        "--trips_path", "--leases_path", "--output_path", "--year_month",
        "--snapshot_date",
    ):
        assert option in command
    assert "xcom_pull(task_ids='validate_inputs')['year_month']" in command
    # 배정이 사라져 seed 로 갈리는 결과가 없습니다. 인자가 남아 있으면 그 값이
    # 무언가를 바꾼다고 읽힙니다.
    assert "--seed" not in command


@pytest.mark.parametrize("missing", ["trips", "leases"])
def test_validate_inputs는_두_파티션이_모두_있어야_계보를_반환한다(tmp_path, missing):
    paths = {}
    for name in ("trips", "leases"):
        partition = tmp_path / name / "year_month=2024-03"
        partition.mkdir(parents=True)
        paths[f"{name}_path"] = str(tmp_path / name)

    result = task_module.validate_input_paths("2024-03", "2024-03-01", paths)

    assert result == {"year_month": "2024-03", "snapshot_date": "2024-03-01"}
    (tmp_path / missing / "year_month=2024-03").rmdir()
    with pytest.raises(FileNotFoundError, match=f"{missing}_path"):
        task_module.validate_input_paths("2024-03", "2024-03-01", paths)


@pytest.mark.parametrize("violation", ["empty", "missing_column", "duplicate", "null_fk", "contract", "month"])
def test_validate_silver는_잘못된_출력을_거부한다(tmp_path, violation):
    partition = tmp_path / "year_month=2024-03"
    rows = [_row()]
    if violation == "empty":
        rows = []
    elif violation == "missing_column":
        rows[0].pop("model_key")
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
        task_module.validate_silver_partition(tmp_path, "2024-03")


def test_validate_silver는_정상_파티션과_다른월_보존을_확인한다(tmp_path):
    _write(tmp_path / "year_month=2024-02" / "part.parquet", [_row(year_month="2024-02")])
    _write(tmp_path / "year_month=2024-03" / "part.parquet", [_row()])

    task_module.validate_silver_partition(tmp_path, "2024-03")


