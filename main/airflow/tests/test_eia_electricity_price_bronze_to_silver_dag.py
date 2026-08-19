"""EIA 전력요금 Bronze→Silver DAG 계약. 이슈 #512.

1. 월간 스케줄과 세 단계 순서 — 원본 확인이 변환보다 먼저
2. 지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달을 고름
3. 변환만 재시도 — 확인·검증은 다시 봐도 결과가 같아 재시도가 실패를 늦추기만 함
4. 산출물 검증이 스키마·행 수·날짜 완결성을 본다. 하루라도 비면 하류 일자 조인에서
   그 날이 통째로 빠지는데, 실패가 아니라 조용히 줄어든 집계로 나타남
"""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import eia_electricity_price_bronze_to_silver_dag as dag_module
from main.airflow.scripts.eia_electricity_price_bronze_to_silver import tasks as task_module
from schema.silver import CLEAN_EV_CHARGING_PRICE_SCHEMA


DAG = dag_module.eia_electricity_price_bronze_to_silver_dag


def _write(path, rows, schema=CLEAN_EV_CHARGING_PRICE_SCHEMA):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _march(day_count=31):
    from datetime import date

    return [
        {"date": date(2024, 3, day), "ev_price": 0.28}
        for day in range(1, day_count + 1)
    ]


def test_DAG는_월간_스케줄로_확인_변환_검증을_순서대로_처리한다():
    assert DAG.dag_id == "eia_electricity_price_bronze_to_silver_pipeline"
    assert DAG.schedule == "0 7 1 * *"
    assert set(DAG.task_ids) == {"check_bronze", "bronze_to_silver", "validate_silver"}
    assert DAG.get_task("check_bronze").downstream_task_ids == {"bronze_to_silver"}
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.catchup is False and DAG.max_active_runs == 1


def test_변환만_재시도하고_확인과_검증은_재시도하지_않는다():
    assert DAG.get_task("bronze_to_silver").retries == 1
    assert DAG.get_task("check_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        (datetime(2024, 5, 1, tzinfo=timezone.utc), "2024-02"),
        (datetime(2024, 2, 1, tzinfo=timezone.utc), "2023-11"),
        (datetime(2024, 1, 15, tzinfo=timezone.utc), "2023-10"),
    ],
)
def test_지정이_없으면_전력_공개지연만큼_물러선_달을_고른다(reference, expected):
    assert task_module.default_year_month(reference) == expected


def test_지정한_달이_있으면_그대로_쓴다():
    assert task_module.resolve_year_month({"params": {"year_month": "2024-03"}}) == "2024-03"


def test_원본이_없으면_돌려야_할_DAG_를_알려주며_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="eia_electricity_price_raw_to_bronze_pipeline"):
        task_module.require_bronze(str(tmp_path), "2024-03")


def test_검증은_그달_전_일수가_있어야_통과한다(tmp_path):
    path = task_module.silver_file(str(tmp_path), "2024-03")
    _write(path, _march())

    task_module.validate_silver(str(tmp_path), "2024-03")


@pytest.mark.parametrize("violation", ["missing_day", "duplicate_day", "schema"])
def test_일수부족_중복일자_스키마불일치는_실패한다(tmp_path, violation):
    from datetime import date

    path = task_module.silver_file(str(tmp_path), "2024-03")
    if violation == "missing_day":
        _write(path, _march(30))
    elif violation == "duplicate_day":
        rows = _march(30)
        rows.append({"date": date(2024, 3, 30), "ev_price": 0.28})
        _write(path, rows)
    else:
        schema = pa.schema([("date", pa.date32()), ("price", pa.float64())])
        _write(path, [{"date": date(2024, 3, day), "price": 0.28} for day in range(1, 32)], schema)

    with pytest.raises(ValueError):
        task_module.validate_silver(str(tmp_path), "2024-03")


def test_산출물이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="충전 단가 Silver"):
        task_module.validate_silver(str(tmp_path), "2024-03")
