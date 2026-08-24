"""EIA 전력요금 Raw→Silver DAG 계약. 이슈 #630.

1. 수집·Bronze 검증·Silver 변환·Silver 검증을 한 DAG에서 순서대로 실행
2. 수집 2회, 변환 1회, 검증 0회의 장애 유형별 재시도 유지
3. 지정이 없으면 공개 지연 3개월 전, 수동 year_month가 있으면 지정한 달 사용
4. Silver 스키마·행 수·날짜 완결성 검증 유지
"""

from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import eia_electricity_price_raw_to_silver_dag as dag_module
from main.airflow.scripts.eia_electricity_price_bronze_to_silver import tasks as task_module
from schema.silver import CLEAN_EV_CHARGING_PRICE_SCHEMA as SCHEMA
from shared.airflow.common import lambda_invoke
from shared.airflow.common.validation import S3Location


DAG = dag_module.eia_electricity_price_raw_to_silver_dag
SOURCE_COLLECTED_AT = "2026-08-17T12:34:56.123456Z"


def _write(path, rows, schema=SCHEMA):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _march(day_count=31):
    return [
        {"date": date(2024, 3, day), "ev_price": 0.28}
        for day in range(1, day_count + 1)
    ]


def test_DAG는_월간_스케줄로_Raw부터_Silver까지_네_task를_순서대로_처리한다():
    assert DAG.dag_id == "eia_electricity_price_raw_to_silver_pipeline"
    assert DAG.schedule == "0 6 1 * *"
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}
    assert DAG.get_task("validate_bronze").downstream_task_ids == {"bronze_to_silver"}
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.catchup is False and DAG.max_active_runs == 3


def test_수집과_변환만_장애유형에_맞게_재시도한다():
    collection = DAG.get_task("raw_to_bronze")
    assert collection.retries == 2
    assert collection.retry_delay == timedelta(minutes=5)
    assert collection.retry_exponential_backoff is True

    transform = DAG.get_task("bronze_to_silver")
    assert transform.retries == 1
    assert transform.retry_delay == timedelta(minutes=10)

    assert DAG.get_task("validate_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


def test_수집task는_지역과_수집시각만_람다에_보낸다(monkeypatch):
    called = {}
    handlers = []

    def handler(*, event):
        called.update(event)
        return {"row_count": 1, "locations": ["/bronze/x.xlsx"]}

    monkeypatch.setattr(
        lambda_invoke,
        "lambda_handler_for",
        lambda name, **_: handlers.append(name) or handler,
    )
    dag_run = type(
        "DagRun", (), {"start_date": datetime(2026, 8, 17, 12, 34, 56, tzinfo=timezone.utc)}
    )()
    DAG.get_task("raw_to_bronze").python_callable(
        params={"bronze_dir": "/bronze", "service_area": "TX"},
        dag_run=dag_run,
    )
    assert handlers == ["eia_electricity_price_raw_to_bronze"]
    assert called == {
        "service_area": "TX",
        "collected_at": "2026-08-17T12:34:56.000000Z",
        "base_dir": "/bronze",
    }


def test_정제task는_년월_지역_markup만_람다에_보낸다(monkeypatch):
    called = {}
    handlers = []

    def handler(*, event):
        called.update(event)
        return {"row_count": 31, "locations": ["/silver/x.parquet"]}

    monkeypatch.setattr(
        lambda_invoke,
        "lambda_handler_for",
        lambda name, **_: handlers.append(name) or handler,
    )
    DAG.get_task("bronze_to_silver").python_callable(
        params={
            "year": "2024",
            "month": "3",
            "bronze_dir": "/bronze",
            "silver_dir": "/silver",
            "markup": 0.15,
            "service_area": "TX",
        },
    )
    assert handlers == ["eia_electricity_price_bronze_to_silver"]
    assert called == {
        "year_month": "2024-03",
        "service_area": "TX",
        "markup": 0.15,
        "bronze_dir": "/bronze",
        "silver_dir": "/silver",
    }


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


def test_지정한_연도와_월은_year_month로_정규화한다():
    assert task_module.resolve_year_month(
        {"params": {"year": "2024", "month": "3"}}
    ) == "2024-03"


@pytest.mark.parametrize(
    "params",
    [
        {"year": "2024", "month": None},
        {"year": None, "month": "3"},
    ],
)
def test_연도와_월중_하나만_지정하면_실패한다(params):
    with pytest.raises(ValueError, match="year와 month는 함께"):
        task_module.resolve_year_month({"params": params})


def test_검증은_그달_전_일수가_있어야_통과한다(tmp_path):
    path = task_module.silver_file(
        str(tmp_path), "2024-03", SOURCE_COLLECTED_AT, "NYC"
    )
    _write(path, _march())

    task_module.validate_silver(
        {"year_month": "2024-03", "source_collected_at": SOURCE_COLLECTED_AT, "row_count": 31, "locations": [str(path)]},
        "NYC",
    )


@pytest.mark.parametrize("violation", ["missing_day", "duplicate_day", "schema"])
def test_일수부족_중복일자_스키마불일치는_실패한다(tmp_path, violation):
    path = task_module.silver_file(
        str(tmp_path), "2024-03", SOURCE_COLLECTED_AT, "NYC"
    )
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
        task_module.validate_silver(
            {"year_month": "2024-03", "source_collected_at": SOURCE_COLLECTED_AT, "row_count": 31, "locations": [str(path)]},
            "NYC",
        )


def test_산출물이_없으면_실패한다(tmp_path):
    path = task_module.silver_file(
        str(tmp_path), "2024-03", SOURCE_COLLECTED_AT, "NYC"
    )
    with pytest.raises(FileNotFoundError, match="충전 단가 Silver"):
        task_module.validate_silver(
            {"year_month": "2024-03", "source_collected_at": SOURCE_COLLECTED_AT, "row_count": 31, "locations": [str(path)]},
            "NYC",
        )


def test_S3_Silver_경로를_로컬_Path로_변환하지_않는다(monkeypatch):
    seen = []
    table = pa.Table.from_pylist(_march(), schema=SCHEMA)
    monkeypatch.setattr(
        task_module,
        "read_parquet",
        lambda path: seen.append(path) or table,
    )

    task_module.validate_silver(
        {
            "year_month": "2024-03",
            "row_count": 31,
            "locations": [
                "s3://data-lake/silver/eia_electricity_price/service_area=NYC/"
                "year_month=2024-03/source_collected_at=20260817T123456123456Z/"
                "eia_electricity_price.parquet"
            ],
            "source_collected_at": SOURCE_COLLECTED_AT,
        },
        "NYC",
    )

    assert isinstance(seen[0], S3Location)


# --- validate-then-publish (#912) --------------------------------------------


def _fake_task_instance(result: dict):
    return type(
        "TaskInstance", (), {"xcom_pull": lambda self, task_ids: result}
    )()


def test_검증_실패시_SUCCESS를_공개하지_않는다(tmp_path):
    final = task_module.silver_file(
        str(tmp_path), "2024-03", SOURCE_COLLECTED_AT, "NYC"
    )
    _write(final, _march(30))
    task_instance = _fake_task_instance(
        {"year_month": "2024-03", "source_collected_at": SOURCE_COLLECTED_AT, "row_count": 30, "locations": [str(final)]}
    )

    with pytest.raises(ValueError, match="31일이어야"):
        task_module.validate_silver_task.function(
            task_instance=task_instance,
            params={"service_area": "NYC", "silver_dir": str(tmp_path)},
        )

    assert final.is_file()
    assert not (final.parent / "_SUCCESS").exists()


def test_검증_통과시_최종_경로에_SUCCESS를_공개한다(tmp_path):
    final = task_module.silver_file(
        str(tmp_path), "2024-03", SOURCE_COLLECTED_AT, "NYC"
    )
    _write(final, _march())
    task_instance = _fake_task_instance(
        {"year_month": "2024-03", "source_collected_at": SOURCE_COLLECTED_AT, "row_count": 31, "locations": [str(final)]}
    )

    task_module.validate_silver_task.function(
        task_instance=task_instance,
        params={"service_area": "NYC", "silver_dir": str(tmp_path)},
    )

    assert final.is_file()
    assert (final.parent / "_SUCCESS").is_file()
    assert pq.ParquetFile(final).read().num_rows == 31
