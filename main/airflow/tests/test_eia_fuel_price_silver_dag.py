"""연료비 통합 silver → silver DAG 계약. 이슈 #518.

Bronze 원본 검증은 여기 없습니다 — `test_eia_raw_to_bronze_validation.py` 로 옮겼습니다.
통합이 CLEAN Silver 만 읽게 되면서 이 DAG 와 상관이 없어졌습니다.

1. 월간 스케줄과 세 단계 순서 — CLEAN 확인이 통합보다 먼저. 한쪽만 있으면 통합이 더
   안쪽에서 죽어 어느 정제가 문제인지 로그를 파야 함
2. 두 CLEAN 중 하나라도 없으면 어느 DAG 를 돌릴지 알려주며 실패
3. 지정이 없으면 전력 공개 지연(약 3개월)만큼 물러섬 — 두 CLEAN 중 전력이 늦게 나옴
4. 산출물 검증: 행 수·스키마·`price_source`·계보·확정상태
5. 잠정값(`Preliminary`)은 실패시키지 않고 통과 — 정상 산출물이지만 재생성 시 값이 바뀜

Lambda 핸들러는 부르지 않습니다 — 파일을 직접 놓고 검증 함수만 확인합니다.
"""

from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ★ import 순서가 중요합니다. `scripts` 패키지가 두 곳(airflow/scripts, 저장소 루트
#   scripts)에 있는데, `common.project_paths` 가 저장소 루트를 sys.path 앞에 꽂아
#   airflow 쪽을 가립니다. tasks 를 먼저 부르면 그 안에서 경로 설정이 끝나 양쪽이 다
#   잡힙니다 — 런타임(DAG 파싱)도 같은 순서입니다.
from main.airflow.scripts.eia_fuel_price_silver import tasks as silver_tasks
from dags import eia_fuel_price_silver_dag as dag_module
from main.aws_lambda.functions.eia_fuel_price_silver.extractor import clean_silver_file
from schema.silver import (
    CLEAN_EV_CHARGING_PRICE_SCHEMA as EV_SCHEMA,
    CLEAN_FUEL_PRICE_SCHEMA as SCHEMA,
    CLEAN_GAS_PRICE_SCHEMA as GAS_SCHEMA,
    EIA,
    FINAL,
    PRELIMINARY,
)


DAG = dag_module.eia_fuel_price_silver_dag
COLLECTED = date(2026, 8, 17)


def _write_clean(silver, year_month="2025-05", *, gas=True, electricity=True) -> None:
    year, month = (int(part) for part in year_month.split("-"))
    import calendar

    days = calendar.monthrange(year, month)[1]
    if gas:
        rows = [
            {"date": date(year, month, d), "gas_price": 3.0,
             "bronze_collected_date": COLLECTED}
            for d in range(1, days + 1)
        ]
        path = clean_silver_file(str(silver), "eia_gas_price", year_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=GAS_SCHEMA), path)
    if electricity:
        rows = [
            {"date": date(year, month, d), "ev_price": 0.4,
             "bronze_collected_date": COLLECTED, "ev_price_status": FINAL}
            for d in range(1, days + 1)
        ]
        path = clean_silver_file(str(silver), "eia_electricity_price", year_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=EV_SCHEMA), path)


def _write_silver(silver, year_month, rows, source=EIA, schema=SCHEMA,
                  collected=COLLECTED, status=FINAL):
    path = silver_tasks.integrated_silver_file(str(silver), year_month)
    path.parent.mkdir(parents=True, exist_ok=True)
    year, month = (int(part) for part in year_month.split("-"))
    records = [
        {"date": date(year, month, day), "gas_price": 3.0, "ev_price": 0.4,
         "price_source": source, "bronze_collected_date": collected,
         "ev_price_status": status}
        for day in range(1, rows + 1)
    ]
    if schema is not SCHEMA:
        records = [{k: v for k, v in r.items() if k in schema.names} for r in records]
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)


def test_DAG는_월간_스케줄로_확인_통합_검증을_순서대로_처리한다():
    assert DAG.dag_id == "eia_fuel_price_silver_pipeline"
    assert DAG.schedule == "0 8 1 * *"
    assert set(DAG.task_ids) == {"check_clean_silver", "combine_silver", "validate_silver"}
    assert DAG.get_task("check_clean_silver").downstream_task_ids == {"combine_silver"}
    assert DAG.get_task("combine_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.catchup is False and DAG.max_active_runs == 1


def test_통합만_재시도하고_확인과_검증은_재시도하지_않는다():
    assert DAG.get_task("combine_silver").retries == 1
    assert DAG.get_task("check_clean_silver").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


def test_Bronze_경로_파라미터는_더_이상_없다():
    """CLEAN Silver 만 읽으므로 Bronze 경로를 받을 이유가 없습니다 (#518)."""
    assert "bronze_dir" not in DAG.params
    assert "markup" not in DAG.params


def test_두_CLEAN_이_있으면_통과한다(tmp_path):
    _write_clean(tmp_path)

    found = silver_tasks.require_clean_silver(str(tmp_path), "2025-05")

    assert set(found) == {"eia_gas_price", "eia_electricity_price"}


@pytest.mark.parametrize(
    ("missing", "expected_dag"),
    [
        ({"gas": False}, "eia_gas_price_bronze_to_silver_pipeline"),
        ({"electricity": False}, "eia_electricity_price_bronze_to_silver_pipeline"),
    ],
)
def test_CLEAN_이_하나라도_없으면_어느_DAG를_돌릴지_알려준다(tmp_path, missing, expected_dag):
    _write_clean(tmp_path, **missing)

    with pytest.raises(FileNotFoundError, match=expected_dag):
        silver_tasks.require_clean_silver(str(tmp_path), "2025-05")


def test_지정이_없으면_전력_공개지연만큼_물러선다():
    assert silver_tasks.default_year_month(datetime(2024, 5, 1, tzinfo=timezone.utc)) == "2024-02"


def test_연초_경계에서_연도가_함께_내려간다():
    assert silver_tasks.default_year_month(datetime(2024, 2, 1, tzinfo=timezone.utc)) == "2023-11"


def test_파라미터가_있으면_그_값을_쓴다():
    assert silver_tasks.resolve_year_month({"params": {"year_month": "2024-03"}}) == "2024-03"


@pytest.mark.parametrize("value", ["2024-13", "2024/03", "abcd-ef"])
def test_형식이_잘못된_year_month는_거부한다(value):
    with pytest.raises(ValueError):
        silver_tasks.resolve_year_month({"params": {"year_month": value}})


def test_정상_산출물은_검증을_통과한다(tmp_path):
    _write_silver(tmp_path, "2024-03", 31)

    silver_tasks.validate_silver(str(tmp_path), "2024-03")


def test_행수가_그달_일수와_다르면_실패한다(tmp_path):
    _write_silver(tmp_path, "2024-03", 30)

    with pytest.raises(ValueError, match="31일이어야"):
        silver_tasks.validate_silver(str(tmp_path), "2024-03")


def test_스키마가_다르면_실패한다(tmp_path):
    trimmed = pa.schema([("date", pa.date32()), ("gas_price", pa.float64())])
    _write_silver(tmp_path, "2024-03", 31, schema=trimmed)

    with pytest.raises(ValueError, match="스키마가 다릅니다"):
        silver_tasks.validate_silver(str(tmp_path), "2024-03")


def test_다른_출처가_만든_산출물은_EIA_검증에서_실패한다(tmp_path):
    _write_silver(tmp_path, "2024-03", 31, source="aaa")

    with pytest.raises(ValueError, match="price_source"):
        silver_tasks.validate_silver(str(tmp_path), "2024-03")


def test_산출물이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        silver_tasks.validate_silver(str(tmp_path), "2024-03")


def test_잠정값도_검증을_통과한다(tmp_path):
    """잠정값도 정상 산출물입니다. 다만 재생성 시 값이 바뀐다는 것을 로그로 남깁니다."""
    _write_silver(tmp_path, "2024-03", 31, status=PRELIMINARY)

    silver_tasks.validate_silver(str(tmp_path), "2024-03")
