"""EIA 연료비 3단 DAG 의 검증 시나리오. 이슈 #445.

수집 두 개(휘발유·전력)와 통합 하나로 나눈 이유는 두 원본이 남남이라는 것입니다 —
URL·형식·파서·공개 주기가 전부 달라서 한쪽이 죽어도 다른 쪽은 살아야 합니다.

 1. 원본이 규칙과 다른 경로에 있으면 실패 (두 데이터셋 각각)
 2. 원본이 하한보다 작으면 실패 (형식이 바뀌면 파싱이 조용히 이상한 값을 냄)
 2-1. 그 하한을 수집(lambda)과 검증(airflow)이 같은 값으로 봄
 3. 통합 전에 원본 두 개를 확인하고, 없으면 어느 DAG 를 돌릴지 알려줌
 4. 하나만 있어도 실패 (변환이 더 안쪽에서 죽는 것을 방지)
 5. 지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달
 6. 연초 경계에서 연도가 함께 내려감
 7. `year_month` 파라미터가 있으면 그 값을 그대로 씀 / 형식 오류는 거부
 8. 정상 산출물은 검증 통과
 9. 행 수가 그 달 일수와 다르면 실패 (하루라도 비면 Gold 조인이 조용히 줄어듦)
10. 스키마·`price_source` 가 다르면 실패
11. 계보(`bronze_collected_date`)가 비었거나 한 달 안에서 섞이면 실패
12. 잠정값(`Preliminary`)은 실패시키지 않고 통과 — 정상 산출물이지만 재생성 시 값이 바뀜

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
from sub.airflow.scripts.eia_fuel_price_bronze_to_silver import tasks as silver_tasks
from sub.airflow.scripts.eia_electricity_price_raw_to_bronze import tasks as electricity_tasks
from sub.airflow.scripts.eia_gas_price_raw_to_bronze import tasks as gas_tasks
from schema.silver.gas_ev_price import EIA, FINAL, PRELIMINARY, SCHEMA


def _layout():
    import importlib

    return importlib.import_module("shared.lambda_runtime.common.eia_fuel_price_layout")


BIG_ENOUGH = b"x" * (_layout().ELECTRICITY_MIN_BYTES + 1)


def _write_bronze(bronze, collected_date: date, *, gas=True, electricity=True) -> None:
    layout = _layout()
    targets = []
    if gas:
        targets.append(layout.gas_bronze_file(str(bronze), collected_date))
    if electricity:
        targets.append(layout.electricity_bronze_file(str(bronze), collected_date))
    for path in targets:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(BIG_ENOUGH)


def _write_silver(
    silver,
    year_month: str,
    rows: int,
    source: str = EIA,
    schema=SCHEMA,
    collected=date(2026, 8, 17),
    status: str = FINAL,
):
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
        records = [{k: v for k, v in record.items() if k in schema.names}
                   for record in records]
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)


# --- 수집 DAG (휘발유·전력 공통 계약) ---------------------------------------

DATASETS = [
    pytest.param(gas_tasks, "gas_bronze_file", "GAS_MIN_BYTES", id="gas"),
    pytest.param(
        electricity_tasks, "electricity_bronze_file", "ELECTRICITY_MIN_BYTES",
        id="electricity",
    ),
]


@pytest.mark.parametrize(("tasks", "bronze_file", "_min_attr"), DATASETS)
def test_원본이_규칙과_다른_경로면_실패한다(tmp_path, tasks, bronze_file, _min_attr):
    stray = tmp_path / "stray.xls"
    stray.write_bytes(BIG_ENOUGH)
    result = {"row_count": 1, "locations": [str(stray)], "collected_date": "2026-08-17"}

    with pytest.raises(ValueError, match="적재 경로가 예상과 다릅니다"):
        tasks.validate_bronze_task.function(result, params={"bronze_dir": str(tmp_path)})


@pytest.mark.parametrize(("tasks", "bronze_file", "min_attr"), DATASETS)
def test_원본이_하한보다_작으면_실패한다(tmp_path, tasks, bronze_file, min_attr):
    layout = _layout()
    path = getattr(layout, bronze_file)(str(tmp_path), date(2026, 8, 17))
    path.parent.mkdir(parents=True, exist_ok=True)
    # 하한보다 1바이트 작게 — 각 데이터셋의 하한이 실제로 적용되는지 봅니다.
    path.write_bytes(b"x" * (getattr(layout, min_attr) - 1))
    result = {"row_count": 1, "locations": [str(path)], "collected_date": "2026-08-17"}

    with pytest.raises(ValueError, match="EIA 원본이 너무 작습니다"):
        tasks.validate_bronze_task.function(result, params={"bronze_dir": str(tmp_path)})


def test_수집과_검증이_같은_하한을_본다():
    """lambda 가 받아들인 파일을 airflow 가 되돌리면 안 됩니다.

    이전에는 airflow 쪽이 두 데이터셋 모두 10_000 으로 굳어 있어서, 전력 xlsx 가
    lambda 하한(100_000)에 못 미치는데도 검증만 보면 통과처럼 보였습니다.
    """
    layout = _layout()
    from importlib import import_module

    for module_name, expected in (
        ("sub.lambda_runtime.functions.eia_gas_price_raw_to_bronze.extractor", layout.GAS_MIN_BYTES),
        (
            "sub.lambda_runtime.functions.eia_electricity_price_raw_to_bronze.extractor",
            layout.ELECTRICITY_MIN_BYTES,
        ),
    ):
        assert import_module(module_name).MIN_BYTES == expected


# --- 통합 DAG ---------------------------------------------------------------

def test_원본_두개가_있으면_통과한다(tmp_path):
    _write_bronze(tmp_path, date(2026, 8, 17))

    found = silver_tasks.require_bronze(str(tmp_path), "2025-05")

    assert len(found) == 2


@pytest.mark.parametrize(
    ("missing", "expected_dag"),
    [
        ({"gas": False}, "eia_gas_price_raw_to_bronze_pipeline"),
        ({"electricity": False}, "eia_electricity_price_raw_to_bronze_pipeline"),
    ],
)
def test_원본이_하나라도_없으면_어느_DAG를_돌릴지_알려준다(tmp_path, missing, expected_dag):
    # 하나만 있으면 변환이 더 안쪽에서 죽어 어느 수집이 문제인지 로그를 파야 합니다.
    _write_bronze(tmp_path, date(2026, 8, 17), **missing)

    with pytest.raises(FileNotFoundError, match=expected_dag):
        silver_tasks.require_bronze(str(tmp_path), "2025-05")


def test_지정이_없으면_전력_공개지연만큼_물러선다():
    # 2026-08 에 돌면 전력 통계는 2026-05 까지만 나와 있습니다.
    assert silver_tasks.default_year_month(
        datetime(2026, 8, 17, tzinfo=timezone.utc)
    ) == "2026-05"


def test_연초_경계에서_연도가_함께_내려간다():
    assert silver_tasks.default_year_month(datetime(2026, 2, 5, tzinfo=timezone.utc)) == "2025-11"
    assert silver_tasks.default_year_month(datetime(2026, 1, 5, tzinfo=timezone.utc)) == "2025-10"


def test_파라미터가_있으면_그_값을_쓴다():
    assert silver_tasks.resolve_year_month({"params": {"year_month": " 2025-05 "}}) == "2025-05"


@pytest.mark.parametrize("value", ["2025-13", "2025/05", "202505"])
def test_형식이_잘못된_year_month는_거부한다(value):
    with pytest.raises(ValueError):
        silver_tasks.resolve_year_month({"params": {"year_month": value}})


def test_정상_산출물은_검증을_통과한다(tmp_path):
    _write_silver(tmp_path, "2025-05", rows=31)

    silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_행수가_그달_일수와_다르면_실패한다(tmp_path):
    # 30행이면 5월(31일)에 하루가 빕니다. 그 날 운행은 Gold 조인에서 통째로 빠지는데,
    # 실패가 아니라 **조용히 줄어든 집계**로 나타나므로 여기서 막습니다.
    _write_silver(tmp_path, "2025-05", rows=30)

    with pytest.raises(ValueError, match="31일이어야 하는데 30행"):
        silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_스키마가_다르면_실패한다(tmp_path):
    narrowed = pa.schema([field for field in SCHEMA if field.name != "price_source"])
    _write_silver(tmp_path, "2025-05", rows=31, schema=narrowed)

    with pytest.raises(ValueError, match="통합 Silver 스키마가 다릅니다"):
        silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_다른_출처가_만든_산출물은_EIA_검증에서_실패한다(tmp_path):
    # 지금 생산자는 EIA 하나지만, 소스가 늘면 같은 자리에 쓰게 됩니다. 그때 남의
    # 산출물을 EIA 검증으로 통과시키면 어느 성질의 값인지 말할 수 없게 됩니다.
    _write_silver(tmp_path, "2025-05", rows=31, source="somewhere_else")

    with pytest.raises(ValueError, match="price_source 가 다릅니다"):
        silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_산출물이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="통합 연료비 Silver 가 없습니다"):
        silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_계보가_비어있으면_실패한다(tmp_path):
    # 무엇으로 만들었는지 모르면 "왜 지난번과 숫자가 다르지" 에 답할 수 없습니다.
    _write_silver(tmp_path, "2025-05", rows=31, collected=None)

    with pytest.raises(ValueError, match="bronze_collected_date 계보가"):
        silver_tasks.validate_silver(str(tmp_path), "2025-05")


def test_잠정값도_검증을_통과한다(tmp_path):
    # 잠정값은 정상 산출물입니다. 막으면 최근 3개월을 아예 만들 수 없습니다.
    _write_silver(tmp_path, "2025-05", rows=31, status=PRELIMINARY)

    silver_tasks.validate_silver(str(tmp_path), "2025-05")
