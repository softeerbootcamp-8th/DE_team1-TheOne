"""EIA 원본 두 종의 Bronze 적재 검증 계약. 이슈 #518 에서 통합 DAG 테스트로부터 분리.

통합이 CLEAN Silver 만 읽게 되면서(#518) 이 검사들은 통합과 상관이 없어졌습니다.
검증 대상은 각 `*_raw_to_bronze` 가 적재한 원본입니다.

1. 적재 경로가 layout 규칙과 다르면 실패
2. 원본이 데이터셋별 하한보다 작으면 실패 — 형식만 바뀌어도 파싱은 예외 없이 이상한
   값을 내므로 크기로 1차 확인
3. 수집(lambda)과 검증(airflow)이 **같은 하한**을 봄. 예전에 airflow 쪽만 10_000 으로
   굳어 있어 전력 xlsx 가 lambda 하한(100_000)에 못 미쳐도 통과처럼 보였습니다
"""

import importlib
from datetime import date

import pytest

from main.airflow.scripts.eia_electricity_price_raw_to_bronze import tasks as electricity_tasks
from main.airflow.scripts.eia_gas_price_raw_to_bronze import tasks as gas_tasks


def _layout():
    return importlib.import_module("main.aws_lambda.common.eia_fuel_price_layout")


BIG_ENOUGH = b"x" * (_layout().ELECTRICITY_MIN_BYTES + 1)


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
        ("main.aws_lambda.functions.eia_gas_price_raw_to_bronze.extractor", layout.GAS_MIN_BYTES),
        (
            "main.aws_lambda.functions.eia_electricity_price_raw_to_bronze.extractor",
            layout.ELECTRICITY_MIN_BYTES,
        ),
    ):
        assert import_module(module_name).MIN_BYTES == expected
