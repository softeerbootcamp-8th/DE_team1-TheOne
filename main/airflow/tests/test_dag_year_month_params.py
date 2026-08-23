"""`year`·`month` Param 형식 계약. 이슈 #534.

형식 검증이 없으면 `{"month":"13"}` 같은 값이 트리거를 통과하고 컨테이너가 뜬 뒤
태스크 안에서야 죽습니다. 여기서 막으면 트리거 화면에서 바로 걸립니다.

`"3"` 과 `"03"` 을 둘 다 받는 것은 기존 사용법입니다 — 실행 모듈이 `zfill(2)` 로
맞추므로 좁히면 지금 쓰는 config 가 깨집니다.
"""

import importlib

import pytest
from airflow.exceptions import ParamValidationError


# (모듈, DAG 변수) — year/month 를 받는 DAG 전부
DAGS = [
    ("dags.monthly_taxi_trip_raw_to_silver_dag", "monthly_taxi_trip_dag"),
    ("dags.monthly_taxi_trip_silver_to_gold_dag", "monthly_taxi_trip_silver_to_gold_dag"),
    ("dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag", "driver_vehicle_monthly_snapshot_raw_to_silver_dag"),
    ("dags.eia_gas_price_raw_to_silver_dag", "eia_gas_price_raw_to_silver_dag"),
    ("dags.eia_electricity_price_raw_to_silver_dag", "eia_electricity_price_raw_to_silver_dag"),
]


def _params(module_name, variable):
    return getattr(importlib.import_module(module_name), variable).params


@pytest.mark.parametrize(("module_name", "variable"), DAGS)
@pytest.mark.parametrize("year", ["2024", "2026"])
def test_네자리_연도는_통과한다(module_name, variable, year):
    params = _params(module_name, variable)
    params["year"] = year

    assert params["year"] == year


@pytest.mark.parametrize(("module_name", "variable"), DAGS)
@pytest.mark.parametrize("month", ["1", "01", "9", "09", "10", "12"])
def test_한자리와_두자리_월을_모두_받는다(module_name, variable, month):
    """실행 모듈이 zfill(2) 로 맞추므로 둘 다 유효한 입력입니다."""
    params = _params(module_name, variable)
    params["month"] = month

    assert params["month"] == month


@pytest.mark.parametrize(("module_name", "variable"), DAGS)
@pytest.mark.parametrize("month", ["13", "0", "00", "abc", "1-2", ""])
def test_월_범위_밖이면_트리거_단계에서_거부한다(module_name, variable, month):
    params = _params(module_name, variable)

    with pytest.raises(ParamValidationError):
        params["month"] = month


@pytest.mark.parametrize(("module_name", "variable"), DAGS)
@pytest.mark.parametrize("year", ["24", "20260", "abcd", ""])
def test_연도_형식이_틀리면_트리거_단계에서_거부한다(module_name, variable, year):
    params = _params(module_name, variable)

    with pytest.raises(ParamValidationError):
        params["year"] = year


@pytest.mark.parametrize(("module_name", "variable"), DAGS)
def test_비워두는_것은_계속_허용한다(module_name, variable):
    """비우면 실행 모듈이 대상 월을 자동 계산합니다. 그 경로가 살아 있어야 합니다."""
    params = _params(module_name, variable)
    params["year"] = None
    params["month"] = None

    assert params["year"] is None and params["month"] is None
