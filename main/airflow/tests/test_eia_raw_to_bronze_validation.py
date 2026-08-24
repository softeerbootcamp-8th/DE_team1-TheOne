"""EIA 원본 두 종의 Bronze 적재 검증 계약. 이슈 #518 에서 통합 DAG 테스트로부터 분리.

통합이 CLEAN Silver 만 읽게 되면서(#518) 이 검사들은 통합과 상관이 없어졌습니다.
검증 대상은 각 `*_raw_to_bronze` 가 적재한 원본입니다.

1. 적재 경로가 layout 규칙과 다르면 실패
2. 원본이 데이터셋별 하한보다 작으면 실패 — 형식만 바뀌어도 파싱은 예외 없이 이상한
   값을 내므로 크기로 1차 확인
3. 수집(lambda)과 검증(airflow)이 **같은 하한**을 봄. 예전에 airflow 쪽만 10_000 으로
   굳어 있어 전력 xlsx 가 lambda 하한(100_000)에 못 미쳐도 통과처럼 보였습니다
4. service_area가 있으면 layout_tail 세그먼트 폭이 늘어나 데이터셋 이름이 다른
   엉뚱한 경로도 여전히 잡힘 (gas #843, electricity #844)
"""

import importlib
import pytest

from main.airflow.scripts.eia_electricity_price_raw_to_bronze import tasks as electricity_tasks
from main.airflow.scripts.eia_gas_price_raw_to_bronze import tasks as gas_tasks


def _layout():
    return importlib.import_module("main.aws_lambda.common.eia_fuel_price_layout")


BIG_ENOUGH = b"x" * (_layout().ELECTRICITY_MIN_BYTES + 1)
COLLECTED_AT = "2026-08-17T12:34:56.123456Z"


DATASETS = [
    # gas(#843)/electricity(#844) 모두 service_area 를 params 계약에 추가했으므로
    # 값이 필요합니다.
    pytest.param(gas_tasks, "gas_bronze_file", "GAS_MIN_BYTES", "NYC", id="gas"),
    pytest.param(
        electricity_tasks, "electricity_bronze_file", "ELECTRICITY_MIN_BYTES", "NYC",
        id="electricity",
    ),
]


@pytest.mark.parametrize(("tasks", "bronze_file", "_min_attr", "service_area"), DATASETS)
def test_원본이_규칙과_다른_경로면_실패한다(tmp_path, tasks, bronze_file, _min_attr, service_area):
    stray = tmp_path / "stray.xls"
    stray.write_bytes(BIG_ENOUGH)
    result = {
        "row_count": 1,
        "locations": [str(stray)],
        "collected_at": COLLECTED_AT,
        "collected_date": "2026-08-17",
    }

    with pytest.raises(ValueError, match="적재 경로가 예상과 다릅니다"):
        tasks.validate_bronze_task.function(
            result, params={"bronze_dir": str(tmp_path), "service_area": service_area}
        )


@pytest.mark.parametrize(("tasks", "bronze_file", "min_attr", "service_area"), DATASETS)
def test_원본이_하한보다_작으면_실패한다(tmp_path, tasks, bronze_file, min_attr, service_area):
    layout = _layout()
    path = getattr(layout, bronze_file)(str(tmp_path), COLLECTED_AT, service_area)
    path.parent.mkdir(parents=True, exist_ok=True)
    # 하한보다 1바이트 작게 — 각 데이터셋의 하한이 실제로 적용되는지 봅니다.
    path.write_bytes(b"x" * (getattr(layout, min_attr) - 1))
    result = {
        "row_count": 1,
        "locations": [str(path)],
        "collected_at": COLLECTED_AT,
        "collected_date": "2026-08-17",
    }

    with pytest.raises(ValueError, match="EIA 원본이 너무 작습니다"):
        tasks.validate_bronze_task.function(
            result, params={"bronze_dir": str(tmp_path), "service_area": service_area}
        )


@pytest.mark.parametrize(
    ("tasks", "file_name_attr"),
    [
        pytest.param(gas_tasks, "GAS_FILE_NAME", id="gas"),
        pytest.param(electricity_tasks, "ELECTRICITY_FILE_NAME", id="electricity"),
    ],
)
def test_service_area가_있으면_데이터셋_이름이_달라도_경로_검증이_잡는다(
    tmp_path, tasks, file_name_attr
):
    """layout_tail이 service_area만큼 세그먼트 폭을 안 늘리면, tail 3칸 비교에서
    맨 앞 데이터셋 이름이 잘려 나갑니다. 그러면 지역·날짜·파일명만 같고 데이터셋
    이름이 다른 엉뚱한 경로도 통과해 버립니다(#839가 경고한 함정) — 폭이 실제로
    늘어났는지 각 데이터셋의 validate_bronze_task 호출부에서 고정합니다."""
    layout = _layout()
    wrong = (
        tmp_path / "not_the_real_dataset" / "service_area=NYC"
        / "year_month=2026-08"
        / "collected_at=20260817T123456123456Z"
        / getattr(layout, file_name_attr)
    )
    wrong.parent.mkdir(parents=True, exist_ok=True)
    wrong.write_bytes(BIG_ENOUGH)
    result = {
        "row_count": 1,
        "locations": [str(wrong)],
        "collected_at": COLLECTED_AT,
        "collected_date": "2026-08-17",
    }

    with pytest.raises(ValueError, match="적재 경로가 예상과 다릅니다"):
        tasks.validate_bronze_task.function(
            result, params={"bronze_dir": str(tmp_path), "service_area": "NYC"}
        )


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
