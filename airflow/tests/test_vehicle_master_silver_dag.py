"""차량 마스터 Silver DAG 의 스케줄 계약과 검증 시나리오.

 1. 스케줄이 (대장 & Uber & Lyft) | 제원 — 제원을 AND 에 넣으면 월 1회로 묶임
 2. 상류 4개 DAG 가 그 Asset 을 실제로 발행 (생산자와 소비자가 같은 객체를 봄)
 3. 상류는 적재가 아니라 **검증** 태스크에서 발행
 4. 정상 결과는 통과하고 vehicle_master Asset 을 발행
 5. layout 규칙과 다른 경로면 실패
 6. loader.SCHEMA 와 다른 스키마면 실패
 7. 도시 파일이 0행이면 실패 (합계만 보면 못 잡음)
 8. 주간 원천이 2주 넘게 낡으면 실패
 9. 제원은 월 1회라 45일까지 허용
10. source_collected_dates 가 없으면 실패

핸들러는 부르지 않습니다. 검증 태스크에 결과 dict 를 직접 넘겨 파일만 실제로 씁니다.
"""

import importlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import vehicle_master_silver_dag as dag_module
from dags.common import assets

layout = importlib.import_module("lambda.functions.common.vehicle_master_layout")
loader = importlib.import_module("lambda.functions.vehicle_master_silver.loader")

DAG = dag_module.vehicle_master_dag
validate_silver = DAG.get_task("validate_silver").python_callable
COLLECTED_DATE = "2026-08-13"
CITY = "new-york"

# 원천 4개가 모두 신선한 기본값. 개별 테스트가 필요한 것만 덮어씁니다.
FRESH_SOURCES = {
    "vehicle_catalog": "2026-08-12",
    "uber_eligible_vehicles": "2026-08-11",
    "lyft_eligible_vehicles": "2026-08-12",
    "fueleconomy_vehicle_specs": "2026-08-01",
}

UPSTREAM = {
    "vehicle_catalog_raw_to_silver_dag": assets.VEHICLE_CATALOG_SILVER,
    "uber_eligible_vehicles_raw_to_silver_dag": assets.UBER_ELIGIBLE_VEHICLES_SILVER,
    "lyft_eligible_vehicles_raw_to_silver_dag": assets.LYFT_ELIGIBLE_VEHICLES_SILVER,
    "fueleconomy_vehicle_specs_raw_to_silver_dag": assets.FUELECONOMY_VEHICLE_SPECS_SILVER,
}


def master_row() -> dict:
    """`loader.SCHEMA` 를 채운 한 행. 값 자체는 검증 대상이 아닙니다."""
    return {
        name: 1 if pa.types.is_integer(field.type)
        else 1.0 if pa.types.is_floating(field.type)
        else "x"
        for name, field in zip(loader.SCHEMA.names, loader.SCHEMA)
    }


def write_master(silver_dir: Path, rows: int = 2, schema=None, city: str = CITY) -> Path:
    schema = loader.SCHEMA if schema is None else schema
    path = layout.silver_file(str(silver_dir), date.fromisoformat(COLLECTED_DATE), city)
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {name: master_row().get(name, "x") for name in schema.names} for _ in range(rows)
    ]
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return path


def result_for(paths: list[Path], row_count: int, sources: dict | None = None) -> dict:
    return {
        "row_count": row_count,
        "locations": [str(path) for path in paths],
        "collected_date": COLLECTED_DATE,
        "source_collected_dates": FRESH_SOURCES if sources is None else sources,
    }


def params_for(silver_dir: Path) -> dict:
    return {"params": {"silver_dir": str(silver_dir)}}


def test_스케줄이_주간_3종_AND_와_제원_OR_이다():
    """[필수] 제원을 AND 에 넣으면 월 1회로 묶여 렌트료가 3주 묵습니다."""
    condition = DAG.timetable.asset_condition
    weekly, specs = condition.objects

    assert {asset.name for asset in weekly.objects} == {
        assets.VEHICLE_CATALOG_SILVER.name,
        assets.UBER_ELIGIBLE_VEHICLES_SILVER.name,
        assets.LYFT_ELIGIBLE_VEHICLES_SILVER.name,
    }
    assert specs.name == assets.FUELECONOMY_VEHICLE_SPECS_SILVER.name
    # AND 묶음 하나와 제원 하나를 OR 로 잇습니다.
    assert type(condition).__name__ == "AssetAny"
    assert type(weekly).__name__ == "AssetAll"


@pytest.mark.parametrize(("module_name", "asset"), sorted(UPSTREAM.items()))
def test_상류_DAG_가_검증_태스크에서_Asset_을_발행한다(module_name, asset):
    """발행이 없으면 이 DAG 는 영원히 트리거되지 않습니다 — 실패하지 않고 안 돕니다."""
    upstream = importlib.import_module(f"dags.{module_name}")
    dag = next(
        value for value in vars(upstream).values() if getattr(value, "dag_id", None)
    )

    # 적재 태스크가 아니라 검증 태스크여야 합니다. 적재 직후 발행하면 깨진
    # Silver 로 조립이 돌아갑니다.
    assert asset.name in {a.name for a in dag.get_task("validate_silver").outlets}
    assert not dag.get_task("bronze_to_silver").outlets


def test_정상_결과는_통과하고_마스터_Asset_을_발행한다(tmp_path):
    path = write_master(tmp_path, rows=2)

    validate_silver(result_for([path], 2), **params_for(tmp_path))

    outlets = DAG.get_task("validate_silver").outlets
    assert [a.name for a in outlets] == [assets.VEHICLE_MASTER_SILVER.name]


def test_layout_규칙과_다른_경로면_실패한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    moved = path.parent / "elsewhere.parquet"
    path.rename(moved)

    with pytest.raises(ValueError, match="layout 규칙과 다릅니다"):
        validate_silver(result_for([moved], 2), **params_for(tmp_path))


def test_스키마가_다르면_실패한다(tmp_path):
    trimmed = pa.schema([field for field in loader.SCHEMA][:5])
    path = write_master(tmp_path, rows=2, schema=trimmed)

    with pytest.raises(ValueError, match="loader.SCHEMA 와 다릅니다"):
        validate_silver(result_for([path], 2), **params_for(tmp_path))


def test_도시_파일이_0행이면_실패한다(tmp_path):
    """행 수 합계만 보면 도시 하나가 통째로 비어도 지나갑니다."""
    filled = write_master(tmp_path, rows=2)
    empty = write_master(tmp_path, rows=0, city="boston")

    with pytest.raises(ValueError, match="행이 없습니다"):
        validate_silver(result_for([filled, empty], 2), **params_for(tmp_path))


def test_주간_원천이_2주_넘게_낡으면_실패한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    # 대장 크롤링이 3주 멈춘 상황. Extractor 는 기준일 이하 최신을 쓰므로
    # 이 가드가 없으면 지난달 렌트료로 조용히 성공합니다.
    stale = {**FRESH_SOURCES, "vehicle_catalog": "2026-07-20"}

    with pytest.raises(ValueError, match="vehicle_catalog=24일"):
        validate_silver(result_for([path], 2, stale), **params_for(tmp_path))


def test_제원은_월_1회라_45일까지_허용한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    # 40일 전 = 지난달 1일 수집분. 주간 기준(14일)을 적용하면 매달 대부분의 날에
    # 실패하게 됩니다.
    sources = {**FRESH_SOURCES, "fueleconomy_vehicle_specs": "2026-07-04"}

    validate_silver(result_for([path], 2, sources), **params_for(tmp_path))


def test_원천_수집일이_없으면_실패한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    sources = {key: value for key, value in FRESH_SOURCES.items() if key != "lyft_eligible_vehicles"}

    with pytest.raises(ValueError, match="원천 수집일이 빠졌습니다"):
        validate_silver(result_for([path], 2, sources), **params_for(tmp_path))
