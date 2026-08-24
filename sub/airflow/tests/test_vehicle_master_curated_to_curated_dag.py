"""차량 마스터 Curated DAG 의 스케줄 계약과 검증 시나리오.

 1. 스케줄이 원천 4종 AND — 하나라도 이번 달 것이 없으면 조립하지 않음
 2. 상류 4개 DAG 가 그 Asset 을 실제로 발행 (생산자와 소비자가 같은 객체를 봄)
 3. 상류는 적재가 아니라 **검증** 태스크에서 발행
 4. 정상 결과는 통과하고 소비자 없는 vehicle_master Asset 은 발행하지 않음
 5. layout 규칙과 다른 경로면 실패
 6. loader.SCHEMA 와 다른 스키마면 실패
 7. 도시 파일이 0행이면 실패 (합계만 보면 못 잡음)
 8. 원천이 45일 넘게 낡으면 실패
 9. 월 중 수동 실행(원천이 한 달 가까이 된 상태)은 통과
10. source_collected_dates 가 없으면 실패
11. 낡음 판정 기준이 `as_of` — 적재 파티션(`collected_date`)으로 재면 가드가 무력화

핸들러는 부르지 않습니다. 검증 태스크에 결과 dict 를 직접 넘겨 파일만 실제로 씁니다.
"""

import importlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import vehicle_master_curated_to_curated_dag as dag_module
from sub.airflow.common import assets

layout = importlib.import_module("sub.aws_lambda.common.vehicle_master_layout")
loader = importlib.import_module("sub.aws_lambda.functions.vehicle_master_curated_to_curated.loader")

DAG = dag_module.vehicle_master_dag
validate_silver = DAG.get_task("validate_curated").python_callable
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
    "vehicle_catalog_raw_to_curated_dag": assets.VEHICLE_CATALOG_CURATED,
    "uber_eligible_vehicles_raw_to_curated_dag": assets.UBER_ELIGIBLE_VEHICLES_CURATED,
    "lyft_eligible_vehicles_raw_to_curated_dag": assets.LYFT_ELIGIBLE_VEHICLES_CURATED,
    "fueleconomy_vehicle_specs_raw_to_curated_dag": assets.FUELECONOMY_VEHICLE_SPECS_CURATED,
}


def master_row() -> dict:
    """`loader.SCHEMA` 를 채운 한 행. 값 자체는 검증 대상이 아닙니다."""
    return {
        name: 1 if pa.types.is_integer(field.type)
        else 1.0 if pa.types.is_floating(field.type)
        else "x"
        for name, field in zip(loader.SCHEMA.names, loader.SCHEMA)
    }


def write_master(
    silver_dir: Path,
    rows: int = 2,
    schema=None,
    city: str = CITY,
    blank: str | None = None,
    collected_date: str = COLLECTED_DATE,
) -> Path:
    """`blank` 를 주면 그 컬럼만 전 행 NULL 로 씁니다 (#567 재현)."""
    schema = loader.SCHEMA if schema is None else schema
    path = layout.curated_file(str(silver_dir), date.fromisoformat(collected_date), city)
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            name: None if name == blank else master_row().get(name, "x")
            for name in schema.names
        }
        for _ in range(rows)
    ]
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return path


def result_for(
    paths: list[Path],
    row_count: int,
    sources: dict | None = None,
    collected_date: str = COLLECTED_DATE,
) -> dict:
    return {
        "row_count": row_count,
        "locations": [str(path) for path in paths],
        # 적재 파티션. 핸들러가 읽은 원천의 최신 수집일로 정합니다.
        "collected_date": collected_date,
        # 읽기 상한. 낡음 판정은 이 값을 기준으로 합니다.
        "as_of": COLLECTED_DATE,
        "source_collected_dates": FRESH_SOURCES if sources is None else sources,
    }


def params_for(silver_dir: Path) -> dict:
    return {"params": {"curated_dir": str(silver_dir)}}


def test_스케줄이_원천_4종_AND_이다():
    """[필수] 하나라도 OR 로 빠지면 그 원천만 갱신돼도 조립이 돌아, 나머지 3개는
    지난달 값으로 마스터가 만들어집니다.

    원천 4개가 모두 매월 1일이라 AND 로 묶어도 신선도 손해가 없습니다. 예전에는
    3종이 주간이라 제원을 AND 에 넣으면 전체가 월 1회로 묶여 배차 자격이 최대 3주
    묵었고, 그래서 제원만 OR 로 빼뒀습니다 — 주기를 맞추면서 그 이유가 사라졌습니다.
    """
    condition = DAG.timetable.asset_condition

    assert type(condition).__name__ == "AssetAll", "OR 이 남아 있으면 부분 트리거됩니다"
    assert {asset.name for asset in condition.objects} == {
        assets.VEHICLE_CATALOG_CURATED.name,
        assets.UBER_ELIGIBLE_VEHICLES_CURATED.name,
        assets.LYFT_ELIGIBLE_VEHICLES_CURATED.name,
        assets.FUELECONOMY_VEHICLE_SPECS_CURATED.name,
    }


@pytest.mark.parametrize(("module_name", "asset"), sorted(UPSTREAM.items()))
def test_상류_DAG_가_검증_태스크에서_Asset_을_발행한다(module_name, asset):
    """발행이 없으면 이 DAG 는 영원히 트리거되지 않습니다 — 실패하지 않고 안 돕니다."""
    upstream = importlib.import_module(f"dags.{module_name}")
    dag = next(
        value for value in vars(upstream).values() if getattr(value, "dag_id", None)
    )

    # 적재 태스크가 아니라 검증 태스크여야 합니다. 적재 직후 발행하면 깨진
    # Curated 로 조립이 돌아갑니다.
    assert asset.name in {a.name for a in dag.get_task("validate_curated").outlets}
    assert not dag.get_task("raw_to_curated").outlets


def test_정상_결과는_통과하고_마스터_Asset_은_발행하지_않는다(tmp_path):
    path = write_master(tmp_path, rows=2)

    validate_silver(result_for([path], 2), **params_for(tmp_path))

    assert not DAG.get_task("validate_curated").outlets


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


def test_원천이_45일_넘게_낡으면_실패한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    # 대장 크롤링이 두 달 멈춘 상황. Extractor 는 기준일 이하 최신을 쓰므로
    # 이 가드가 없으면 지지난달 렌트료로 조용히 성공합니다.
    stale = {**FRESH_SOURCES, "vehicle_catalog": "2026-06-20"}

    with pytest.raises(ValueError, match="vehicle_catalog=54일"):
        validate_silver(result_for([path], 2, stale), **params_for(tmp_path))


def test_월_중_수동_실행은_통과한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    # 원천 4개가 모두 지난달 1일 수집분인 상태에서 월 중에 손으로 돌린 경우.
    # 주간 시절 한도(14일)를 그대로 뒀다면 여기서 막혀 재실행이 불가능합니다.
    sources = {key: "2026-07-01" for key in FRESH_SOURCES}

    validate_silver(result_for([path], 2, sources), **params_for(tmp_path))


def test_원천_수집일이_없으면_실패한다(tmp_path):
    path = write_master(tmp_path, rows=2)
    sources = {key: value for key, value in FRESH_SOURCES.items() if key != "lyft_eligible_vehicles"}

    with pytest.raises(ValueError, match="원천 수집일이 빠졌습니다"):
        validate_silver(result_for([path], 2, sources), **params_for(tmp_path))


# --- 계약상 비면 안 되는 컬럼 (#567) -----------------------------------------

def test_요금이_전_행_NULL_이면_실패한다(tmp_path):
    """스키마 검사는 이름과 타입만 봅니다.

    `weekly_lease_fee` 는 nullable 이라 전 행이 비어도 스키마는 통과하고, 그 값은
    Gold 의 렌탈 객단가로 이어집니다. 실제로 상류 컬럼명이 바뀌었을 때 142행 전부
    NULL 인 마스터가 여기를 지나갔습니다.
    """
    path = write_master(tmp_path, rows=2, blank="weekly_lease_fee")

    with pytest.raises(ValueError, match="weekly_lease_fee 이 2/2 행에서 비었습니다"):
        validate_silver(result_for([path], 2), **params_for(tmp_path))


def test_자격이_없어_비는_컬럼은_통과시킨다(tmp_path):
    """`platform` 은 자격이 없으면 NULL 이 정상이라 계약에 없습니다."""
    path = write_master(tmp_path, rows=2, blank="platform")

    validate_silver(result_for([path], 2), **params_for(tmp_path))


def test_낡음_판정은_적재_파티션이_아니라_as_of_기준이다(tmp_path):
    """`collected_date` 는 원천 날짜에서 나온 값입니다.

    그걸 기준으로 나이를 재면 원천이 전부 반 년 낡아도 서로 같은 날짜라
    나이가 0 으로 나오고 가드가 통째로 무력해집니다. 실패하지 않으니 지난해
    렌트료로 추천이 나가는 것을 아무도 모릅니다.
    """
    stale = {key: "2026-01-05" for key in FRESH_SOURCES}
    path = write_master(tmp_path, rows=2, collected_date="2026-01-05")
    result = result_for([path], 2, stale, collected_date="2026-01-05")

    with pytest.raises(ValueError, match="한도 45일"):
        validate_silver(result, **params_for(tmp_path))
