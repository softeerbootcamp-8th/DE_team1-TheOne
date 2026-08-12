"""차종별 제원 DAG 의 적재 결과 검증 태스크가 실제로 불량을 잡는지 봅니다.

이 DAG 는 **매년 1월 1일에만** 돕니다. 벌크 CSV 가 빈 채로 적재돼도 다음 실행까지
1년 동안 아무도 모릅니다. 매일 도는 DAG 보다 검증이 더 중요한 이유입니다.

검증 태스크의 값어치는 "통과한다" 가 아니라 "불량을 통과시키지 않는다" 입니다.
그래서 이 파일은 **정상 2건 + 불량 여러 건** 으로 짜여 있습니다.

Bronze 는 원본 84컬럼을 그대로 실어 스키마가 고정이 아니라(`build_schema` 가 행을 보고
만듭니다) 스키마 대조를 하지 않습니다. 대신 행 수가 0이 아닌지를 봅니다 —
빈 벌크 CSV 차단이 그 단계의 목적입니다.

실제 Parquet 을 tmp_path 에 씁니다. 20MB CSV 를 내려받지 않습니다.
"""

import importlib
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import fueleconomy_vehicle_specs_raw_to_silver_dag as dag_module

layout = importlib.import_module("lambda.functions.common.vehicle_specs_layout")
silver_loader = importlib.import_module(
    "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.loader"
)

DAG = dag_module.fueleconomy_vehicle_specs_dag
COLLECTED_AT = datetime(2026, 1, 1, 4, 0, 0, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-01-01"
SOURCE = "fueleconomy.gov"

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def silver_rows(count: int = 2) -> list[dict]:
    return [
        {
            "source_id": str(i),
            "year": 2026,
            "make_key": f"MAKE{i}",
            "model_key": f"MODEL{i}",
            "base_model_key": f"MODEL{i}",
            "combined_mpg": 34.0,
            "combined_kwh_per_100mi": 0.0,
            "range_miles": 0.0,
            "atv_type": None,
            "bronze_path": "/bronze/x.parquet",
        }
        for i in range(count)
    ]


def write_bronze(bronze_dir, source: str = SOURCE, rows: int = 3) -> str:
    """Bronze 는 스키마가 고정이 아니라 원본처럼 문자열 컬럼만 넣습니다."""
    path = layout.bronze_file(str(bronze_dir), source, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "id": [str(i) for i in range(rows)],
            "year": ["2026"] * rows,
            "make": ["Toyota"] * rows,
        }
    )
    pq.write_table(table, path)
    return str(path)


def write_silver(silver_dir, source: str, rows: list[dict], schema=None) -> str:
    path = layout.silver_file(str(silver_dir), COLLECTED_AT.date(), source)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema or silver_loader.SCHEMA)
    pq.write_table(table, path)
    return str(path)


def bronze_result(locations: list[str], **overrides) -> dict:
    return {
        "row_count": 3,
        "locations": locations,
        "collected_date": COLLECTED_DATE,
    } | overrides


def silver_result(locations: list[str], **overrides) -> dict:
    return {
        "row_count": 2 * len(locations),
        "locations": locations,
        "collected_date": COLLECTED_DATE,
    } | overrides


# --------------------------------------------------------------------------
# 정상
# --------------------------------------------------------------------------


def test_규칙대로_적재된_Bronze_는_통과한다(tmp_path):
    path = write_bronze(tmp_path)

    validate_bronze(bronze_result([path]), params={"bronze_dir": str(tmp_path)})


def test_출처가_여럿이어도_행_수_합계가_맞으면_통과한다(tmp_path):
    paths = [
        write_silver(tmp_path, "fueleconomy.gov", silver_rows()),
        write_silver(tmp_path, "othersource", silver_rows()),
    ]

    validate_silver(silver_result(paths), params={"silver_dir": str(tmp_path)})


# --------------------------------------------------------------------------
# 불량 — 핸들러 응답
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row_count", [0, -1, "3", None, True], ids=["zero", "negative", "str", "none", "bool"]
)
def test_row_count_가_수상하면_실패한다(tmp_path, row_count):
    """0 은 빈 벌크 CSV 가 그대로 적재된 상황입니다. True 는 int 라 그냥 두면 1 로 통과합니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result([path], row_count=row_count),
            params={"bronze_dir": str(tmp_path)},
        )


def test_빈_벌크_CSV_가_적재되면_실패한다(tmp_path):
    """이 DAG 가 검증을 두는 가장 큰 이유입니다.

    원본 CSV 가 비면 Bronze 도 0행으로 쓰이고 `row_count` 도 0 이 됩니다. 이때
    "파일 행 수 == row_count" 는 **0 == 0 으로 통과합니다.** `row_count > 0` 검사만이
    이걸 잡습니다. 연 1회 DAG 라 놓치면 1년을 빈 데이터로 갑니다.
    """
    path = write_bronze(tmp_path, rows=0)

    with pytest.raises(ValueError, match="1 이상"):
        validate_bronze(
            bronze_result([path], row_count=0), params={"bronze_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("locations", [[], [""], "not-a-list"], ids=["empty", "blank", "str"])
def test_locations_가_비었으면_실패한다(tmp_path, locations):
    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result(locations), params={"bronze_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    "collected_date", ["2026-1-1", "20260101", "", None], ids=["nopad", "nodash", "empty", "none"]
)
def test_collected_date_형식이_틀리면_실패한다(tmp_path, collected_date):
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result([path], collected_date=collected_date),
            params={"bronze_dir": str(tmp_path)},
        )


# --------------------------------------------------------------------------
# 불량 — 실제 파일
# --------------------------------------------------------------------------


def test_파일이_없으면_실패한다(tmp_path):
    missing = str(layout.bronze_file(str(tmp_path), SOURCE, COLLECTED_AT))

    with pytest.raises(FileNotFoundError):
        validate_bronze(
            bronze_result([missing]), params={"bronze_dir": str(tmp_path)}
        )


def test_출처_파일이_0바이트면_실패한다(tmp_path):
    path = layout.silver_file(str(tmp_path), COLLECTED_AT.date(), SOURCE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()

    with pytest.raises(ValueError, match="0바이트"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_출처_하나가_0행이면_합계가_맞아도_실패한다(tmp_path):
    """0바이트가 아니라 **정상 Parquet 인데 행이 0** 인 경우입니다.

    합계만 보면 통과합니다 — 다른 출처가 4행이고 row_count 가 4면 총합이 맞습니다.
    """
    paths = [
        write_silver(tmp_path, "fueleconomy.gov", silver_rows(4)),
        write_silver(tmp_path, "othersource", []),  # 정상 스키마, 0행
    ]

    with pytest.raises(ValueError, match="행이 없습니다"):
        validate_silver(
            silver_result(paths, row_count=4), params={"silver_dir": str(tmp_path)}
        )


def test_layout_규칙과_다른_경로면_실패한다(tmp_path):
    stray = tmp_path / "source=fueleconomy.gov" / "fueleconomy_vehicle_specs.parquet"
    stray.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(silver_rows(), schema=silver_loader.SCHEMA), stray
    )

    with pytest.raises(ValueError, match="layout 규칙"):
        validate_silver(
            silver_result([str(stray)]), params={"silver_dir": str(tmp_path)}
        )


def test_Bronze_파일명의_수집일이_다르면_실패한다(tmp_path):
    """연 1회라 수집일이 어긋나면 1년치 파티션이 통째로 엇갈립니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match="수집일"):
        validate_bronze(
            bronze_result([path], collected_date="2026-01-02"),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Silver_스키마가_다르면_실패한다(tmp_path):
    other = pa.schema([("make_key", pa.string()), ("year", pa.int16())])
    path = write_silver(
        tmp_path, SOURCE, [{"make_key": "TOYOTA", "year": 2026}] * 2, schema=other
    )

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_행_수_합계가_row_count_와_다르면_실패한다(tmp_path):
    path = write_silver(tmp_path, SOURCE, silver_rows(2))

    with pytest.raises(ValueError, match="행 수 합계"):
        validate_silver(
            silver_result([path], row_count=99), params={"silver_dir": str(tmp_path)}
        )


def test_같은_출처가_두_번_적재되면_실패한다(tmp_path):
    path = write_silver(tmp_path, SOURCE, silver_rows())

    with pytest.raises(ValueError, match="두 번"):
        validate_silver(
            silver_result([path, path]), params={"silver_dir": str(tmp_path)}
        )
