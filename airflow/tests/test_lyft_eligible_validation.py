"""Lyft 자격 차량 DAG 의 적재 결과 검증 태스크가 실제로 불량을 잡는지 봅니다.

핸들러는 쓰기에 성공하기만 하면 돌아옵니다. 그래서 **적재 결과가 비어 있어도 DAG 는
성공으로 끝납니다.** Silver 는 도시별로 파일을 여러 개 쓰는데, 그중 하나가 비어도
`row_count` 합계만 맞으면 아무도 모릅니다.

검증 태스크의 값어치는 "통과한다" 가 아니라 "불량을 통과시키지 않는다" 입니다.
그래서 이 파일은 **정상 2건 + 불량 여러 건** 으로 짜여 있습니다.

Lyft 만의 항목이 하나 있습니다 — 자격 페이지가 도시마다 다르므로, 요청한 도시와
수집한 도시가 같은지 봅니다. 다르면 조인 대상이 통째로 달라집니다.

실제 Parquet 을 tmp_path 에 씁니다. 네트워크는 타지 않습니다.
"""

import importlib
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import lyft_eligible_vehicles_raw_to_silver_dag as dag_module

layout = importlib.import_module(
    "lambda.functions.common.lyft_eligible_vehicles_layout"
)
bronze_loader = importlib.import_module(
    "lambda.functions.lyft_eligible_vehicles_raw_to_bronze.loader"
)
silver_loader = importlib.import_module(
    "lambda.functions.lyft_eligible_vehicles_bronze_to_silver.loader"
)

DAG = dag_module.lyft_eligible_vehicles_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 49, 22, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-08-11"
CITY = "new-york"

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def bronze_rows(count: int = 3) -> list[dict]:
    return [
        {
            "city_slug": CITY,
            "make": "Toyota",
            "model": f"MODEL{i}",
            "min_year": 2018,
            "products": ["Extra Comfort"],
            "raw_eligibility": "2018 (Extra Comfort)",
            "raw_vehicle": f"MODEL{i} 2018 (Extra Comfort)",
            "source_url": "https://www.lyft.com/driver/eligible-premium-vehicles",
            "collected_at": COLLECTED_AT,
        }
        for i in range(count)
    ]


def silver_rows(count: int = 2) -> list[dict]:
    return [
        {
            "make_key": f"MAKE{i}",
            "model_key": f"MODEL{i}",
            "product": "Extra Comfort",
            "min_year": 2018,
            "bronze_path": "/bronze/x.parquet",
        }
        for i in range(count)
    ]


def write_bronze(bronze_dir, city: str = CITY, rows: int = 3) -> str:
    path = layout.bronze_file(str(bronze_dir), city, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(bronze_rows(rows), schema=bronze_loader.SCHEMA)
    pq.write_table(table, path)
    return str(path)


def write_silver(silver_dir, city: str, rows: list[dict], schema=None) -> str:
    path = layout.silver_file(str(silver_dir), COLLECTED_AT.date(), city)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema or silver_loader.SCHEMA)
    pq.write_table(table, path)
    return str(path)


def bronze_result(locations: list[str], **overrides) -> dict:
    return {
        "row_count": 3,
        "locations": locations,
        "city_slug": CITY,
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

    validate_bronze(
        bronze_result([path]),
        params={"bronze_dir": str(tmp_path), "city_slug": CITY},
    )


def test_도시가_여럿이어도_행_수_합계가_맞으면_통과한다(tmp_path):
    paths = [
        write_silver(tmp_path, "new-york", silver_rows()),
        write_silver(tmp_path, "chicago", silver_rows()),
    ]

    validate_silver(silver_result(paths), params={"silver_dir": str(tmp_path)})


# --------------------------------------------------------------------------
# 불량 — Lyft 고유 (도시)
# --------------------------------------------------------------------------


def test_요청한_도시와_수집한_도시가_다르면_실패한다(tmp_path):
    """자격 페이지가 도시마다 달라, 어긋나면 조인 대상이 통째로 달라집니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match="도시"):
        validate_bronze(
            bronze_result([path], city_slug="chicago"),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_city_slug_가_빠지면_실패한다(tmp_path):
    path = write_bronze(tmp_path)
    result = bronze_result([path])
    del result["city_slug"]

    with pytest.raises(ValueError, match="도시"):
        validate_bronze(
            result, params={"bronze_dir": str(tmp_path), "city_slug": CITY}
        )


def test_같은_도시가_두_번_적재되면_실패한다(tmp_path):
    path = write_silver(tmp_path, CITY, silver_rows())

    with pytest.raises(ValueError, match="두 번"):
        validate_silver(
            silver_result([path, path]), params={"silver_dir": str(tmp_path)}
        )


# --------------------------------------------------------------------------
# 불량 — 핸들러 응답
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row_count", [0, -1, "3", None, True], ids=["zero", "negative", "str", "none", "bool"]
)
def test_row_count_가_수상하면_실패한다(tmp_path, row_count):
    """True 는 int 의 하위 타입이라 그냥 두면 1 로 통과합니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result([path], row_count=row_count),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize("locations", [[], [""], "not-a-list"], ids=["empty", "blank", "str"])
def test_locations_가_비었으면_실패한다(tmp_path, locations):
    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result(locations),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize(
    "collected_date", ["2026-8-11", "20260811", "", None], ids=["nopad", "nodash", "empty", "none"]
)
def test_collected_date_형식이_틀리면_실패한다(tmp_path, collected_date):
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result([path], collected_date=collected_date),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


# --------------------------------------------------------------------------
# 불량 — 실제 파일
# --------------------------------------------------------------------------


def test_파일이_없으면_실패한다(tmp_path):
    missing = str(layout.bronze_file(str(tmp_path), CITY, COLLECTED_AT))

    with pytest.raises(FileNotFoundError):
        validate_bronze(
            bronze_result([missing]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_도시_파일이_0바이트면_실패한다(tmp_path):
    """도시 하나가 통째로 빠지는 상황입니다. 합계만 보면 못 잡습니다."""
    path = layout.silver_file(str(tmp_path), COLLECTED_AT.date(), CITY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()

    with pytest.raises(ValueError, match="0바이트"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_layout_규칙과_다른_경로면_실패한다(tmp_path):
    stray = tmp_path / "city=new-york" / "lyft_eligible_vehicles.parquet"
    stray.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(silver_rows(), schema=silver_loader.SCHEMA), stray
    )

    with pytest.raises(ValueError, match="layout 규칙"):
        validate_silver(
            silver_result([str(stray)]), params={"silver_dir": str(tmp_path)}
        )


def test_Bronze_파일명의_수집일이_다르면_실패한다(tmp_path):
    """하루 경계에서 파티션이 어긋난 상황입니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match="수집일"):
        validate_bronze(
            bronze_result([path], collected_date="2026-08-12"),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Silver_스키마가_다르면_실패한다(tmp_path):
    other = pa.schema([("make_key", pa.string()), ("min_year", pa.int16())])
    path = write_silver(
        tmp_path, CITY, [{"make_key": "TOYOTA", "min_year": 2018}] * 2, schema=other
    )

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_행_수_합계가_row_count_와_다르면_실패한다(tmp_path):
    path = write_silver(tmp_path, CITY, silver_rows(2))

    with pytest.raises(ValueError, match="행 수 합계"):
        validate_silver(
            silver_result([path], row_count=99), params={"silver_dir": str(tmp_path)}
        )
