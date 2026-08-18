"""Lyft 자격 차량 DAG의 적재 경계와 GX 데이터 품질 규칙을 검증합니다.

실제 Parquet을 tmp_path에 만들고 validate_bronze·validate_silver Task 함수를
직접 호출합니다. Handler 응답·경로·파일 경계와 Bronze/Silver GX 실패를 분리해
검증합니다. Silver의 논리 문자열 타입과 숫자 폭도 확인하며 네트워크는 사용하지 않습니다.
"""

import importlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import lyft_eligible_vehicles_raw_to_silver_dag as dag_module

layout = importlib.import_module(
    "shared.aws_lambda.common.lyft_eligible_vehicles_layout"
)
bronze_loader = importlib.import_module(
    "sub.aws_lambda.functions.lyft_eligible_vehicles_raw_to_bronze.loader"
)
silver_loader = importlib.import_module(
    "sub.aws_lambda.functions.lyft_eligible_vehicles_bronze_to_silver.loader"
)

DAG = dag_module.lyft_eligible_vehicles_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 49, 22, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-08-11"
CITY = "new-york"

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert dag_module.slack_failure_callback in validation_task.on_failure_callback


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


def write_bronze_records(
    bronze_dir,
    records: list[dict],
    *,
    city: str = CITY,
    schema=None,
) -> str:
    path = layout.bronze_file(str(bronze_dir), city, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema or bronze_loader.SCHEMA)
    pq.write_table(table, path)
    return str(path)


def write_bronze(bronze_dir, city: str = CITY, rows: int = 3) -> str:
    return write_bronze_records(bronze_dir, bronze_rows(rows), city=city)


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


def test_규칙대로_적재된_Bronze_는_통과한다(tmp_path):
    path = write_bronze(tmp_path)

    validate_bronze(
        bronze_result([path]),
        params={"bronze_dir": str(tmp_path), "city_slug": CITY},
    )


def test_Bronze_실제_행_수와_Handler_row_count가_다르면_GX가_실패한다(tmp_path):
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match=r"expect_table_row_count_to_equal\[table\]"):
        validate_bronze(
            bronze_result([path], row_count=4),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_필수_컬럼이_없으면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        field for field in bronze_loader.SCHEMA if field.name != "model"
    )
    records = bronze_rows()
    for record in records:
        record.pop("model")
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize(
    "column",
    [
        "city_slug",
        "make",
        "model",
        "min_year",
        "products",
        "raw_eligibility",
        "raw_vehicle",
        "source_url",
        "collected_at",
    ],
)
def test_Bronze_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = bronze_rows()
    records[0][column] = None
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_not_be_null\[{column}\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_파일_내_도시가_요청_도시와_다르면_GX가_실패한다(
    tmp_path, caplog
):
    records = bronze_rows()
    records[0]["city_slug"] = "chicago"
    path = write_bronze_records(tmp_path, records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[city_slug\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=city_slug" in caplog.text
    assert "observed_value=['chicago']" in caplog.text


@pytest.mark.parametrize("products", [[], ["   "]], ids=["empty", "blank_item"])
def test_Bronze_products가_비었으면_GX가_실패한다(tmp_path, products):
    records = bronze_rows()
    records[0]["products"] = products
    path = write_bronze_records(tmp_path, records)

    expected_rule = (
        r"expect_column_values_to_be_between\[products_count\]"
        if not products
        else r"expect_column_values_to_be_in_set\[products_nonblank\]"
    )
    with pytest.raises(ValueError, match=expected_rule):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize(
    "column", ["make", "model", "raw_eligibility", "raw_vehicle", "source_url"]
)
def test_Bronze_필수_문자열이_비어있으면_GX가_실패한다(tmp_path, column):
    records = bronze_rows()
    records[0][column] = "   "
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_match_regex\[{column}\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize("min_year", [1979, 2101])
def test_Bronze_연식이_범위를_벗어나면_GX가_실패한다(tmp_path, min_year):
    records = bronze_rows()
    records[0]["min_year"] = min_year
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[min_year\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_min_year_타입이_다르면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.string()) if field.name == "min_year" else field
        for field in bronze_loader.SCHEMA
    )
    records = bronze_rows()
    for record in records:
        record["min_year"] = "2018"
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_of_type\[min_year\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_collected_at_UTC_날짜가_수집일과_다르면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    records[0]["collected_at"] = COLLECTED_AT + timedelta(days=1)
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[collected_date_utc\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_collected_at에_시간대가_없으면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.timestamp("us"))
        if field.name == "collected_at"
        else field
        for field in bronze_loader.SCHEMA
    )
    records = bronze_rows()
    for record in records:
        record["collected_at"] = COLLECTED_AT.replace(tzinfo=None)
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[collected_at_has_timezone\]",
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_collected_at의_UTC_날짜가_같아도_시간대가_UTC가_아니면_GX가_실패한다(
    tmp_path,
):
    collected_at = COLLECTED_AT.astimezone(ZoneInfo("America/New_York"))
    records = bronze_rows()
    for record in records:
        record["collected_at"] = collected_at
    schema = pa.schema(
        pa.field(
            field.name,
            pa.timestamp(field.type.unit, tz="America/New_York"),
        )
        if field.name == "collected_at"
        else field
        for field in bronze_loader.SCHEMA
    )
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError,
        match=(
            r"expect_column_values_to_be_in_set"
            r"\[collected_at_timezone_is_utc\]"
        ),
    ):
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


def test_요청한_도시와_수집한_도시가_다르면_실패한다(tmp_path):
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


@pytest.mark.parametrize(
    "row_count", [0, -1, "3", None, True], ids=["zero", "negative", "str", "none", "bool"]
)
def test_row_count_가_수상하면_실패한다(tmp_path, row_count):
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


def test_파일이_없으면_실패한다(tmp_path):
    missing = str(layout.bronze_file(str(tmp_path), CITY, COLLECTED_AT))

    with pytest.raises(FileNotFoundError):
        validate_bronze(
            bronze_result([missing]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_도시_파일이_0바이트면_실패한다(tmp_path):
    path = layout.silver_file(str(tmp_path), COLLECTED_AT.date(), CITY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()

    with pytest.raises(ValueError, match="비어 있습니다"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_도시_하나가_0행이면_합계가_맞아도_GX가_실패한다(tmp_path):
    paths = [
        write_silver(tmp_path, "new-york", silver_rows(4)),
        write_silver(tmp_path, "chicago", []),
    ]

    with pytest.raises(ValueError, match="expect_table_row_count_to_be_between"):
        validate_silver(
            silver_result(paths, row_count=4), params={"silver_dir": str(tmp_path)}
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


def test_Bronze_layout_규칙과_다른_경로면_실패한다(tmp_path):
    stray = tmp_path / "elsewhere" / f"{COLLECTED_AT:%Y%m%dT%H%M%SZ}.parquet"
    stray.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(bronze_rows(), schema=bronze_loader.SCHEMA), stray
    )

    with pytest.raises(ValueError, match="layout 규칙"):
        validate_bronze(
            bronze_result([str(stray)]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_파일명의_수집일이_다르면_실패한다(tmp_path):
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

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_min_year_타입이_다르면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.int32()) if field.name == "min_year" else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, CITY, silver_rows(), schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_of_type\[min_year\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_string과_large_string은_논리_타입이_같아_통과한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.large_string())
        if field.name == "make_key"
        else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, CITY, silver_rows(), schema=schema)

    validate_silver(silver_result([path]), params={"silver_dir": str(tmp_path)})


def test_행_수_합계가_row_count_와_다르면_실패한다(tmp_path):
    path = write_silver(tmp_path, CITY, silver_rows(2))

    with pytest.raises(ValueError, match="행 수 합계"):
        validate_silver(
            silver_result([path], row_count=99), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    "column", ["make_key", "model_key", "product", "min_year", "bronze_path"]
)
def test_Silver_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = None
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_not_be_null\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("column", ["make_key", "model_key", "product", "bronze_path"])
def test_Silver_문자열이_비어있으면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = "   "
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_match_regex\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_Lyft_상품이_아니면_GX가_실패하고_규칙을_로그한다(
    tmp_path, caplog
):
    records = silver_rows()
    records[0]["product"] = "Standard"
    path = write_silver(tmp_path, CITY, records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[product\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )

    assert "gx_validation failed layer=silver" in caplog.text
    assert "expectation=expect_column_values_to_be_in_set" in caplog.text
    assert "column=product" in caplog.text
    assert "unexpected_count=1" in caplog.text
    assert "observed_value=['Standard']" in caplog.text


@pytest.mark.parametrize("min_year", [1979, 2101])
def test_Silver_연식이_범위를_벗어나면_GX가_실패한다(tmp_path, min_year):
    records = silver_rows()
    records[0]["min_year"] = min_year
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[min_year\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_차종과_상품이_중복되면_GX가_실패한다(tmp_path):
    duplicate = silver_rows(1)[0]
    path = write_silver(tmp_path, CITY, [duplicate, dict(duplicate)])

    with pytest.raises(ValueError, match="expect_compound_columns_to_be_unique"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )
