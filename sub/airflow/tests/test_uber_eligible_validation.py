"""Uber 자격 차량 Validation Task의 적재 경계와 GX 규칙을 검증합니다.

시나리오:
1. 정상 Bronze와 여러 도시 Silver가 통과한다.
2. Handler 응답·파일·layout·수집일·요청 도시·중복 도시·총 행 수 경계를 거부한다.
3. Bronze GX가 행 수·필수 컬럼/값·문자열·products·연식·수집시각을 검증한다.
4. Silver GX가 컬럼 순서·논리 타입·숫자 폭·필수값·연식·복합 유일성을 검증한다.
5. GX 실패 로그가 layer·expectation·column·unexpected_count·observed_value를 남긴다.
6. Validation Task가 1회 재시도·10분 지연·실패 콜백을 사용한다.

실제 Parquet을 tmp_path에 쓰며 네트워크는 사용하지 않습니다. Uber 상품명은 원천
표기를 보존하고 허용 목록이 없으므로 product allowlist는 검증하지 않습니다.
"""

import importlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import uber_eligible_vehicles_raw_to_silver_dag as dag_module

layout = importlib.import_module(
    "shared.aws_lambda.common.uber_eligible_vehicles_layout"
)
bronze_loader = importlib.import_module(
    "sub.aws_lambda.functions.uber_eligible_vehicles_raw_to_bronze.loader"
)
silver_loader = importlib.import_module(
    "sub.aws_lambda.functions.uber_eligible_vehicles_bronze_to_silver.loader"
)

DAG = dag_module.uber_eligible_vehicles_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 53, 54, tzinfo=timezone.utc)
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
            "min_year": 2010,
            "products": ["UberX"],
            "raw_eligibility": "2010 (UberX)",
            "collected_at": COLLECTED_AT,
        }
        for i in range(count)
    ]


def silver_rows(count: int = 2) -> list[dict]:
    return [
        {
            "make_key": f"MAKE{i}",
            "model_key": f"MODEL{i}",
            "product": "UberX",
            "min_year": 2010,
            "bronze_path": "/bronze/x.parquet",
        }
        for i in range(count)
    ]


def write_bronze_records(
    bronze_dir, records: list[dict], *, city: str = CITY, schema=None
) -> str:
    path = layout.bronze_file(str(bronze_dir), city, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(records, schema=schema or bronze_loader.SCHEMA), path
    )
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


# --------------------------------------------------------------------------
# 정상
# --------------------------------------------------------------------------


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


@pytest.mark.parametrize("column", bronze_loader.SCHEMA.names)
def test_Bronze_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = bronze_rows()
    records[0][column] = None
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError, match=rf"expect_column_values_to_not_be_null\[{column}\]"
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_파일_속_도시가_요청_도시와_다르면_GX가_실패를_로그한다(
    tmp_path, caplog
):
    records = bronze_rows()
    records[0]["city_slug"] = "chicago"
    path = write_bronze_records(tmp_path, records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError, match=r"expect_column_values_to_be_in_set\[city_slug\]"
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "expectation=expect_column_values_to_be_in_set" in caplog.text
    assert "column=city_slug" in caplog.text
    assert "unexpected_count=1" in caplog.text
    assert "observed_value=['chicago']" in caplog.text


@pytest.mark.parametrize("column", ["city_slug", "make", "model", "raw_eligibility"])
def test_Bronze_필수_문자열이_공백이면_GX가_실패한다(tmp_path, column):
    records = bronze_rows()
    records[0][column] = "   "
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError, match=rf"expect_column_values_to_match_regex\[{column}\]"
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize(
    ("products", "schema"),
    [
        ([], bronze_loader.SCHEMA),
        (["   "], bronze_loader.SCHEMA),
        (
            "UberX",
            pa.schema(
                pa.field(field.name, pa.string())
                if field.name == "products"
                else field
                for field in bronze_loader.SCHEMA
            ),
        ),
    ],
    ids=["empty", "blank_item", "not_list"],
)
def test_Bronze_products가_목록이_아니거나_비었으면_GX가_실패한다(
    tmp_path, products, schema
):
    records = bronze_rows()
    for record in records:
        record["products"] = products
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(ValueError, match="products_(count|nonblank)"):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Bronze_min_year가_int16이_아니면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.string()) if field.name == "min_year" else field
        for field in bronze_loader.SCHEMA
    )
    records = bronze_rows()
    for record in records:
        record["min_year"] = "2010"
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_of_type\[min_year\]"
    ):
        validate_bronze(
            bronze_result([path]),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


@pytest.mark.parametrize("min_year", [1979, 2101])
def test_Bronze_연식이_1980_2100_범위를_벗어나면_GX가_실패한다(
    tmp_path, min_year
):
    records = bronze_rows()
    records[0]["min_year"] = min_year
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_between\[min_year\]"
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

    with pytest.raises(ValueError, match="collected_at_has_timezone"):
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


def test_Bronze_collected_at의_UTC_날짜가_수집일과_다르면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    records[0]["collected_at"] = COLLECTED_AT + timedelta(days=1)
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(ValueError, match="collected_date_utc"):
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
# 불량 — 도시
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

    with pytest.raises(ValueError, match="비어 있습니다"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_도시_하나가_0행이면_합계가_맞아도_실패한다(tmp_path):
    """0바이트가 아니라 **정상 Parquet 인데 행이 0** 인 경우입니다.

    합계만 보면 통과합니다 — 다른 도시가 4행이고 row_count 가 4면 총합이 맞습니다.
    그래서 도시별로 따로 봐야 잡힙니다.
    """
    paths = [
        write_silver(tmp_path, "new-york", silver_rows(4)),
        write_silver(tmp_path, "chicago", []),  # 정상 스키마, 0행
    ]

    with pytest.raises(ValueError, match="expect_table_row_count_to_be_between"):
        validate_silver(
            silver_result(paths, row_count=4), params={"silver_dir": str(tmp_path)}
        )


def test_layout_규칙과_다른_경로면_실패한다(tmp_path):
    stray = tmp_path / "city=new-york" / "uber_eligible_vehicles.parquet"
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
    """하루 경계에서 파티션이 어긋난 상황입니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match="수집일"):
        validate_bronze(
            bronze_result([path], collected_date="2026-08-12"),
            params={"bronze_dir": str(tmp_path), "city_slug": CITY},
        )


def test_Silver_컬럼_순서가_loader_계약과_다르면_GX가_실패한다(tmp_path):
    reversed_schema = pa.schema(reversed(silver_loader.SCHEMA))
    path = write_silver(tmp_path, CITY, silver_rows(), schema=reversed_schema)

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
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


@pytest.mark.parametrize("column", silver_loader.SCHEMA.names)
def test_Silver_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = None
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError, match=rf"expect_column_values_to_not_be_null\[{column}\]"
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("column", ["make_key", "model_key", "product", "bronze_path"])
def test_Silver_필수_문자열이_공백이면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = "   "
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError, match=rf"expect_column_values_to_match_regex\[{column}\]"
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_min_year가_int16이_아니면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.int32()) if field.name == "min_year" else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, CITY, silver_rows(), schema=schema)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_of_type\[min_year\]"
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("min_year", [1979, 2101])
def test_Silver_연식이_1980_2100_범위를_벗어나면_GX가_실패한다(
    tmp_path, min_year
):
    records = silver_rows()
    records[0]["min_year"] = min_year
    path = write_silver(tmp_path, CITY, records)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_between\[min_year\]"
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
