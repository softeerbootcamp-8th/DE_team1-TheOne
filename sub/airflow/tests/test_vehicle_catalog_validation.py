"""차량 대장 DAG의 적재 경계와 GX 데이터 품질 규칙을 검증합니다.

핸들러는 쓰기에 성공하기만 하면 돌아옵니다. 그래서 **적재 결과가 비어 있어도 DAG 는
성공으로 끝납니다.** 특히 Silver 는 업체별로 파일을 여러 개 쓰는데, 그중 하나가 비어도
`row_count` 합계만 맞으면 아무도 모릅니다.

검증 태스크의 값어치는 "통과한다" 가 아니라 "불량을 통과시키지 않는다" 입니다.
그래서 이 파일은 정상·논리 타입 경계·불량 시나리오로 짜여 있습니다.

Bronze는 Loader 컬럼·필수값·업체·주간 요금·수집일을 검증합니다. Silver는
업체별 행 수·조인 키·주간 요금·차량 중복을 검증합니다. Handler 응답과 파일·layout은
GX 실행 전 경계 검사로 남깁니다.

실제 Parquet 을 tmp_path 에 씁니다. 네트워크는 타지 않습니다.
"""

import importlib
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import vehicle_catalog_raw_to_silver_dag as dag_module

layout = importlib.import_module("sub.aws_lambda.common.vehicle_catalog_layout")
bronze_loader = importlib.import_module(
    "sub.aws_lambda.functions.vehicle_catalog_raw_to_bronze.loader"
)
silver_loader = importlib.import_module(
    "sub.aws_lambda.functions.vehicle_catalog_bronze_to_silver.loader"
)

DAG = dag_module.vehicle_catalog_dag
COLLECTED_AT = datetime(2026, 8, 9, 15, 47, 47, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-08-09"
VENDOR = "fasttrack"

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert (
            dag_module.slack_failure_callback
            in validation_task.on_failure_callback
        )


def bronze_rows(count: int = 3) -> list[dict]:
    return [
        {
            "make": "Toyota",
            "model": f"Camry {index}",
            "raw_name": f"TOYOTA CAMRY {index}",
            "price_usd": 514.0 + index,
            "price_period": "week",
            "image_url": f"https://example.com/camry-{index}.png",
            "booking_url": "https://example.com/book",
            "source_url": "https://example.com/catalog",
            "source_html_path": "/raw/source.html",
            "source_image_path": f"/raw/camry-{index}.png",
            "collected_at": COLLECTED_AT,
        }
        for index in range(count)
    ]


def silver_rows(count: int = 2) -> list[dict]:
    return [
        {
            "make_key": f"MAKE{i}",
            "model_key": f"MODEL{i}",
            "weekly_price_usd": 500.0 + i,
            "image_url": f"https://example.com/model-{i}.png",
            "bronze_path": "/bronze/x.parquet",
        }
        for i in range(count)
    ]


def write_silver(silver_dir, vendor: str, rows: list[dict], schema=None) -> str:
    path = layout.silver_file(str(silver_dir), COLLECTED_AT.date(), vendor)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema or silver_loader.SCHEMA)
    pq.write_table(table, path)
    return str(path)


def write_bronze_records(
    bronze_dir,
    records: list[dict],
    *,
    vendor: str = VENDOR,
    schema=None,
) -> str:
    path = layout.bronze_file(str(bronze_dir), vendor, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        records,
        schema=bronze_loader.SCHEMA if schema is None else schema,
    )
    pq.write_table(table, path)
    return str(path)


def write_bronze(bronze_dir, vendor: str = VENDOR, rows: int = 3) -> str:
    return write_bronze_records(bronze_dir, bronze_rows(rows), vendor=vendor)


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


def test_Bronze_실제_행_수와_Handler_row_count가_다르면_GX가_실패한다(tmp_path):
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match=r"expect_table_row_count_to_equal\[table\]"):
        validate_bronze(
            bronze_result([path], row_count=4),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Bronze_Loader_필수_컬럼이_없으면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    for record in records:
        record.pop("model")
    schema = pa.schema(
        field for field in bronze_loader.SCHEMA if field.name != "model"
    )
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError, match=r"expect_table_columns_to_match_ordered_list\[table\]"
    ):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    "column",
    [
        "make",
        "model",
        "price_usd",
        "price_period",
        "image_url",
        "source_url",
        "source_html_path",
        "source_image_path",
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
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_Bronze_원본에서_허용한_NULL은_통과한다(tmp_path):
    records = bronze_rows()
    records[0]["raw_name"] = None
    records[0]["booking_url"] = None
    path = write_bronze_records(tmp_path, records)

    validate_bronze(
        bronze_result([path]), params={"bronze_dir": str(tmp_path)}
    )


@pytest.mark.parametrize(
    "column",
    [
        "make",
        "model",
        "raw_name",
        "image_url",
        "booking_url",
        "source_url",
        "source_html_path",
        "source_image_path",
    ],
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
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_Bronze_업체가_수집_대상과_다르면_GX가_실패한다(tmp_path):
    path = write_bronze(tmp_path, vendor="unknown")

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_in_set\[vendor\]"
    ):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_Bronze_요금_주기가_week가_아니면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    records[0]["price_period"] = "day"
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_in_set\[price_period\]"
    ):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("price", [49.99, 5000.01, math.inf, math.nan])
def test_Bronze_주간_요금이_범위를_벗어나면_GX가_실패한다(
    tmp_path, caplog, price
):
    records = bronze_rows()
    records[0]["price_usd"] = price
    path = write_bronze_records(tmp_path, records)

    with caplog.at_level("ERROR"), pytest.raises(ValueError, match="price_usd"):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=price_usd" in caplog.text
    assert "unexpected_count=1" in caplog.text
    assert "observed_value=" in caplog.text


def test_Bronze_collected_at_UTC_날짜가_수집일과_다르면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    records[0]["collected_at"] = COLLECTED_AT.replace(day=10)
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_in_set\[collected_date_utc\]"
    ):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_Bronze_collected_at에_시간대가_없으면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    for record in records:
        record["collected_at"] = COLLECTED_AT.replace(tzinfo=None)
    schema = pa.schema(
        pa.field(field.name, pa.timestamp("us"))
        if field.name == "collected_at"
        else field
        for field in bronze_loader.SCHEMA
    )
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[collected_at_has_timezone\]",
    ):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
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
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_업체가_여럿이어도_행_수_합계가_맞으면_통과한다(tmp_path):
    paths = [
        write_silver(tmp_path, "fasttrack", silver_rows()),
        write_silver(tmp_path, "othervendor", silver_rows()),
    ]

    validate_silver(silver_result(paths), params={"silver_dir": str(tmp_path)})


def test_Silver_업체_파일_하나가_0행이면_GX가_실패한다(tmp_path):
    paths = [
        write_silver(tmp_path, "fasttrack", silver_rows(4)),
        write_silver(tmp_path, "othervendor", []),
    ]

    with pytest.raises(
        ValueError, match=r"expect_table_row_count_to_be_between\[table\]"
    ):
        validate_silver(
            silver_result(paths), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_필수_컬럼이_없으면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        field for field in silver_loader.SCHEMA if field.name != "model_key"
    )
    records = silver_rows()
    for record in records:
        record.pop("model_key")
    path = write_silver(tmp_path, VENDOR, records, schema=schema)

    with pytest.raises(
        ValueError, match=r"expect_table_columns_to_match_ordered_list\[table\]"
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("column", silver_loader.SCHEMA.names)
def test_Silver_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = None
    path = write_silver(tmp_path, VENDOR, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_not_be_null\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("column", ["make_key", "model_key", "bronze_path"])
def test_Silver_필수_문자열이_비어있으면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = "   "
    path = write_silver(tmp_path, VENDOR, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_match_regex\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("price", [49.99, 5000.01, math.inf, math.nan])
def test_Silver_주간_요금이_범위를_벗어나면_GX가_실패한다(tmp_path, price):
    records = silver_rows()
    records[0]["weekly_price_usd"] = price
    path = write_silver(tmp_path, VENDOR, records)

    with pytest.raises(ValueError, match="weekly_price_usd"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_float32_요금은_float64_계약과_달라_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.float32())
        if field.name == "weekly_price_usd"
        else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, VENDOR, silver_rows(), schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_of_type\[weekly_price_usd\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_같은_업체의_차량_조인키가_중복되면_GX가_실패한다(tmp_path):
    records = silver_rows()
    records[1]["make_key"] = records[0]["make_key"]
    records[1]["model_key"] = records[0]["model_key"]
    path = write_silver(tmp_path, VENDOR, records)

    with pytest.raises(
        ValueError, match=r"expect_compound_columns_to_be_unique\[make_key/model_key\]"
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
    path = write_silver(tmp_path, VENDOR, silver_rows(), schema=schema)

    validate_silver(silver_result([path]), params={"silver_dir": str(tmp_path)})


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
            params={"bronze_dir": str(tmp_path)},
        )


@pytest.mark.parametrize("locations", [[], [""], "not-a-list"], ids=["empty", "blank", "str"])
def test_locations_가_비었으면_실패한다(tmp_path, locations):
    with pytest.raises(ValueError):
        validate_bronze(
            bronze_result(locations), params={"bronze_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    "collected_date", ["2026-8-9", "20260809", "", None], ids=["nopad", "nodash", "empty", "none"]
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
    missing = str(layout.bronze_file(str(tmp_path), VENDOR, COLLECTED_AT))

    with pytest.raises(FileNotFoundError):
        validate_bronze(
            bronze_result([missing]), params={"bronze_dir": str(tmp_path)}
        )


def test_업체_파일이_0바이트면_실패한다(tmp_path):
    """업체 하나가 통째로 빠지는 상황입니다. 합계만 보면 못 잡습니다."""
    path = layout.silver_file(str(tmp_path), COLLECTED_AT.date(), VENDOR)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()

    with pytest.raises(ValueError, match="비어 있습니다"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_layout_규칙과_다른_경로면_실패한다(tmp_path):
    stray = tmp_path / "vendor=fasttrack" / "vehicle_catalog.parquet"
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
            bronze_result([path], collected_date="2026-08-10"),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Silver_스키마가_다르면_GX가_실패한다(tmp_path):
    other = pa.schema([("make_key", pa.string()), ("weekly_price_usd", pa.float64())])
    path = write_silver(
        tmp_path,
        VENDOR,
        [{"make_key": "TOYOTA", "weekly_price_usd": 564.0}] * 2,
        schema=other,
    )

    with pytest.raises(
        ValueError, match=r"expect_table_columns_to_match_ordered_list\[table\]"
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_행_수_합계가_row_count_와_다르면_실패한다(tmp_path):
    path = write_silver(tmp_path, VENDOR, silver_rows(2))

    with pytest.raises(ValueError, match="행 수 합계"):
        validate_silver(
            silver_result([path], row_count=99), params={"silver_dir": str(tmp_path)}
        )


def test_같은_업체가_두_번_적재되면_실패한다(tmp_path):
    path = write_silver(tmp_path, VENDOR, silver_rows())

    with pytest.raises(ValueError, match="두 번"):
        validate_silver(
            silver_result([path, path]), params={"silver_dir": str(tmp_path)}
        )
