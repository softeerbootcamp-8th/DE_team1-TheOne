"""차종별 제원 DAG의 적재 경계와 GX 데이터 품질 규칙을 검증합니다.

이 DAG 는 **매월 1일에만** 돕니다. 벌크 CSV 가 빈 채로 적재돼도 다음 실행까지
한 달 동안 아무도 모릅니다. 매일 도는 DAG 보다 검증이 더 중요한 이유입니다.

검증 태스크의 값어치는 "통과한다" 가 아니라 "불량을 통과시키지 않는다" 입니다.
그래서 이 파일은 정상·논리 타입 경계·불량 시나리오로 짜여 있습니다.

Bronze는 원본 84컬럼 전체를 고정하지 않고 Silver에 필요한 컬럼과 변환 가능한 행 비율을
검증합니다. Silver는 조인 키·연식·선택형 제원 값과 출처별 ID 중복을 검증합니다.

실제 Parquet 을 tmp_path 에 씁니다. 20MB CSV 를 내려받지 않습니다.
"""

import importlib
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import fueleconomy_vehicle_specs_raw_to_silver_dag as dag_module

layout = importlib.import_module("lambda.functions.common.vehicle_specs_layout")
bronze_loader = importlib.import_module(
    "lambda.functions.fueleconomy_vehicle_specs_raw_to_bronze.loader"
)
silver_loader = importlib.import_module(
    "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.loader"
)

DAG = dag_module.fueleconomy_vehicle_specs_dag
COLLECTED_AT = datetime(2026, 1, 1, 4, 0, 0, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-01-01"
SOURCE = "fueleconomy.gov"

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert dag_module.slack_failure_callback in validation_task.on_failure_callback


def bronze_rows(count: int = 3) -> list[dict]:
    return [
        {
            "id": str(index),
            "year": "2026",
            "make": "Toyota",
            "model": f"RAV4 {index}",
            "baseModel": "RAV4",
            "comb08": "30",
            "combE": "0",
            "range": "0",
            "atvType": None,
            "extra_source_column": "preserved",
            "collected_at": COLLECTED_AT,
        }
        for index in range(count)
    ]


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


def write_bronze_records(
    bronze_dir,
    records: list[dict],
    *,
    source: str = SOURCE,
    schema=None,
) -> str:
    path = layout.bronze_file(str(bronze_dir), source, COLLECTED_AT)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        records,
        schema=bronze_loader.build_schema(records[0]) if schema is None else schema,
    )
    pq.write_table(table, path)
    return str(path)


def write_bronze(bronze_dir, source: str = SOURCE, rows: int = 3) -> str:
    records = bronze_rows(rows)
    schema = None if records else bronze_loader.build_schema(bronze_rows(1)[0])
    return write_bronze_records(
        bronze_dir, records, source=source, schema=schema
    )


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


def test_Bronze_실제_행_수와_Handler_row_count가_다르면_GX가_실패한다(tmp_path):
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match=r"expect_table_row_count_to_equal\[table\]"):
        validate_bronze(
            bronze_result([path], row_count=4),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Bronze_Silver_필수_원본_컬럼이_없으면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    for record in records:
        record.pop("model")
    schema = bronze_loader.build_schema(records[0])
    path = write_bronze_records(tmp_path, records, schema=schema)

    with pytest.raises(ValueError, match=r"expect_column_to_exist\[model\]"):
        validate_bronze(
            bronze_result([path]), params={"bronze_dir": str(tmp_path)}
        )


def test_Bronze_변환_불가_행이_1퍼센트면_통과한다(tmp_path):
    records = bronze_rows(100)
    records[0]["model"] = None
    path = write_bronze_records(tmp_path, records)

    validate_bronze(
        bronze_result([path], row_count=100),
        params={"bronze_dir": str(tmp_path)},
    )


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("id", None),
        ("year", "not-a-year"),
        ("make", "   "),
        ("model", None),
        ("comb08", "not-a-number"),
        ("combE", "-1"),
        ("range", "Infinity"),
    ],
)
def test_Bronze_변환_불가_행이_1퍼센트를_넘으면_GX가_실패한다(
    tmp_path, column, value
):
    records = bronze_rows(100)
    records[0][column] = value
    records[1][column] = value
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[row_is_transformable\]",
    ):
        validate_bronze(
            bronze_result([path], row_count=100),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Bronze_collected_at_UTC_날짜가_수집일과_다르면_GX가_실패한다(tmp_path):
    records = bronze_rows()
    records[0]["collected_at"] = COLLECTED_AT.replace(day=2)
    path = write_bronze_records(tmp_path, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[collected_date_utc\]",
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
        for field in bronze_loader.build_schema(records[0])
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
        for field in bronze_loader.build_schema(records[0])
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


def test_출처가_여럿이어도_행_수_합계가_맞으면_통과한다(tmp_path):
    paths = [
        write_silver(tmp_path, "fueleconomy.gov", silver_rows()),
        write_silver(tmp_path, "othersource", silver_rows()),
    ]

    validate_silver(silver_result(paths), params={"silver_dir": str(tmp_path)})


def test_Silver_필수_컬럼이_없으면_GX가_실패한다(tmp_path):
    schema = pa.schema(
        field for field in silver_loader.SCHEMA if field.name != "model_key"
    )
    records = silver_rows()
    for record in records:
        record.pop("model_key")
    path = write_silver(tmp_path, SOURCE, records, schema=schema)

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    "column", ["source_id", "year", "make_key", "model_key", "bronze_path"]
)
def test_Silver_필수값이_NULL이면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = None
    path = write_silver(tmp_path, SOURCE, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_not_be_null\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("column", ["source_id", "make_key", "model_key", "bronze_path"])
def test_Silver_필수_문자열이_비어있으면_GX가_실패한다(tmp_path, column):
    records = silver_rows()
    records[0][column] = "   "
    path = write_silver(tmp_path, SOURCE, records)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_match_regex\[{column}\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize("year", [1979, 2101])
def test_Silver_연식이_범위를_벗어나면_GX가_실패한다(tmp_path, year):
    records = silver_rows()
    records[0]["year"] = year
    path = write_silver(tmp_path, SOURCE, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[year\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("combined_mpg", -1.0),
        ("combined_kwh_per_100mi", math.inf),
        ("range_miles", -1.0),
    ],
)
def test_Silver_제원_값이_비정상이면_GX가_실패한다(
    tmp_path, caplog, column, value
):
    records = silver_rows()
    records[0][column] = value
    path = write_silver(tmp_path, SOURCE, records)

    with caplog.at_level("ERROR"), pytest.raises(ValueError, match=column):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )

    assert "gx_validation failed layer=silver" in caplog.text
    assert f"column={column}" in caplog.text
    assert "unexpected_count=1" in caplog.text
    assert "observed_value=" in caplog.text


def test_Silver_int32_연식은_int16_계약과_달라_GX가_실패한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.int32()) if field.name == "year" else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, SOURCE, silver_rows(), schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_of_type\[year\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_source_id가_중복되면_GX가_실패한다(tmp_path):
    records = silver_rows()
    records[1]["source_id"] = records[0]["source_id"]
    path = write_silver(tmp_path, SOURCE, records)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_unique\[source_id\]",
    ):
        validate_silver(
            silver_result([path]), params={"silver_dir": str(tmp_path)}
        )


def test_Silver_string과_large_string은_논리_타입이_같아_통과한다(tmp_path):
    schema = pa.schema(
        pa.field(field.name, pa.large_string())
        if field.name == "source_id"
        else field
        for field in silver_loader.SCHEMA
    )
    path = write_silver(tmp_path, SOURCE, silver_rows(), schema=schema)

    validate_silver(silver_result([path]), params={"silver_dir": str(tmp_path)})


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
    이걸 잡습니다. 월 1회 DAG 라 놓치면 한 달을 빈 데이터로 갑니다.
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

    with pytest.raises(ValueError, match="비어 있습니다"):
        validate_silver(
            silver_result([str(path)]), params={"silver_dir": str(tmp_path)}
        )


def test_출처_하나가_0행이면_합계가_맞아도_GX가_실패한다(tmp_path):
    """0바이트가 아니라 **정상 Parquet 인데 행이 0** 인 경우입니다.

    합계만 보면 통과합니다 — 다른 출처가 4행이고 row_count 가 4면 총합이 맞습니다.
    """
    paths = [
        write_silver(tmp_path, "fueleconomy.gov", silver_rows(4)),
        write_silver(tmp_path, "othersource", []),  # 정상 스키마, 0행
    ]

    with pytest.raises(
        ValueError, match=r"expect_table_row_count_to_be_between\[table\]"
    ):
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
    """월 1회라 수집일이 어긋나면 한 달치 파티션이 통째로 엇갈립니다."""
    path = write_bronze(tmp_path)

    with pytest.raises(ValueError, match="수집일"):
        validate_bronze(
            bronze_result([path], collected_date="2026-01-02"),
            params={"bronze_dir": str(tmp_path)},
        )


def test_Silver_스키마가_다르면_GX가_실패한다(tmp_path):
    other = pa.schema([("make_key", pa.string()), ("year", pa.int16())])
    path = write_silver(
        tmp_path, SOURCE, [{"make_key": "TOYOTA", "year": 2026}] * 2, schema=other
    )

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
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
