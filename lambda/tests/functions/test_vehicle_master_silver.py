"""차량 마스터 통합 Silver 시나리오 (원천 Silver 파티션을 직접 깔고 핸들러 실행).

 1. 원천 4개의 최신 파티션을 각각 고름 (제원만 1년 전 파티션)
 2. as_of 이후 파티션은 건너뜀
 3. 차종 1대가 자격 수만큼 펼쳐지고 platform 이 uber / lyft 로 구분됨
 4. 자격 없는 차종도 platform · product NULL 로 남음
 5. base_model_key 폴백 + spec_match_level=BASE_MODEL
 6. 제원을 못 찾으면 NONE, 연비 · fuel_type NULL
 7. 대표 제원은 "연비 있는 것 > 최신 연식"
 8. 대장에 중복 차종이면 실패
 9. 자격 목록에 중복 행이면 실패
10. 같은 날 재실행하면 덮어씀
11. Loader 가 layout 경로에 SCHEMA 대로 쓰고 city 는 컬럼에 없음
"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from functions.common import vehicle_master_layout as layout
from functions.vehicle_master_silver.handler import lambda_handler as to_master
from functions.vehicle_master_silver.loader import SCHEMA

AS_OF = "2026-08-13"
CITY = "new-york"
VENDOR = "fasttrack"
SPECS_SOURCE = "fueleconomy.gov"

# 원천 Silver 의 실제 스키마 (각 loader.SCHEMA 와 같은 모양). 파티션 키(vendor /
# source / city)는 파일 안에 없으므로 여기에도 없습니다.
CATALOG_SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),
        ("model_key", pa.string()),
        ("weekly_price_usd", pa.float64()),
        ("bronze_path", pa.string()),
    ]
)
SPECS_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),
        ("year", pa.int16()),
        ("make_key", pa.string()),
        ("model_key", pa.string()),
        ("base_model_key", pa.string()),
        ("combined_mpg", pa.float64()),
        ("combined_kwh_per_100mi", pa.float64()),
        ("range_miles", pa.float64()),
        ("atv_type", pa.string()),
        ("bronze_path", pa.string()),
    ]
)
ELIGIBILITY_SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),
        ("model_key", pa.string()),
        ("product", pa.string()),
        ("min_year", pa.int16()),
        ("bronze_path", pa.string()),
    ]
)

CATALOG = [
    {
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "weekly_price_usd": 614.0,
        "bronze_path": "bronze/vehicle_catalog.parquet",
    },
    {
        "make_key": "MITSUBISHI",
        "model_key": "OUTLANDER SPORT",
        "weekly_price_usd": 529.0,
        "bronze_path": "bronze/vehicle_catalog.parquet",
    },
    {
        "make_key": "HONDA",
        "model_key": "FIT",
        "weekly_price_usd": 514.0,
        "bronze_path": "bronze/vehicle_catalog.parquet",
    },
]

SPECS = [
    # 같은 차종의 두 연식. 최신(2024)이 대표가 되어야 합니다.
    {
        "source_id": "1",
        "year": 2022,
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "base_model_key": "CAMRY",
        "combined_mpg": 32.0,
        "combined_kwh_per_100mi": 0.0,
        "range_miles": None,
        "atv_type": None,
        "bronze_path": "bronze/specs.parquet",
    },
    {
        "source_id": "2",
        "year": 2024,
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "base_model_key": "CAMRY",
        "combined_mpg": 51.0,
        "combined_kwh_per_100mi": 0.0,
        "range_miles": None,
        "atv_type": "Hybrid",
        "bronze_path": "bronze/specs.parquet",
    },
    # 대장은 "OUTLANDER SPORT", 제원은 구동방식이 붙어 model_key 로 안 붙습니다.
    {
        "source_id": "3",
        "year": 2023,
        "make_key": "MITSUBISHI",
        "model_key": "OUTLANDER SPORT 4WD",
        "base_model_key": "OUTLANDER SPORT",
        "combined_mpg": 26.0,
        "combined_kwh_per_100mi": 0.0,
        "range_miles": None,
        "atv_type": None,
        "bronze_path": "bronze/specs.parquet",
    },
]

UBER = [
    {
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "product": "UberX",
        "min_year": 2010,
        "bronze_path": "bronze/uber.parquet",
    },
    {
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "product": "Comfort",
        "min_year": 2015,
        "bronze_path": "bronze/uber.parquet",
    },
    {
        "make_key": "MITSUBISHI",
        "model_key": "OUTLANDER SPORT",
        "product": "UberX",
        "min_year": 2010,
        "bronze_path": "bronze/uber.parquet",
    },
]

LYFT = [
    {
        "make_key": "TOYOTA",
        "model_key": "CAMRY",
        "product": "Extra Comfort",
        "min_year": 2016,
        "bronze_path": "bronze/lyft.parquet",
    },
]


def write_source(
    silver_dir: Path,
    dataset: str,
    collected_date: str,
    sub_key: str,
    sub_value: str,
    rows: list[dict],
    schema: pa.Schema,
) -> Path:
    path = (
        silver_dir
        / dataset
        / f"collected_date={collected_date}"
        / f"{sub_key}={sub_value}"
        / f"{dataset}.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def build_sources(
    silver_dir: Path,
    *,
    catalog: list[dict] = CATALOG,
    specs: list[dict] = SPECS,
    uber: list[dict] = UBER,
    lyft: list[dict] = LYFT,
    catalog_date: str = "2026-08-12",
    # 제원은 연 1회 수집이라 늘 훨씬 오래된 파티션에 있습니다.
    specs_date: str = "2025-10-01",
    uber_date: str = "2026-08-11",
    lyft_date: str = "2026-08-12",
) -> None:
    write_source(
        silver_dir, "vehicle_catalog", catalog_date, "vendor", VENDOR,
        catalog, CATALOG_SCHEMA,
    )
    write_source(
        silver_dir, "fueleconomy_vehicle_specs", specs_date, "source", SPECS_SOURCE,
        specs, SPECS_SCHEMA,
    )
    write_source(
        silver_dir, "uber_eligible_vehicles", uber_date, "city", CITY,
        uber, ELIGIBILITY_SCHEMA,
    )
    write_source(
        silver_dir, "lyft_eligible_vehicles", lyft_date, "city", CITY,
        lyft, ELIGIBILITY_SCHEMA,
    )


def run(silver_dir: Path, as_of: str = AS_OF) -> dict:
    return to_master(event={"silver_dir": str(silver_dir), "collected_date": as_of})


def read_rows(result: dict) -> list[dict]:
    return pq.ParquetFile(result["locations"][0]).read().to_pylist()


def find(rows: list[dict], model_key: str, platform=None, product=None) -> dict:
    matched = [
        row
        for row in rows
        if row["model_key"] == model_key
        and row["platform"] == platform
        and row["product"] == product
    ]
    assert len(matched) == 1, f"{model_key} / {platform} / {product}: {len(matched)}건"
    return matched[0]


def test_원천마다_수집일이_달라도_각자의_최신_파티션을_읽는다(tmp_path):
    build_sources(tmp_path)
    # 지난주 대장. 파티션이 여러 개일 때 최신이 아니라 아무거나 고르면 지난주
    # 가격으로 추천이 나갑니다.
    stale = [{**row, "weekly_price_usd": 111.0} for row in CATALOG]
    write_source(
        tmp_path, "vehicle_catalog", "2026-08-05", "vendor", VENDOR,
        stale, CATALOG_SCHEMA,
    )

    result = run(tmp_path)

    # 제원만 1년 가까이 오래된 파티션에 있습니다. 실행일 파티션을 그대로 찾으면
    # 여기서 FileNotFoundError 가 나거나 제원이 통째로 안 붙습니다.
    assert result["source_collected_dates"] == {
        "vehicle_catalog": "2026-08-12",
        "fueleconomy_vehicle_specs": "2025-10-01",
        "uber_eligible_vehicles": "2026-08-11",
        "lyft_eligible_vehicles": "2026-08-12",
    }
    rows = read_rows(result)
    assert all(row["weekly_price_usd"] != 111.0 for row in rows)
    assert find(rows, "CAMRY", "lyft", "Extra Comfort")["combined_mpg"] == 51.0


def test_기준일_이후에_수집된_파티션은_쓰지_않는다(tmp_path):
    build_sources(tmp_path)
    # 기준일 다음날 대장이 갱신돼 가격이 바뀐 상황. 과거 날짜로 다시 돌렸을 때
    # 이 파티션을 읽으면 그때의 결과를 재현할 수 없습니다.
    future = [{**row, "weekly_price_usd": 999.0} for row in CATALOG]
    write_source(
        tmp_path, "vehicle_catalog", "2026-08-14", "vendor", VENDOR,
        future, CATALOG_SCHEMA,
    )

    result = run(tmp_path)

    assert result["source_collected_dates"]["vehicle_catalog"] == "2026-08-12"
    assert all(row["weekly_price_usd"] != 999.0 for row in read_rows(result))


def test_차종_하나가_자격_수만큼_펼쳐지고_플랫폼이_구분된다(tmp_path):
    build_sources(tmp_path)

    rows = read_rows(run(tmp_path))

    camry = [row for row in rows if row["model_key"] == "CAMRY"]
    assert {(row["platform"], row["product"]) for row in camry} == {
        ("uber", "UberX"),
        ("uber", "Comfort"),
        ("lyft", "Extra Comfort"),
    }
    # 상품별 최소 연식은 접으면 사라지는 값입니다. 행마다 따로 남아야 합니다.
    assert find(rows, "CAMRY", "uber", "UberX")["min_year"] == 2010
    assert find(rows, "CAMRY", "uber", "Comfort")["min_year"] == 2015
    assert find(rows, "CAMRY", "lyft", "Extra Comfort")["min_year"] == 2016


def test_자격이_하나도_없는_차종도_남는다(tmp_path):
    build_sources(tmp_path)

    rows = read_rows(run(tmp_path))

    # HONDA FIT 은 두 플랫폼 어느 목록에도 없습니다. INNER JOIN 이 되면 통째로
    # 사라져서 "아무 상품도 못 받는 차" 라는 사실이 Gold 에 전달되지 않습니다.
    fit = find(rows, "FIT")
    assert fit["weekly_price_usd"] == 514.0
    assert fit["platform"] is None
    assert fit["product"] is None
    assert fit["min_year"] is None


def test_구동방식_접미사가_붙은_제원은_base_model_key로_붙인다(tmp_path):
    build_sources(tmp_path)

    row = find(read_rows(run(tmp_path)), "OUTLANDER SPORT", "uber", "UberX")

    assert row["spec_match_level"] == "BASE_MODEL"
    assert row["combined_mpg"] == 26.0
    assert row["spec_year"] == 2023


def test_제원을_못_찾으면_연료_구분까지_비운다(tmp_path):
    build_sources(tmp_path)

    fit = find(read_rows(run(tmp_path)), "FIT")

    assert fit["spec_match_level"] == "NONE"
    assert fit["combined_mpg"] is None
    # GAS 로 채우면 Gold 가 없는 연비로 에너지비를 계산하려 듭니다.
    assert fit["fuel_type"] is None


def test_대표_제원은_최신_연식보다_연비_있는_행을_먼저_고른다(tmp_path):
    # 최신 연식(2025)에 연비가 비어 있고, 직전 연식(2024)에만 값이 있는 경우.
    specs = SPECS + [
        {
            "source_id": "4",
            "year": 2025,
            "make_key": "TOYOTA",
            "model_key": "CAMRY",
            "base_model_key": "CAMRY",
            "combined_mpg": None,
            "combined_kwh_per_100mi": None,
            "range_miles": None,
            "atv_type": "Hybrid",
            "bronze_path": "bronze/specs.parquet",
        }
    ]
    build_sources(tmp_path, specs=specs)

    row = find(read_rows(run(tmp_path)), "CAMRY", "uber", "UberX")

    assert row["spec_year"] == 2024
    assert row["combined_mpg"] == 51.0
    assert row["fuel_type"] == "HYBRID"


def test_대장에_같은_차종이_두_번_있으면_실패한다(tmp_path):
    build_sources(tmp_path, catalog=CATALOG + [CATALOG[0]])

    # 통과시키면 자격 수만큼 곱해져 행이 조용히 배로 늘어납니다.
    with pytest.raises(ValueError, match="중복 차종"):
        run(tmp_path)


def test_자격_목록에_같은_상품이_두_번_있으면_실패한다(tmp_path):
    build_sources(tmp_path, uber=UBER + [UBER[0]])

    with pytest.raises(ValueError, match="중복 행"):
        run(tmp_path)


def test_같은_날_다시_돌리면_덮어쓴다(tmp_path):
    build_sources(tmp_path)

    first = run(tmp_path)
    second = run(tmp_path)

    assert first["locations"] == second["locations"]
    assert first["row_count"] == second["row_count"]
    assert len(read_rows(second)) == second["row_count"]


def test_layout_이_정한_경로에_스키마대로_쓴다(tmp_path):
    build_sources(tmp_path)

    result = run(tmp_path)

    path = Path(result["locations"][0])
    assert path == layout.silver_file(str(tmp_path), date.fromisoformat(AS_OF), CITY)
    table = pq.ParquetFile(path).read()
    assert table.schema == SCHEMA
    # city 와 collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
    assert "city" not in table.schema.names
    assert "collected_date" not in table.schema.names
