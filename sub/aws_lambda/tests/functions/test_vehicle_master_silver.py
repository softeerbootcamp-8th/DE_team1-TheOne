"""차량 마스터 통합 Silver 시나리오 (원천 Silver 파티션을 직접 깔고 핸들러 실행).

 1. 원천 4개의 최신 파티션을 각각 고름 (제원만 1년 전 파티션)
 2. as_of 이후 파티션은 건너뜀
 3. 차종 1대가 자격 수만큼 펼쳐지고 platform 이 uber / lyft 로 구분됨
 4. 자격 없는 차종도 platform · product NULL 로 남음
 5. 구동방식만 다른 트림은 같은 차 (spec_match_level=DRIVETRAIN)
 6. 하이브리드 트림은 후보에서 제외 (#320)
 7. 이름이 더 긴 다른 차종이 후보에 안 섞임 (#320)
 8. 기준일에서 먼 연식 · 미출시 연식 제외 (#320)
 9. 후보의 연료가 갈리면 fuel_type=MIXED
10. 제원을 못 찾으면 NONE, 연비 · fuel_type NULL
11. 대장에 중복 차종이면 실패
12. 자격 목록에 중복 행이면 실패
13. 같은 날 재실행하면 덮어씀
14. Loader 가 layout 경로에 SCHEMA 대로 쓰고 city 는 컬럼에 없음
"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sub.aws_lambda.common import vehicle_master_layout as layout
from sub.aws_lambda.functions.vehicle_master_silver.handler import (
    lambda_handler as to_master,
)
from sub.aws_lambda.functions.vehicle_master_silver.loader import SCHEMA

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
        ("weekly_lease_fee", pa.float64()),
        ("image_url", pa.string()),
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

def catalog_row(make, model, price):
    return {
        "make_key": make,
        "model_key": model,
        "weekly_lease_fee": price,
        "image_url": f"https://example.com/{model.casefold().replace(' ', '-')}.png",
        "bronze_path": "bronze/vehicle_catalog.parquet",
    }


# 대장에는 트림이 없습니다. "SPORTAGE" 이지 "SPORTAGE FWD" 가 아닙니다.
CATALOG = [
    catalog_row("TOYOTA", "CAMRY", 614.0),
    catalog_row("KIA", "SPORTAGE", 574.0),
    catalog_row("MITSUBISHI", "OUTLANDER", 549.0),
    catalog_row("MITSUBISHI", "OUTLANDER SPORT", 529.0),
    catalog_row("HONDA", "FIT", 514.0),
]

def spec(source_id, year, make, model, base_model, mpg, atv_type=None, kwh=0.0):
    return {
        "source_id": source_id,
        "year": year,
        "make_key": make,
        "model_key": model,
        "base_model_key": base_model,
        "combined_mpg": mpg,
        "combined_kwh_per_100mi": kwh,
        "range_miles": None,
        "atv_type": atv_type,
        "bronze_path": "bronze/specs.parquet",
    }


# 실제 fueleconomy 원본의 모양을 따릅니다 (#320 에서 실수집분으로 확인한 것):
#   - 같은 차종이 연식 × 구동방식 × 파워트레인으로 여러 행
#   - `baseModel` 이 뭉툭해서 OUTLANDER SPORT 의 base 도 "OUTLANDER"
#   - 미출시 연식이 미리 올라옴 (2026-08 수집분에 2027년식)
SPECS = [
    # CAMRY: model_key 로 정확히 붙는 케이스. 연식 두 개.
    spec("1", 2024, "TOYOTA", "CAMRY", "CAMRY", 32.0),
    spec("2", 2025, "TOYOTA", "CAMRY", "CAMRY", 30.0),
    # SPORTAGE: 대장에는 트림이 없고 제원에는 내연/하이브리드가 섞여 있습니다.
    # 하이브리드(41)가 후보에 들어가면 연비를 40% 과대평가합니다.
    spec("3", 2025, "KIA", "SPORTAGE FWD", "SPORTAGE", 28.0),
    spec("4", 2025, "KIA", "SPORTAGE AWD", "SPORTAGE", 25.0),
    spec("5", 2025, "KIA", "SPORTAGE HYBRID FWD", "SPORTAGE", 41.0, "Hybrid"),
    spec("6", 2025, "KIA", "SPORTAGE X-PRO", "SPORTAGE", 24.0),
    # OUTLANDER 와 OUTLANDER SPORT 는 다른 차인데 base 가 둘 다 "OUTLANDER".
    spec("7", 2025, "MITSUBISHI", "OUTLANDER 4WD", "OUTLANDER", 26.0),
    spec("8", 2025, "MITSUBISHI", "OUTLANDER SPORT 2WD", "OUTLANDER", 27.0),
    spec("9", 2025, "MITSUBISHI", "OUTLANDER SPORT 4WD", "OUTLANDER", 25.0),
    # 미출시 연식. 기준일(2026-08-13) 기준 +1년까지만 후보입니다.
    spec("10", 2028, "MITSUBISHI", "OUTLANDER 4WD", "OUTLANDER", 99.0),
    # 오래된 연식. 기준일 -3년보다 이전이라 후보에서 빠집니다.
    spec("11", 2015, "TOYOTA", "CAMRY", "CAMRY", 12.0),
]

def eligibility_row(make, model, product, min_year, source):
    return {
        "make_key": make,
        "model_key": model,
        "product": product,
        "min_year": min_year,
        "bronze_path": f"bronze/{source}.parquet",
    }


UBER = [
    eligibility_row("TOYOTA", "CAMRY", "UberX", 2010, "uber"),
    eligibility_row("TOYOTA", "CAMRY", "Comfort", 2015, "uber"),
    eligibility_row("KIA", "SPORTAGE", "UberX", 2010, "uber"),
    eligibility_row("MITSUBISHI", "OUTLANDER", "UberX", 2010, "uber"),
    eligibility_row("MITSUBISHI", "OUTLANDER SPORT", "UberX", 2010, "uber"),
]

LYFT = [
    eligibility_row("TOYOTA", "CAMRY", "Extra Comfort", 2016, "lyft"),
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
    # 제원은 월 1회 수집이라 주간 원천보다 오래된 파티션에 있습니다.
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
    stale = [{**row, "weekly_lease_fee": 111.0} for row in CATALOG]
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
    assert all(row["weekly_lease_fee"] != 111.0 for row in rows)
    assert find(rows, "CAMRY", "lyft", "Extra Comfort")["combined_mpg_max"] == 32.0


def test_기준일_이후에_수집된_파티션은_쓰지_않는다(tmp_path):
    build_sources(tmp_path)
    # 기준일 다음날 대장이 갱신돼 가격이 바뀐 상황. 과거 날짜로 다시 돌렸을 때
    # 이 파티션을 읽으면 그때의 결과를 재현할 수 없습니다.
    future = [{**row, "weekly_lease_fee": 999.0} for row in CATALOG]
    write_source(
        tmp_path, "vehicle_catalog", "2026-08-14", "vendor", VENDOR,
        future, CATALOG_SCHEMA,
    )

    result = run(tmp_path)

    assert result["source_collected_dates"]["vehicle_catalog"] == "2026-08-12"
    assert all(row["weekly_lease_fee"] != 999.0 for row in read_rows(result))


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
    assert fit["weekly_lease_fee"] == 514.0
    assert fit["image_url"] == "https://example.com/fit.png"
    assert fit["platform"] is None
    assert fit["product"] is None
    assert fit["min_year"] is None


def test_구동방식만_다른_트림은_같은_차로_붙인다(tmp_path):
    build_sources(tmp_path)

    row = find(read_rows(run(tmp_path)), "OUTLANDER SPORT", "uber", "UberX")

    # 제원의 base_model_key 는 "OUTLANDER" 입니다. 그걸로 붙이면 못 찾습니다.
    assert row["spec_match_level"] == "DRIVETRAIN"
    assert row["spec_trim_count"] == 2  # SPORT 2WD / SPORT 4WD
    assert (row["combined_mpg_min"], row["combined_mpg_max"]) == (25.0, 27.0)


def test_하이브리드_트림은_후보에서_뺀다(tmp_path):
    build_sources(tmp_path)

    row = find(read_rows(run(tmp_path)), "SPORTAGE", "uber", "UberX")

    # 대장은 트림 없이 "SPORTAGE" 입니다. 하이브리드(41mpg)를 후보에 넣으면
    # 내연기관 차의 에너지비를 40% 과소평가합니다.
    assert row["spec_trim_count"] == 2  # FWD 28 / AWD 25 만. HYBRID·X-PRO 제외
    assert (row["combined_mpg_min"], row["combined_mpg_max"]) == (25.0, 28.0)
    assert row["fuel_type"] == "GAS"


def test_이름이_더_긴_다른_차종이_후보에_섞이지_않는다(tmp_path):
    build_sources(tmp_path)

    outlander = find(read_rows(run(tmp_path)), "OUTLANDER", "uber", "UberX")

    # "OUTLANDER SPORT 2WD/4WD" 는 base_model_key 가 똑같이 "OUTLANDER" 라
    # 예전 폴백에서는 여기 섞여 들어왔습니다. 다른 차입니다.
    assert outlander["spec_trim_count"] == 1  # OUTLANDER 4WD 하나뿐
    assert outlander["combined_mpg_min"] == 26.0


def test_기준일에서_너무_멀거나_미출시인_연식은_후보에서_뺀다(tmp_path):
    build_sources(tmp_path)

    rows = read_rows(run(tmp_path))

    # 2028년식 OUTLANDER 4WD(99mpg) 와 2015년식 CAMRY(12mpg) 가 fixture 에 있습니다.
    outlander = find(rows, "OUTLANDER", "uber", "UberX")
    assert outlander["combined_mpg_max"] == 26.0
    camry = find(rows, "CAMRY", "uber", "UberX")
    assert camry["combined_mpg_max"] == 32.0
    assert camry["spec_year_min"] == 2024


def test_후보의_연료가_갈리면_MIXED_로_남긴다(tmp_path):
    # 같은 model_key 아래 내연과 하이브리드가 함께 있는 경우 (RAV4 가 그렇습니다).
    specs = SPECS + [spec("12", 2025, "TOYOTA", "CAMRY", "CAMRY", 51.0, "Hybrid")]
    build_sources(tmp_path, specs=specs)

    camry = find(read_rows(run(tmp_path)), "CAMRY", "uber", "UberX")

    # 어느 단가를 곱할지 Gold 가 정할 수 없다는 사실을 그대로 넘깁니다.
    assert camry["fuel_type"] == "MIXED"
    assert (camry["combined_mpg_min"], camry["combined_mpg_max"]) == (30.0, 51.0)


def test_제원을_못_찾으면_연료_구분까지_비운다(tmp_path):
    build_sources(tmp_path)

    fit = find(read_rows(run(tmp_path)), "FIT")

    assert fit["spec_match_level"] == "NONE"
    assert fit["spec_trim_count"] == 0
    assert fit["combined_mpg_min"] is None
    # GAS 로 채우면 Gold 가 없는 연비로 에너지비를 계산하려 듭니다.
    assert fit["fuel_type"] is None


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
