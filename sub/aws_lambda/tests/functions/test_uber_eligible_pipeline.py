"""Uber Eligible Vehicles Raw -> Bronze 배선 검증 (네트워크 없이 fetch 만 대체)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from sub.aws_lambda.functions.uber_eligible_vehicles_raw_to_bronze import extractor
from sub.aws_lambda.functions.uber_eligible_vehicles_raw_to_bronze.loader import UberEligibleVehiclesBronzeLoader

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
CITY_SLUG = "new-york"

# {제조사: {모델: 원문}} — 한 모델이 연식별로 여러 묶음을 갖는 실제 형태
PAYLOAD = {
    "Acura": {"ZDX": "2010 (UberX, Comfort) / 2018 (Comfort Electric)"},
    "Kia": {"NIRO": "2019 (UberX)"},
}


def test_extractor가_연식_묶음을_행으로_펼친다(monkeypatch):
    monkeypatch.setattr(extractor, "fetch", lambda city_slug, timeout: PAYLOAD)

    rows = extractor.UberEligibleVehiclesExtractor(CITY_SLUG, COLLECTED_AT).extract()

    assert [(row["model"], row["min_year"], row["products"]) for row in rows] == [
        ("ZDX", 2010, ["UberX", "Comfort"]),
        ("ZDX", 2018, ["Comfort Electric"]),
        ("NIRO", 2019, ["UberX"]),
    ]


def test_loader가_수집일_도시_파티션에_쓴다(monkeypatch, tmp_path):
    monkeypatch.setattr(extractor, "fetch", lambda city_slug, timeout: PAYLOAD)
    rows = extractor.UberEligibleVehiclesExtractor(CITY_SLUG, COLLECTED_AT).extract()

    loader = UberEligibleVehiclesBronzeLoader(str(tmp_path), CITY_SLUG, COLLECTED_AT)
    result = loader.write(rows)

    assert Path(result.location).parent == (
        tmp_path / "uber_eligible_vehicles" / "collected_date=2026-08-10" / "city=new-york"
    )
    assert result.row_count == len(rows)

    # 스키마대로 읽히는지까지 봐야 파티션만 맞고 내용이 깨진 경우를 잡습니다.
    written = pq.ParquetFile(result.location).read().to_pylist()
    assert {row["make"] for row in written} == {"Acura", "Kia"}
    assert written[0]["products"] == ["UberX", "Comfort"]
