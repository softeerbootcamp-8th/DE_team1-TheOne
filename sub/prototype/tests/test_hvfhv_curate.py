from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from sub.prototype import hvfhv_curate as curate
from sub.prototype.curated import TRIP_COLUMNS, trip_part_files


def _row(i: int = 0, **overrides) -> dict:
    """자연키가 `i` 마다 달라지는 한 행. 의도적 중복 테스트는 같은 `i`(기본 0)를 씁니다."""
    row = {
        "hvfhs_license_num": "HV0003",
        "on_scene_datetime": datetime(2026, 1, 5, 8, 0),
        "pickup_datetime": datetime(2026, 1, 5, 8, 10) + timedelta(seconds=i),
        "dropoff_datetime": datetime(2026, 1, 5, 8, 30) + timedelta(seconds=i),
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_miles": 3.5,
        "trip_time": 900,
        "base_passenger_fare": 20.0,
        "tolls": 0.0,
        "sales_tax": 1.5,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "tips": 2.0,
        "driver_pay": 15.0,
    }
    row.update(overrides)
    return row


def _raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)[curate.RAW_COLUMNS]


def _write_parquet(tmp_path, rows: list[dict], name: str = "raw.parquet") -> str:
    path = tmp_path / name
    _raw(rows).to_parquet(path, index=False)
    return str(path)


def _zone_lookup_csv(tmp_path) -> str:
    path = tmp_path / "taxi_zone_lookup.csv"
    pd.DataFrame([
        {"LocationID": 1, "Borough": "Manhattan", "Zone": "Astoria", "service_zone": "Boro Zone"},
        {"LocationID": 2, "Borough": "Queens", "Zone": "LaGuardia", "service_zone": "Airports"},
    ]).to_csv(path, index=False)
    return str(path)


def test_유효하지_않은_행은_걸러지고_불합격비율이_임계치를_넘으면_실패한다(tmp_path):
    zone_lookup = _zone_lookup_csv(tmp_path)
    rows = [_row(i) for i in range(39)] + [_row(39, trip_miles=-1.0)]  # 1/40 = 2.5% < 임계치
    trips = curate.curate_month(
        _write_parquet(tmp_path, rows), zone_lookup_path=zone_lookup, error_threshold=0.05,
    )
    assert len(trips) == 39

    with pytest.raises(ValueError, match="불합격 비율"):
        curate.curate_month(
            _write_parquet(
                tmp_path, [_row(i) for i in range(9)] + [_row(9, trip_miles=-1.0)],
                name="raw2.parquet",
            ),
            zone_lookup_path=zone_lookup, error_threshold=0.05,
        )  # 1/10 = 10% > 5%


def test_등급판정_구역조인_결과가_계약_컬럼을_모두_채운다(tmp_path):
    trips = curate.curate_month(
        _write_parquet(tmp_path, [_row(0), _row(1, PULocationID=2, DOLocationID=1)]),
        zone_lookup_path=_zone_lookup_csv(tmp_path),
    )
    for column in TRIP_COLUMNS:
        assert column in trips.columns, column
    assert set(trips["platform_name"]) == {"Uber"}
    assert set(trips["estimated_service_tier"]) == {"Standard"}
    assert trips.loc[trips["PULocationID"] == 1, "pickup_borough"].iloc[0] == "Manhattan"
    assert trips.loc[trips["DOLocationID"] == 2, "dropoff_borough"].iloc[0] == "Queens"
    assert trips["trip_key"].is_unique


def test_동일_자연키_중복행은_순번으로_다른_trip_key를_받는다(tmp_path):
    # 중복 1쌍을 39개의 서로 다른 자연키 사이에 섞습니다 — 충돌 비율 1/41 ≈ 2.4% < 임계치.
    rows = [_row(i) for i in range(1, 40)] + [_row(0), _row(0)]
    trips = curate.curate_month(
        _write_parquet(tmp_path, rows), zone_lookup_path=_zone_lookup_csv(tmp_path),
    )
    assert len(trips) == 41
    assert trips["trip_key"].nunique() == 41


def test_자연키_충돌비율이_임계치를_넘으면_실패한다(tmp_path):
    # 20건 중 절반이 완전 중복 -> occurrence>1 인 행 10건 / 20건 = 50% >= 5%
    rows = [_row(0) for _ in range(10)] + [_row(1) for _ in range(10)]
    with pytest.raises(ValueError, match="자연키 충돌 비율"):
        curate.curate_month(
            _write_parquet(tmp_path, rows), zone_lookup_path=_zone_lookup_csv(tmp_path),
        )


def test_등급판정은_관측수와_할증배율_기준을_따른다(tmp_path):
    # 같은 (플랫폼, PU, DO) 그룹에 20건 이상 있어야 판정 대상. 19건 요금 10, 1건 30
    # (중앙값 10, 1.15배=11.5 초과) -> Comfort. Lyft 는 Extra Comfort.
    base = [_row(i, base_passenger_fare=10.0 + i * 0.001) for i in range(19)]
    uber_premium = _row(19, base_passenger_fare=30.0)
    lyft_group = [
        _row(20 + i, hvfhs_license_num="HV0005", PULocationID=1, DOLocationID=2,
             base_passenger_fare=10.0 + i * 0.001)
        for i in range(19)
    ]
    lyft_premium = _row(40, hvfhs_license_num="HV0005", base_passenger_fare=30.0)
    trips = curate.curate_month(
        _write_parquet(tmp_path, base + [uber_premium] + lyft_group + [lyft_premium]),
        zone_lookup_path=_zone_lookup_csv(tmp_path),
    )
    assert trips.loc[trips["base_passenger_fare"] == 30.0, "estimated_service_tier"].tolist() == [
        "Comfort", "Extra Comfort",
    ]
    assert (trips.loc[trips["base_passenger_fare"] != 30.0, "estimated_service_tier"] == "Standard").all()


def test_관측수가_기준_미만이면_할증이어도_Standard로_남는다(tmp_path):
    rows = [_row(i, base_passenger_fare=10.0) for i in range(5)] + [_row(5, base_passenger_fare=30.0)]
    trips = curate.curate_month(
        _write_parquet(tmp_path, rows), zone_lookup_path=_zone_lookup_csv(tmp_path),
    )
    assert (trips["estimated_service_tier"] == "Standard").all()


def test_curate_and_write는_part_파일로_나눠_쓰고_기존_읽기유틸과_호환된다(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    output_dir = tmp_path / "curated"
    partition_dir = raw_dir / "year_month=2026-01"
    partition_dir.mkdir(parents=True)
    rows = [
        _row(i, PULocationID=1 if i % 2 == 0 else 2, DOLocationID=2 if i % 2 == 0 else 1)
        for i in range(25)
    ]
    _raw(rows).to_parquet(partition_dir / "hvfhv.parquet", index=False)

    partition = curate.curate_and_write(
        "2026-01", raw_dir=raw_dir, zone_lookup_path=_zone_lookup_csv(tmp_path),
        output_dir=output_dir, rows_per_part=10,
    )

    assert partition == output_dir / "year_month=2026-01"
    written = sorted(partition.glob("*.parquet"))
    assert len(written) == 3  # 25행 / 10 = 3 part
    total_rows = sum(len(pd.read_parquet(f, columns=TRIP_COLUMNS)) for f in written)
    assert total_rows == 25

    monkeypatch.setattr("sub.prototype.paths.CURATED_TRIP_DIR", output_dir)
    assert len(trip_part_files("2026-01", part_limit=None)) == 3
    assert len(trip_part_files("2026-01", part_limit=1)) == 1


def test_원천이_없으면_수집_절차를_안내하며_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="collect_source_input|fetch_tlc_hvfhv"):
        curate.curate_and_write(
            "2026-01", raw_dir=tmp_path / "missing",
            zone_lookup_path=_zone_lookup_csv(tmp_path), output_dir=tmp_path / "curated",
        )
