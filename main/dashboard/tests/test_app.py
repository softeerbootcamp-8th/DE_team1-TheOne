"""데이터 로딩/변환 로직 점검. `uv run python test_app.py`

1. year_month 파티션 전체가 이어붙는가
2. 데이터셋이 없으면 빈 DataFrame 을 돌려주는가
3. 시간대 비율·상위 zone 이 순서대로, 소수점 둘째 자리로 반올림돼 차트/표 프레임에 들어가는가
"""

import tempfile
from pathlib import Path

import pandas as pd

from app import HOUR_BLOCKS, _read_partitions, hourly_ratio_frame, top_zone_frame


def write_partition(root: Path, dataset: str, year_month: str, rows: list[dict]) -> None:
    partition = root / dataset / f"year_month={year_month}"
    partition.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(partition / f"{dataset}.csv", index=False)


def test_두_달_파티션이_모두_이어붙는다():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_partition(root, "monthly_report", "2026-01", [{"year_month": "2026-01", "recommended_driver_count": 1}])
        write_partition(root, "monthly_report", "2026-02", [{"year_month": "2026-02", "recommended_driver_count": 2}])
        frame = _read_partitions(root, "monthly_report")
        assert sorted(frame["year_month"]) == ["2026-01", "2026-02"]


def test_없는_데이터셋은_빈_프레임():
    with tempfile.TemporaryDirectory() as tmp:
        frame = _read_partitions(Path(tmp), "driver_car_suggestion")
        assert frame.empty


def test_시간대_비율이_순서대로_들어간다():
    row = pd.Series({f"ratio_{block}": i for i, block in enumerate(HOUR_BLOCKS)})
    frame = hourly_ratio_frame(row)
    assert list(frame["운행 비중"]) == list(range(len(HOUR_BLOCKS)))
    assert list(frame.index) == HOUR_BLOCKS


def test_시간대_비율이_소수점_둘째_자리로_반올림된다():
    row = pd.Series({f"ratio_{block}": 0.0 for block in HOUR_BLOCKS})
    row["ratio_00_03"] = 0.123456
    frame = hourly_ratio_frame(row)
    assert frame["운행 비중"].iat[0] == 0.12


def test_상위_3개_zone이_순위대로_표에_담긴다():
    row = pd.Series({
        "top1_zone_id": 51, "top1_zone_ratio": 0.5,
        "top2_zone_id": 76, "top2_zone_ratio": 0.3,
        "top3_zone_id": 90, "top3_zone_ratio": 0.2,
    })
    frame = top_zone_frame(row)
    assert list(frame["zone_id"]) == [51, 76, 90]
    assert list(frame.index) == ["1위", "2위", "3위"]


def test_zone_비중이_소수점_둘째_자리로_반올림된다():
    row = pd.Series({
        "top1_zone_id": 51, "top1_zone_ratio": 0.409836,
        "top2_zone_id": 76, "top2_zone_ratio": 0.3,
        "top3_zone_id": 90, "top3_zone_ratio": 0.2,
    })
    frame = top_zone_frame(row)
    assert frame["비중"].iat[0] == 0.41


if __name__ == "__main__":
    for name, case in sorted(globals().items()):
        if name.startswith("test_"):
            case()
            print("ok", name)
