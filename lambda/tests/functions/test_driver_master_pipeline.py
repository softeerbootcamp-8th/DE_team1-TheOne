"""driver_master Raw -> Bronze 월별 신규·탈퇴 배치 시나리오.

1. 최초 실행(전월 파티션 없음) → 시드 CSV 기준 스냅샷 생성, list/date/숫자 필드 타입이 정상 파싱됨
2. 전월 파티션이 있으면 시드 대신 그 parquet 을 기준으로 삼음 (handler 2회 연속 실행 체인)
3. 신규/탈퇴 인원 수가 각각 [0,40]/[0,30] 범위를 벗어나지 않음 (활성 인원이 상한보다 적은 경우 포함)
4. 이미 탈퇴한 기사는 이번 달 탈퇴 후보에서 제외되고 원래 churned_at 값이 보존됨
5. 이번 달 탈퇴/신규 날짜가 해당 연월 범위 안에 있음
6. 시드도 전월 파티션도 없으면 FileNotFoundError 로 명시 실패
7. Loader 가 스키마대로 parquet 을 쓰고 year_month= 파티션 경로가 규칙과 일치함 (read-back 값 검증 포함)
8. handler: year/month 누락 시 수집 전에 ValueError
9. 다른 달 파티션은 있는데 바로 전월만 없으면(건너뛴 달) 시드로 리셋되지 않고 명시적으로 실패

커버하지 않음: 상류 변화(외부 원본이 없어 해당 없음), 실패의 격리(행 단위 부분 실패가
없는 순수 생성 로직이라 해당 없음), 이중 파라미터 계약 위반(상충 가능한 파라미터 없음).
"""

import calendar
import random
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from functions.driver_master_raw_to_bronze.extractor import DriverMasterExtractor
from functions.driver_master_raw_to_bronze.handler import lambda_handler
from functions.driver_master_raw_to_bronze.loader import DriverMasterBronzeLoader

SEED_CSV_HEADER = (
    "driver_id,driver_name,primary_distance_bands,primary_time_blocks,active_weekdays,"
    "max_idle_seconds,min_idle_seconds,max_trip_count,min_trip_count,"
    "min_work_minutes,max_work_minutes,max_rest_minutes,min_rest_minutes,churned_at,joined_at"
)


def _seed_row(
    driver_id: str,
    churned_at: str = "",
    joined_at: str = "2025-01-01 00:00:00",
) -> str:
    return (
        f"{driver_id},Test Driver,SHORT,06-09,MON|TUE,"
        f"9000.0,3000.0,10,2,300.0,600.0,60.0,20.0,{churned_at},{joined_at}"
    )


def _write_seed_csv(path: Path, rows: list[str]) -> Path:
    path.write_text(SEED_CSV_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def _month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    days_in_month = calendar.monthrange(year, month)[1]
    return datetime(year, month, 1), datetime(year, month, days_in_month)


def test_최초_실행은_시드_CSV의_필드_타입을_정상_파싱한다(tmp_path):
    """이미 탈퇴한 행이라 이번 달 탈퇴/신규 로직이 건드리지 않아 파싱 결과만 순수하게 본다."""
    seed_path = _write_seed_csv(
        tmp_path / "driver_master.csv",
        [_seed_row("already-churned", churned_at="2025-06-01 00:00:00", joined_at="2025-01-15 00:00:00")],
    )

    rows = DriverMasterExtractor(
        2026, 2, str(tmp_path / "bronze"), seed_path=str(seed_path), rng=random.Random(0)
    ).extract()

    row = next(r for r in rows if r["driver_id"] == "already-churned")
    assert row["churned_at"] == datetime(2025, 6, 1)
    assert row["joined_at"] == datetime(2025, 1, 15)
    assert row["max_idle_seconds"] == 9000.0
    assert row["max_trip_count"] == 10
    assert isinstance(row["max_trip_count"], int)
    assert row["primary_distance_bands"] == "SHORT"
    assert row["active_weekdays"] == "MON|TUE"


def test_전월_파티션이_있으면_시드_대신_그것을_기준으로_삼는다(tmp_path):
    base_dir = tmp_path / "bronze"
    seed_path = _write_seed_csv(
        tmp_path / "driver_master.csv",
        [_seed_row(f"seed-{i}") for i in range(5)],
    )
    missing_seed_path = tmp_path / "does_not_exist.csv"

    first = lambda_handler(
        {"year": "2026", "month": "1", "base_dir": str(base_dir), "seed_path": str(seed_path)}
    )
    # 두 번째 달은 존재하지 않는 seed_path 를 줘서, 시드로 폴백했다면 바로 FileNotFoundError 로 드러난다.
    second = lambda_handler(
        {"year": "2026", "month": "2", "base_dir": str(base_dir), "seed_path": str(missing_seed_path)}
    )

    assert second["row_count"] >= first["row_count"] - 30
    assert second["row_count"] <= first["row_count"] + 40


def test_활성_인원이_상한보다_적어도_탈퇴_후보_추출이_범위를_넘지_않는다(tmp_path):
    seed_path = _write_seed_csv(
        tmp_path / "driver_master.csv",
        [_seed_row(f"active-{i}") for i in range(3)],
    )

    for seed in range(30):
        rows = DriverMasterExtractor(
            2026, 3, str(tmp_path / "bronze"), seed_path=str(seed_path), rng=random.Random(seed)
        ).extract()

        n_churned = sum(1 for r in rows[:3] if r["churned_at"] is not None)
        n_joined = len(rows) - 3

        assert 0 <= n_churned <= 3
        assert 0 <= n_joined <= 40


def test_이미_탈퇴한_기사는_탈퇴_후보에서_제외되고_기록이_보존된다(tmp_path):
    seed_path = _write_seed_csv(
        tmp_path / "driver_master.csv",
        [
            _seed_row("active-1"),
            _seed_row("active-2"),
            _seed_row("active-3"),
            _seed_row("churned-1", churned_at="2024-01-10 00:00:00"),
            _seed_row("churned-2", churned_at="2024-02-20 00:00:00"),
        ],
    )

    for seed in range(15):
        rows = DriverMasterExtractor(
            2026, 4, str(tmp_path / "bronze"), seed_path=str(seed_path), rng=random.Random(seed)
        ).extract()
        by_id = {r["driver_id"]: r for r in rows}

        assert by_id["churned-1"]["churned_at"] == datetime(2024, 1, 10)
        assert by_id["churned-2"]["churned_at"] == datetime(2024, 2, 20)


def test_이번_달_탈퇴_신규_날짜가_대상_연월_범위_안에_있다(tmp_path):
    seed_path = _write_seed_csv(
        tmp_path / "driver_master.csv",
        [_seed_row(f"active-{i}") for i in range(10)],
    )
    year, month = 2026, 1  # 31일짜리 달로 경계값(31일)까지 확인
    month_start, month_end = _month_bounds(year, month)

    for seed in range(15):
        rows = DriverMasterExtractor(
            year, month, str(tmp_path / "bronze"), seed_path=str(seed_path), rng=random.Random(seed)
        ).extract()

        newly_churned = [r for r in rows[:10] if r["churned_at"] is not None]
        newly_joined = rows[10:]

        for row in newly_churned:
            assert month_start <= row["churned_at"] <= month_end
        for row in newly_joined:
            assert month_start <= row["joined_at"] <= month_end


def test_시드도_전월_파티션도_없으면_명시적으로_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        DriverMasterExtractor(
            2026, 5, str(tmp_path / "bronze"), seed_path=str(tmp_path / "missing.csv")
        ).extract()


def test_다른_달_파티션은_있는데_바로_전월만_없으면_시드로_리셋되지_않고_실패한다(tmp_path):
    base_dir = tmp_path / "bronze"
    seed_path = _write_seed_csv(tmp_path / "driver_master.csv", [_seed_row("seed-1")])

    # 2024-01 만 만들어 두고 2024-02 는 건너뛴 채 2024-03 을 돌리는 상황을 재현.
    lambda_handler(
        {"year": "2024", "month": "1", "base_dir": str(base_dir), "seed_path": str(seed_path)}
    )

    with pytest.raises(Exception, match="전월"):
        DriverMasterExtractor(2024, 3, str(base_dir), seed_path=str(seed_path)).extract()


def test_스키마대로_parquet을_쓰고_파티션_경로가_규칙과_일치한다(tmp_path):
    collected_at = datetime(2026, 8, 10, 0, 0)
    data = [
        {
            "driver_id": "d1",
            "driver_name": "Alice",
            "primary_distance_bands": "SHORT|MEDIUM",
            "primary_time_blocks": "06-09",
            "active_weekdays": "MON|TUE|WED",
            "max_idle_seconds": 9000.0,
            "min_idle_seconds": 3000.0,
            "max_trip_count": 10,
            "min_trip_count": 2,
            "min_work_minutes": 300.0,
            "max_work_minutes": 600.0,
            "max_rest_minutes": 60.0,
            "min_rest_minutes": 20.0,
            "churned_at": None,
            "joined_at": datetime(2026, 8, 1),
        }
    ]

    result = DriverMasterBronzeLoader(str(tmp_path), "2026-08", collected_at).write(data)

    path = Path(result.location)
    assert path.parent.name == "year_month=2026-08"
    assert path.parent.parent.name == "driver_master"

    written = pq.ParquetFile(path).read().to_pylist()
    assert written[0]["driver_id"] == "d1"
    assert written[0]["primary_distance_bands"] == "SHORT|MEDIUM"
    assert written[0]["churned_at"] is None
    assert written[0]["joined_at"] == datetime(2026, 8, 1)


@pytest.mark.parametrize(
    "event",
    [
        {"month": "02"},
        {"year": "2026"},
        {},
    ],
)
def test_연월이_없으면_수집_전에_실패한다(event, monkeypatch):
    monkeypatch.delenv("YEAR", raising=False)
    monkeypatch.delenv("MONTH", raising=False)

    with pytest.raises(ValueError, match="year와 month"):
        lambda_handler(event)
