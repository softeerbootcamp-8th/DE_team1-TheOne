"""EIA 휘발유 요금 Bronze→Silver 변환 시나리오. 이슈 #517.

1. 주간 이력을 오름차순으로 읽음 — 원본이 최신순이라 정렬을 빠뜨리면 아래 "그 날 이하
   가장 최근" 규칙이 엉뚱한 값을 집음
2. 각 날짜에 그 날 이하 **가장 최근** 관측치를 복제. 선형 보간하지 않음
3. 그 달 **전 일수**로 펼침. 하루라도 비면 하류 일자 조인에서 그 날이 통째로 빠지는데,
   실패가 아니라 조용히 줄어든 집계로 나타남
4. 대상 월 첫날 이전 관측치가 없으면 실패
5. 단가가 허용 범위 밖이면 실패
6. Loader 가 CLEAN 스키마 그대로, 대상 월 파티션에 씀
7. 대상 월을 덮는 관측이 낡았으면 실패 — forward-fill 이 한 달을 같은 값 하나로
   채우고 일수·범위·중복 검사를 모두 통과해 버림 (#544)
8. 그 달 마지막 주 관측이 있으면 통과. 월말 이후 관측은 기준이 아니라 과거 달
   백필도 막지 않음
9. Bronze 읽기는 쓰기와 같은 service_area 여야 찾음 (#843)
10. Silver 쓰기 경로에 service_area 가 반영되고 지역끼리 서로 겹치지 않음 (#843)
"""

from datetime import date
from io import BytesIO

import pyarrow.parquet as pq
import pytest
import xlrd  # noqa: F401  (구형 xls 파서 존재 확인 — 파싱 대상이 BIFF 라 필수)
import xlwt

from main.aws_lambda.functions.eia_gas_price_bronze_to_silver.loader import (
    EiaGasPriceSilverLoader,
    staged_silver_file,
)
from main.aws_lambda.functions.eia_gas_price_bronze_to_silver.transformer import (
    build_daily_prices,
    gas_price_for,
    parse_gas_weekly,
)
from schema.silver import CLEAN_GAS_PRICE_SCHEMA as SCHEMA

COLLECTED = date(2026, 8, 17)
WEEKLY = [
    (date(2025, 4, 28), 3.0),
    (date(2025, 5, 5), 3.1),
    (date(2025, 5, 26), 3.2),
]


def _xls(observations: list[tuple[date, float]]) -> bytes:
    """EIA 휘발유 파일 모양 — 0번 목차 시트, 1번 주간 계열, 헤더 3행."""
    book = xlwt.Workbook()
    book.add_sheet("Contents")
    sheet = book.add_sheet("Data 1")
    for row in range(3):
        sheet.write(row, 0, "header")
    style = xlwt.XFStyle()
    style.num_format_str = "M/D/YYYY"
    for index, (observed, price) in enumerate(observations, start=3):
        sheet.write(index, 0, observed, style)
        sheet.write(index, 1, price)
    buffer = BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def test_주간_이력을_오름차순으로_읽는다():
    assert parse_gas_weekly(_xls(list(reversed(WEEKLY)))) == WEEKLY


def test_각_날짜에_그날_이하_가장_최근_관측치를_복제한다():
    days = [date(2025, 5, 4), date(2025, 5, 5), date(2025, 5, 25), date(2025, 5, 26)]

    prices = gas_price_for(days, WEEKLY)

    # 5-04 는 아직 4-28 관측치, 5-05 부터 3.1, 5-26 부터 3.2
    assert [prices[day] for day in days] == [3.0, 3.1, 3.1, 3.2]


def test_월_전체를_일별로_펼친다():
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    assert len(rows) == 31
    assert [row["date"] for row in rows] == [date(2025, 5, d) for d in range(1, 32)]
    # 5-01~04 는 4-28 관측치가 이어집니다.
    assert rows[0]["gas_price"] == 3.0
    assert rows[-1]["gas_price"] == 3.2


def test_어느_수집분으로_만들었는지_모든_행에_남는다():
    """같은 달을 다시 만들면 숫자가 달라질 수 있습니다 — EIA 가 과거 값을 개정하기
    때문입니다. 수집분을 남겨두지 않으면 그 차이를 설명할 수 없습니다 (#518)."""
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    assert {row["bronze_collected_date"] for row in rows} == {COLLECTED}


def test_대상월_이전_관측치가_없으면_실패한다():
    with pytest.raises(ValueError, match="이전의 휘발유 관측치가 없습니다"):
        build_daily_prices("2025-03", _xls(WEEKLY), COLLECTED)


def test_단가가_허용범위_밖이면_실패한다():
    with pytest.raises(ValueError, match="허용 범위"):
        build_daily_prices(
            "2025-05", _xls([(date(2025, 4, 28), 99.0), (date(2025, 5, 26), 99.0)]), COLLECTED
        )


def test_주간_계열_시트가_없으면_실패한다():
    book = xlwt.Workbook()
    book.add_sheet("Contents")
    buffer = BytesIO()
    book.save(buffer)

    with pytest.raises(ValueError, match="주간 계열 시트"):
        parse_gas_weekly(buffer.getvalue())


def test_적재는_CLEAN_스키마로_대상월_파티션에_쓴다(tmp_path):
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    result = EiaGasPriceSilverLoader(str(tmp_path), "2025-05").write(rows)

    assert result.location == str(staged_silver_file(str(tmp_path), "2025-05"))
    assert result.row_count == 31
    table = pq.ParquetFile(result.location).read()
    assert table.schema.names == SCHEMA.names


def test_빈_결과는_적재를_거부한다(tmp_path):
    with pytest.raises(ValueError, match="적재할"):
        EiaGasPriceSilverLoader(str(tmp_path), "2025-05").write([])


# --- 원본 신선도 (#544) -------------------------------------------------------
# 수집이 실패해 직전 수집분이 그대로 쓰이면, forward-fill 이 한 달을 같은 값 하나로
# 채우고 기존 검사(일수·범위·중복)를 전부 통과합니다.

def test_대상월을_덮는_관측이_낡았으면_실패한다():
    """8월에서 끊긴 원본으로 9월을 만들려는 상황 — 30일이 전부 8-24 값이 됩니다."""
    stale = [(date(2026, 8, d), 3.1) for d in (3, 10, 17, 24)]

    with pytest.raises(ValueError, match="휘발유 관측이 낡았습니다"):
        build_daily_prices("2026-09", _xls(stale), COLLECTED)


def test_낡은_원본_실패는_다시_돌릴_DAG_을_알려준다():
    stale = [(date(2026, 8, d), 3.1) for d in (3, 10, 17, 24)]

    with pytest.raises(ValueError, match="eia_gas_price_raw_to_bronze_pipeline"):
        build_daily_prices("2026-09", _xls(stale), COLLECTED)


def test_그_달_마지막_주_관측이_있으면_통과한다():
    """정상 수집 — 월말과 6일 차이. 가드가 정상 경로를 막으면 안 됩니다."""
    fresh = [(date(2026, 7, 27), 3.0)] + [(date(2026, 8, d), 3.1) for d in (3, 10, 17, 25)]

    rows = build_daily_prices("2026-08", _xls(fresh), COLLECTED)

    assert len(rows) == 31


def test_과거_달_백필은_막지_않는다():
    """월말 **이후** 관측은 기준이 아닙니다 — 최신 파일로 옛날 달을 만드는 게 정상 경로."""
    weekly = WEEKLY + [(date(2026, 8, 17), 3.5)]

    rows = build_daily_prices("2025-05", _xls(weekly), COLLECTED)

    assert len(rows) == 31


@pytest.mark.parametrize(
    ("last_observed", "fails"),
    [(date(2026, 8, 18), False), (date(2026, 8, 17), True)],
)
def test_허용_간격_경계(last_observed, fails):
    """월말(8-31) 기준 13일까지 허용, 14일부터 실패."""
    weekly = [(date(2026, 7, 27), 3.0), (last_observed, 3.1)]

    if fails:
        with pytest.raises(ValueError, match="휘발유 관측이 낡았습니다"):
            build_daily_prices("2026-08", _xls(weekly), COLLECTED)
    else:
        assert len(build_daily_prices("2026-08", _xls(weekly), COLLECTED)) == 31


# --- S3 배포 (#557) — Bronze 파티션 선택 규칙 -------------------------------

from main.aws_lambda.common import eia_fuel_price_layout as layout  # noqa: E402


def test_S3_키_목록에서도_가장_최신_수집분을_고른다():
    """electricity(#558)에서 검증한 dataset 파라미터화 규칙이 gas 에도 그대로 성립하는지 고정합니다."""
    keys = [
        layout.gas_bronze_key(date(2025, 5, 10)),
        layout.gas_bronze_key(COLLECTED),
    ]

    collected_date, key = layout.newest_bronze_s3_key(
        keys, layout.GAS_DATASET, layout.GAS_FILE_NAME
    )

    assert collected_date == COLLECTED
    assert key == layout.gas_bronze_key(COLLECTED)


def test_S3_키_목록이_비면_실패한다():
    with pytest.raises(FileNotFoundError, match="EIA Bronze S3 파티션이 없습니다"):
        layout.newest_bronze_s3_key([], layout.GAS_DATASET, layout.GAS_FILE_NAME)


# --- 지역(service_area) 격리 (#843) -----------------------------------------

from main.aws_lambda.functions.eia_gas_price_bronze_to_silver.extractor import (  # noqa: E402
    EiaGasPriceBronzeExtractor,
)


def test_bronze_읽기는_쓰기와_같은_지역이어야_찾는다(tmp_path):
    path = layout.gas_bronze_file(str(tmp_path), COLLECTED, "TX")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_xls(WEEKLY))

    result = EiaGasPriceBronzeExtractor(str(tmp_path), "2025-05", "TX").extract()

    assert result["bronze_collected_date"] == COLLECTED
    with pytest.raises(FileNotFoundError):
        EiaGasPriceBronzeExtractor(str(tmp_path), "2025-05", "NYC").extract()


def test_silver_쓰기_경로에_지역이_반영되고_서로_겹치지_않는다(tmp_path):
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    nyc = EiaGasPriceSilverLoader(str(tmp_path), "2025-05", "NYC").write(rows)
    tx = EiaGasPriceSilverLoader(str(tmp_path), "2025-05", "TX").write(rows)

    assert nyc.location != tx.location
    assert "service_area=NYC" in nyc.location
    assert "service_area=TX" in tx.location
