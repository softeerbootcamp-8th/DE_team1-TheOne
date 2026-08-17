"""EIA 연료비 DAG 의 대상 월 해석과 산출물 검증 시나리오. 이슈 #445.

1. 지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달을 채움
2. 연초 경계에서 연도가 함께 내려감
3. `year_month` 파라미터가 있으면 그 값을 그대로 씀
4. 형식이 잘못된 `year_month` 는 거부
5. 정상 산출물은 검증 통과
6. 행 수가 그 달 일수와 다르면 실패 (하루라도 비면 Gold 조인이 조용히 줄어듦)
7. 스키마가 통합 스키마와 다르면 실패
8. `price_source` 가 EIA 가 아니면 실패 (크롤링 산출물을 잘못 검증하는 것 방지)

Lambda 핸들러는 부르지 않습니다 — 파일을 직접 놓고 검증 함수만 확인합니다.
"""

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ★ import 순서가 중요합니다. `scripts` 패키지가 두 곳(airflow/scripts, 저장소
#   루트 scripts)에 있는데, `common.project_paths` 가 저장소 루트를 sys.path 앞에
#   꽂아 airflow 쪽을 가립니다. tasks 를 먼저 부르면 그 안에서 경로 설정이 끝나
#   양쪽이 다 잡힙니다 — 런타임(DAG 파싱)도 같은 순서입니다.
from scripts.eia_fuel_price_raw_to_silver import tasks
from schema.silver.gas_ev_price import CRAWLED, EIA, SCHEMA


def _write_silver(silver_dir, year_month: str, rows: int, source: str = EIA, schema=SCHEMA):
    path = tasks.integrated_silver_file(str(silver_dir), year_month)
    path.parent.mkdir(parents=True, exist_ok=True)
    year, month = (int(part) for part in year_month.split("-"))
    records = [
        {
            "date": __import__("datetime").date(year, month, day),
            "gas_price": 3.0,
            "ev_price": 0.4,
            "price_source": source,
        }
        for day in range(1, rows + 1)
    ]
    if schema is not SCHEMA:
        records = [{key: value for key, value in record.items() if key in schema.names}
                   for record in records]
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return path


def test_지정이_없으면_전력_공개지연만큼_물러선다():
    # 2026-08 에 돌면 전력 통계는 2026-05 까지만 나와 있습니다.
    assert tasks.default_year_month(datetime(2026, 8, 17, tzinfo=timezone.utc)) == "2026-05"


def test_연초_경계에서_연도가_함께_내려간다():
    assert tasks.default_year_month(datetime(2026, 2, 5, tzinfo=timezone.utc)) == "2025-11"
    assert tasks.default_year_month(datetime(2026, 1, 5, tzinfo=timezone.utc)) == "2025-10"


def test_파라미터가_있으면_그_값을_쓴다():
    context = {"params": {"year_month": " 2025-05 "}}

    assert tasks.resolve_year_month(context) == "2025-05"


@pytest.mark.parametrize("value", ["2025-13", "2025/05", "202505"])
def test_형식이_잘못된_year_month는_거부한다(value):
    with pytest.raises(ValueError):
        tasks.resolve_year_month({"params": {"year_month": value}})


def test_정상_산출물은_검증을_통과한다(tmp_path):
    _write_silver(tmp_path, "2025-05", rows=31)

    tasks.validate_silver(str(tmp_path), "2025-05")


def test_행수가_그달_일수와_다르면_실패한다(tmp_path):
    # 30행이면 5월(31일)에 하루가 빕니다. 그 날 운행은 Gold 조인에서 통째로 빠지는데,
    # 실패가 아니라 **조용히 줄어든 집계**로 나타나므로 여기서 막습니다.
    _write_silver(tmp_path, "2025-05", rows=30)

    with pytest.raises(ValueError, match="31일이어야 하는데 30행"):
        tasks.validate_silver(str(tmp_path), "2025-05")


def test_스키마가_다르면_실패한다(tmp_path):
    narrowed = pa.schema([field for field in SCHEMA if field.name != "price_source"])
    _write_silver(tmp_path, "2025-05", rows=31, schema=narrowed)

    with pytest.raises(ValueError, match="통합 Silver 스키마가 다릅니다"):
        tasks.validate_silver(str(tmp_path), "2025-05")


def test_크롤링_산출물을_EIA_검증에_넣으면_실패한다(tmp_path):
    _write_silver(tmp_path, "2025-05", rows=31, source=CRAWLED)

    with pytest.raises(ValueError, match="price_source 가 다릅니다"):
        tasks.validate_silver(str(tmp_path), "2025-05")


def test_산출물이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="통합 연료비 Silver 가 없습니다"):
        tasks.validate_silver(str(tmp_path), "2025-05")
