"""차종별 제원 Raw -> Bronze 배선 검증 (네트워크 없이 parse/Loader만 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sub.lambda_runtime.functions.fueleconomy_vehicle_specs_raw_to_bronze.extractor import parse
from sub.lambda_runtime.functions.fueleconomy_vehicle_specs_raw_to_bronze.loader import VehicleSpecsBronzeLoader

COLLECTED_AT = datetime(2027, 1, 1, 4, 0, tzinfo=timezone.utc)

# 원본 CSV 의 축소판. 필수 컬럼 + 안 쓰는 컬럼 하나를 섞어 원본 보존을 확인합니다.
CSV = (
    "id,year,make,model,baseModel,comb08,combE,range,atvType,cylinders\n"
    "1,2026,Toyota,RAV4 AWD,RAV4,30,0,0,,4\n"
    "2,2026,Tesla,Model 3,Model 3,132,25,272,EV,\n"
)


def test_원본_컬럼을_버리지_않고_그대로_싣는다(tmp_path):
    rows = parse(CSV, COLLECTED_AT)
    location = VehicleSpecsBronzeLoader(str(tmp_path), COLLECTED_AT).write(rows).location

    table = pq.ParquetFile(location).read()
    # source 는 파티션 키라 파일 안에 없고, 나머지 원본 컬럼 + collected_at 이 남습니다.
    assert set(table.column_names) == {
        "id", "year", "make", "model", "baseModel", "comb08", "combE",
        "range", "atvType", "cylinders", "collected_at",
    }
    assert table.num_rows == 2

    written = table.to_pylist()
    # 값은 전부 문자열, 빈 칸은 None (타입 변환은 Silver 단계에서).
    assert written[0]["comb08"] == "30"
    assert written[0]["atvType"] is None


def test_수집일과_출처로_파티션을_나눈다(tmp_path):
    rows = parse(CSV, COLLECTED_AT)
    location = VehicleSpecsBronzeLoader(str(tmp_path), COLLECTED_AT).write(rows).location

    path = Path(location)
    assert path.parent.name == "source=fueleconomy.gov"
    assert path.parent.parent.name == "collected_date=2027-01-01"


def test_같은_해에_다시_돌려도_덮어쓰지_않는다(tmp_path):
    """매 실행이 전량 스냅샷이라 이전 것을 지우면 안 됩니다."""
    rows = parse(CSV, COLLECTED_AT)
    first = VehicleSpecsBronzeLoader(str(tmp_path), COLLECTED_AT).write(rows).location

    later = COLLECTED_AT.replace(hour=6)
    second = VehicleSpecsBronzeLoader(str(tmp_path), later).write(rows).location

    assert first != second
    assert len(list(Path(first).parent.glob("*.parquet"))) == 2


def test_필수_컬럼이_사라지면_실패한다():
    """원본 스키마가 바뀌면 조용히 넘어가지 않아야 합니다."""
    broken = "id,year,make,model\n1,2026,Toyota,RAV4\n"

    with pytest.raises(RuntimeError, match="필수 컬럼이 없습니다"):
        parse(broken, COLLECTED_AT)


def test_결과가_비면_실패한다():
    header_only = "id,year,make,model,baseModel,comb08,combE,range\n"

    with pytest.raises(RuntimeError, match="0건"):
        parse(header_only, COLLECTED_AT)
