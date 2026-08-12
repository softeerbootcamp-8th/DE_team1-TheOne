from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common.validation import (
    parse_handler_result,
    parse_iso_date,
    parse_year_month,
    read_parquet,
    require_file,
)


def test_handler_result를_경로와_행수로_변환한다():
    parsed = parse_handler_result(
        {"row_count": 2, "locations": ["a", "b"]}, expected_locations=2
    )
    assert parsed.row_count == 2
    assert parsed.locations == (Path("a"), Path("b"))


@pytest.mark.parametrize("row_count", [True, 0, -1, "1", None])
def test_row_count가_양의_정수가_아니면_실패한다(row_count):
    with pytest.raises(ValueError, match="row_count"):
        parse_handler_result({"row_count": row_count, "locations": ["a"]})


@pytest.mark.parametrize("locations", [None, [], [""], [Path("a")]])
def test_locations가_문자열_경로_목록이_아니면_실패한다(locations):
    with pytest.raises(ValueError, match="locations"):
        parse_handler_result({"row_count": 1, "locations": locations})


def test_날짜와_월을_엄격한_형식으로_파싱한다():
    assert parse_iso_date("2026-08-12").isoformat() == "2026-08-12"
    assert parse_year_month("2026-08") == "2026-08"
    with pytest.raises(ValueError):
        parse_iso_date("2026-8-12")
    with pytest.raises(ValueError):
        parse_year_month("2026-8")


def test_파일_존재와_비어있지_않음을_확인한다(tmp_path):
    path = tmp_path / "data"
    with pytest.raises(FileNotFoundError):
        require_file(path)
    path.touch()
    with pytest.raises(ValueError, match="비어"):
        require_file(path)


def test_parquet을_읽고_손상된_파일은_명시적으로_실패한다(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    assert read_parquet(path).to_pylist() == [{"id": 1}]
    path.write_text("broken")
    with pytest.raises(RuntimeError, match="Parquet"):
        read_parquet(path)
