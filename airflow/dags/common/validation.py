from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class HandlerResult:
    row_count: int
    locations: tuple[Path, ...]


def parse_handler_result(
    result: object,
    *,
    expected_locations: int | None = None,
    expected_rows: int | None = None,
) -> HandlerResult:
    if not isinstance(result, dict):
        raise TypeError("Handler 결과가 dict가 아닙니다.")

    row_count = result.get("row_count")
    # bool은 int의 하위 타입이므로 명시적으로 제외합니다.
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise ValueError("row_count는 1 이상의 정수여야 합니다.")
    if expected_rows is not None and row_count != expected_rows:
        raise ValueError(f"row_count는 {expected_rows}이어야 합니다.")

    raw_locations = result.get("locations")
    if not isinstance(raw_locations, list) or not raw_locations:
        raise ValueError("locations는 비어 있지 않은 경로 목록이어야 합니다.")
    if not all(isinstance(value, str) and value for value in raw_locations):
        raise ValueError("locations에는 빈 경로가 없어야 합니다.")
    if expected_locations is not None and len(raw_locations) != expected_locations:
        raise ValueError(f"locations에는 경로가 {expected_locations}개 있어야 합니다.")

    return HandlerResult(row_count, tuple(map(Path, raw_locations)))


def parse_iso_date(value: object, field: str = "collected_date") -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field}는 문자열이어야 합니다.")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field}는 YYYY-MM-DD 형식이어야 합니다.") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field}는 YYYY-MM-DD 형식이어야 합니다.")
    return parsed


def parse_year_month(value: object, field: str = "collected_month") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field}는 문자열이어야 합니다.")
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ValueError(f"{field}는 YYYY-MM 형식이어야 합니다.") from exc
    if parsed.strftime("%Y-%m") != value:
        raise ValueError(f"{field}는 YYYY-MM 형식이어야 합니다.")
    return value


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise FileNotFoundError(f"적재 파일이 없습니다: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"적재 파일이 비어 있습니다: {path}")
    return path


def read_parquet(path: Path) -> pa.Table:
    require_file(path)
    if path.suffix != ".parquet":
        raise ValueError(f"Parquet 파일이 아닙니다: {path}")
    try:
        return pq.ParquetFile(path).read()
    except (OSError, pa.ArrowInvalid) as exc:
        raise RuntimeError(f"Parquet 파일을 읽지 못했습니다: {path}") from exc
