"""월별 원천 API에서 받은 단일 Bronze 파일을 검증합니다."""

from pathlib import Path

import pyarrow.parquet as pq

from shared.airflow.common.validation import parse_handler_result, parse_year_month


def validate_synthetic_bronze(
    result: dict,
    *,
    dataset_dir: str,
    base_dir: str | Path | None = None,
) -> tuple[Path, str]:
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    path = parsed.locations[0]
    if not path.is_file():
        raise ValueError(f"Bronze 원본 파일이 없습니다: {path}")
    if (
        path.parent.name != f"year_month={year_month}"
        or path.parent.parent.name != dataset_dir
        or path.name != "data.parquet"
    ):
        raise ValueError(f"Bronze 원본 경로가 월 파티션 계약과 다릅니다: {path}")
    if base_dir is not None:
        expected_partition = Path(base_dir) / dataset_dir / f"year_month={year_month}"
        if path.parent.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {path.parent}"
            )
    if path.stat().st_size != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if pq.ParquetFile(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")
    return path, year_month
