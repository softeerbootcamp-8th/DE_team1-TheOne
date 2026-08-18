"""월 파티션 Silver Parquet을 한 파일로 원자적으로 교체합니다."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from shared.aws_lambda.common.atomic_write import atomic_write


def write_month_partition(
    table: pa.Table,
    output_dir: str | Path,
    year_month: str,
    filename: str,
) -> Path:
    """``year_month=YYYY-MM/<filename>`` 을 임시 파일로 쓴 뒤 교체합니다.

    같은 달을 다시 돌려도 파일이 하나만 남고, 쓰는 도중 실패해도 직전 달치가
    반쯤 덮인 상태로 남지 않습니다.
    """
    target = Path(output_dir) / f"year_month={year_month}" / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(
        target,
        lambda temporary: pq.write_table(table, temporary, compression="snappy"),
    )
    return target
