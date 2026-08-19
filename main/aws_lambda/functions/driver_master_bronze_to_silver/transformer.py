"""기사 계약 Bronze 를 Silver 스키마로 정제합니다.

한 대의 택시나 한 명의 기사가 같은 시점에 두 계약을 가질 수 없습니다. 겹치면
운행 × 리스 조인이 한 운행을 두 계약에 붙여 기사별 수익이 부풀어도 실패하지
않습니다 — 그래서 적재 전에 막습니다.
"""

from datetime import date

import pyarrow as pa
from pipeline_core.transformer import Transformer

from schema.silver.driver_vehicle_leases import REQUIRED_NON_NULL, SCHEMA


MIN_MODEL_YEAR = 1900
MAX_MODEL_YEAR = 2100


def _validate_no_overlap(rows: list[dict], key: str) -> None:
    grouped: dict[str, list[tuple[date, date | None]]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(
            (row["lease_started_on"], row["lease_ended_on"])
        )
    for value, periods in grouped.items():
        periods.sort(key=lambda period: period[0])
        for previous, current in zip(periods, periods[1:]):
            previous_end = previous[1]
            if previous_end is None or current[0] < previous_end:
                raise ValueError(f"{key}의 리스 기간이 겹칩니다: {value}")


class DriverVehicleLeaseSilverTransformer(Transformer):
    """Bronze 테이블을 Silver 스키마의 테이블로 정제합니다."""

    def transform(self, data: pa.Table) -> pa.Table:
        missing = set(SCHEMA.names) - set(data.column_names)
        if missing:
            raise ValueError(f"기사 데이터 필수 컬럼 누락: {sorted(missing)}")
        try:
            cleaned = data.select(SCHEMA.names).cast(SCHEMA)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise ValueError("기사 데이터 타입을 Silver 스키마로 변환하지 못했습니다") from exc
        rows = cleaned.to_pylist()
        if not rows:
            raise ValueError("기사 데이터가 비어 있습니다")

        for row in rows:
            for column in REQUIRED_NON_NULL:
                value = row[column]
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ValueError(f"기사 데이터 필수값이 비었습니다: {column}")
                if isinstance(value, str):
                    row[column] = value.strip()
            row["make_key"] = row["make_key"].upper()
            row["model_key"] = row["model_key"].upper()
            if not MIN_MODEL_YEAR <= row["model_year"] <= MAX_MODEL_YEAR:
                raise ValueError("model_year가 허용 범위를 벗어났습니다")
            ended = row["lease_ended_on"]
            if ended is not None and row["lease_started_on"] >= ended:
                raise ValueError("리스 종료일은 시작일보다 늦어야 합니다")

        lease_ids = [row["lease_id"] for row in rows]
        if len(lease_ids) != len(set(lease_ids)):
            raise ValueError("lease_id가 중복됩니다")
        _validate_no_overlap(rows, "taxi_id")
        _validate_no_overlap(rows, "driver_id")
        return pa.Table.from_pylist(rows, schema=SCHEMA)
