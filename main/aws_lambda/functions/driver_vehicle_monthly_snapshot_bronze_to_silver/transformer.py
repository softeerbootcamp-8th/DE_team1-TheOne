"""기사 차량 월별 스냅샷 Bronze 를 Silver 스키마로 정제합니다.

퇴사한 기사는 Gold 운행 집계 대상이 아니므로 Silver 에서 제외합니다. 남은 한 기사는
그 달에 차량 하나로 스냅샷됩니다. driver_id 가 그 달 안에서 중복되면 운행 ×
스냅샷 조인이 기사 한 명을 두 스냅샷에 붙여 집계가 부풀어도 실패하지 않습니다 —
그래서 적재 전에 막습니다.
"""

import pyarrow as pa
from pipeline_core.transformer import Transformer

from schema.silver import (
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA,
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA_REQUIRED_NON_NULL as REQUIRED_NON_NULL,
)


class DriverVehicleMonthlySnapshotSilverTransformer(Transformer):
    """Bronze 테이블을 Silver 스키마의 테이블로 정제합니다."""

    def transform(self, data: pa.Table) -> pa.Table:
        missing = set(SCHEMA.names) - set(data.column_names)
        if missing:
            raise ValueError(f"기사 차량 스냅샷 필수 컬럼 누락: {sorted(missing)}")
        try:
            cleaned = data.select(SCHEMA.names).cast(SCHEMA)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise ValueError(
                "기사 차량 스냅샷 타입을 Silver 스키마로 변환하지 못했습니다"
            ) from exc
        rows = [row for row in cleaned.to_pylist() if row["exit_date"] is None]
        if not rows:
            raise ValueError("현재 유효한 기사 차량 스냅샷 데이터가 비어 있습니다")

        for row in rows:
            for column in REQUIRED_NON_NULL:
                value = row[column]
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ValueError(f"기사 차량 스냅샷 필수값이 비었습니다: {column}")
                if isinstance(value, str):
                    row[column] = value.strip()
            row["manufacturer"] = row["manufacturer"].upper()
            row["model_name"] = row["model_name"].upper()
            if row["weekly_lease_fee"] <= 0:
                raise ValueError("weekly_lease_fee가 0 이하입니다")

        driver_ids = [row["driver_id"] for row in rows]
        if len(driver_ids) != len(set(driver_ids)):
            raise ValueError("driver_id가 중복됩니다")

        return pa.Table.from_pylist(rows, schema=SCHEMA)
