"""보유 차량 Bronze 를 Silver 스키마로 정제합니다.

재고는 Gold 의 대당 수익 계산에 바로 들어갑니다. 재고·가격·연비가 0 이하이거나
차종 ID 가 겹치면 계산이 실패하지 않고 **틀린 숫자**를 내므로 여기서 막습니다.
"""

import math

import pyarrow as pa
from pipeline_core.transformer import Transformer

from schema.silver import (
    CLEAN_LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL as REQUIRED_NON_NULL,
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SCHEMA,
)


# 0 이하면 재고·연비·가격 어느 쪽이든 계산에 쓸 수 없는 값입니다.
POSITIVE_COLUMNS = ("fuel_efficiency", "weekly_lease_fee", "stock")
MIN_MODEL_YEAR = 1900
MAX_MODEL_YEAR = 2100


class LeaseVehicleInventorySilverTransformer(Transformer):
    """Bronze 테이블을 Silver 스키마의 테이블로 정제합니다."""

    def transform(self, data: pa.Table) -> pa.Table:
        missing = set(SCHEMA.names) - set(data.column_names)
        if missing:
            raise ValueError(f"보유 차량 데이터 필수 컬럼 누락: {sorted(missing)}")
        try:
            cleaned = data.select(SCHEMA.names).cast(SCHEMA)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            raise ValueError(
                "보유 차량 데이터 타입을 Silver 스키마로 변환하지 못했습니다"
            ) from exc
        rows = cleaned.to_pylist()
        if not rows:
            raise ValueError("보유 차량 데이터가 비어 있습니다")

        for row in rows:
            for column in REQUIRED_NON_NULL:
                value = row[column]
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ValueError(f"보유 차량 데이터 필수값이 비었습니다: {column}")
                # NaN 은 None 검사와 양수 검사를 모두 통과하므로 따로 막습니다.
                if isinstance(value, float) and math.isnan(value):
                    raise ValueError(
                        f"보유 차량 데이터 필수값이 NaN 입니다: {column}"
                    )
                if isinstance(value, str):
                    row[column] = value.strip()
            # 리스 계약(driver_vehicle_leases)의 make_key·model_key 와 붙일 조인
            # 키라 같은 대문자 규칙으로 맞춥니다.
            row["manufacturer"] = row["manufacturer"].upper()
            row["model_name"] = row["model_name"].upper()
            if not MIN_MODEL_YEAR <= row["model_year"] <= MAX_MODEL_YEAR:
                raise ValueError("model_year가 허용 범위를 벗어났습니다")
            for column in POSITIVE_COLUMNS:
                if row[column] <= 0:
                    raise ValueError(f"보유 차량 데이터 값이 0 이하입니다: {column}")

        model_ids = [row["vehicle_model_id"] for row in rows]
        if len(model_ids) != len(set(model_ids)):
            raise ValueError("vehicle_model_id가 중복됩니다")
        return pa.Table.from_pylist(rows, schema=SCHEMA)
