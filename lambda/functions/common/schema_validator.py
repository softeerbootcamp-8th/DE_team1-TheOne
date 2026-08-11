"""Parquet 및 PyArrow 스키마 검증 공용 모듈."""

import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def validate_parquet_schema(
    parquet_data: bytes, expected_schema: pa.Schema
) -> list[str]:
    """Parquet 바이너리의 메타데이터 스키마와 기대 스키마를 비교하여 차이점을 반환합니다.

    Args:
        parquet_data: 원천 Parquet 바이너리 데이터
        expected_schema: 기대하는 PyArrow 스키마

    Returns:
        스키마 변동 사항(누락/추가/타입 불일치)을 담은 문자열 리스트. 차이가 없으면 빈 리스트.
    """
    diffs: list[str] = []
    try:
        actual_schema = pq.read_schema(io.BytesIO(parquet_data))
    except Exception as exc:
        logger.error("Parquet 스키마 읽기 실패: %s", exc)
        return [f"⚠️ Parquet 메타데이터 파싱 실패: {exc}"]

    expected_fields = {field.name: field.type for field in expected_schema}
    actual_fields = {field.name: field.type for field in actual_schema}

    # 1. 누락되거나 타입이 변경된 컬럼 검사
    for name, exp_type in expected_fields.items():
        if name not in actual_fields:
            diffs.append(f"❌ 누락된 컬럼: `{name}` (기대 타입: `{exp_type}`)")
        elif actual_fields[name] != exp_type:
            diffs.append(
                f"⚠️ 타입 불일치 컬럼 `{name}`: 기대=`{exp_type}`, 실제=`{actual_fields[name]}`"
            )

    # 2. 새로 추가된 컬럼 검사
    for name, act_type in actual_fields.items():
        if name not in expected_fields:
            diffs.append(f"➕ 신규 추가된 컬럼: `{name}` (`{act_type}`)")

    return diffs
