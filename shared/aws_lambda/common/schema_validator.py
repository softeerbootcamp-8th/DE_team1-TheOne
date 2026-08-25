"""Parquet 및 PyArrow 스키마 검증 공용 모듈."""

import io
import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaValidationResult:
    """스키마 차이를 하류 차단 오류와 허용 가능한 경고로 분리합니다."""

    missing_columns: tuple[str, ...] = ()
    type_mismatches: tuple[str, ...] = ()
    extra_columns: tuple[str, ...] = ()
    parse_error: str | None = None

    @property
    def errors(self) -> tuple[str, ...]:
        errors = list(self.missing_columns) + list(self.type_mismatches)
        if self.parse_error is not None:
            errors.append(self.parse_error)
        return tuple(errors)

    @property
    def warnings(self) -> tuple[str, ...]:
        return self.extra_columns

    @property
    def diffs(self) -> tuple[str, ...]:
        return self.errors + self.warnings


def validate_parquet_schema(
    parquet_data: bytes | pa.Schema, expected_schema: pa.Schema
) -> SchemaValidationResult:
    """Parquet 바이너리 또는 실제 스키마를 기대 스키마와 비교합니다.

    Args:
        parquet_data: 원천 Parquet 바이너리 또는 이미 읽은 PyArrow 스키마
        expected_schema: 기대하는 PyArrow 스키마

    Returns:
        누락·타입 불일치는 오류, 추가 컬럼은 경고로 분리한 결과
    """
    try:
        actual_schema = (
            parquet_data
            if isinstance(parquet_data, pa.Schema)
            else pq.read_schema(io.BytesIO(parquet_data))
        )
    except Exception as exc:
        logger.error("Parquet 스키마 읽기 실패: %s", exc)
        return SchemaValidationResult(
            parse_error=f"⚠️ Parquet 메타데이터 파싱 실패: {exc}"
        )

    expected_fields = {field.name: field.type for field in expected_schema}
    actual_fields = {field.name: field.type for field in actual_schema}
    missing_columns: list[str] = []
    type_mismatches: list[str] = []
    extra_columns: list[str] = []

    # 1. 누락되거나 타입이 변경된 컬럼 검사
    for name, exp_type in expected_fields.items():
        if name not in actual_fields:
            missing_columns.append(
                f"❌ 누락된 컬럼: `{name}` (기대 타입: `{exp_type}`)"
            )
        elif actual_fields[name] != exp_type:
            type_mismatches.append(
                f"⚠️ 타입 불일치 컬럼 `{name}`: 기대=`{exp_type}`, 실제=`{actual_fields[name]}`"
            )

    # 2. 새로 추가된 컬럼 검사
    for name, act_type in actual_fields.items():
        if name not in expected_fields:
            extra_columns.append(f"➕ 신규 추가된 컬럼: `{name}` (`{act_type}`)")

    return SchemaValidationResult(
        missing_columns=tuple(missing_columns),
        type_mismatches=tuple(type_mismatches),
        extra_columns=tuple(extra_columns),
    )
