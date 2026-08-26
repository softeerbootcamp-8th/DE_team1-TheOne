"""Monthly Taxi Trip Silver 후보 전체를 GX Spark로 검증하고 결과를 발행합니다."""

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
from pyspark.sql import DataFrame

from schema.silver.monthly_taxi_trip import REQUIRED_COLUMNS
from shared.common.gx_data_docs import data_docs_config, upload_data_docs
from shared.common.s3_reader import parse_s3_uri


logger = logging.getLogger(__name__)

MISSING_OR_TYPE_VALID_COLUMN = "_gx_missing_or_type_valid"
VALUE_VALID_COLUMN = "_gx_value_valid"
SERVICE_TIER_VALID_COLUMN = "_gx_service_tier_valid"
RECORD_VALID_COLUMN = "_gx_record_valid"

_RULE_MISSING_OR_TYPE = "missing_or_type_mismatch"
_RULE_INVALID_VALUE = "invalid_value"
_RULE_INVALID_SERVICE_TIER = "invalid_service_tier"
_RULE_RECORD_WARNING = "record_warning"
_RULE_RECORD_ERROR = "record_error"
_RULE_EXTRA_COLUMNS = "extra_columns"

_EXPECTED_COLUMNS = (
    *REQUIRED_COLUMNS,
    MISSING_OR_TYPE_VALID_COLUMN,
    VALUE_VALID_COLUMN,
    SERVICE_TIER_VALID_COLUMN,
    RECORD_VALID_COLUMN,
)


@dataclass(frozen=True)
class SparkGXCounts:
    total: int
    valid: int
    invalid: int
    missing_or_type_mismatch: int
    invalid_value: int
    invalid_service_tier: int
    extra_columns: tuple[str, ...]
    invalid_ratio: float
    warning: bool
    warning_threshold: float
    error_threshold: float


def _thresholds(
    warning_threshold: float, error_threshold: float
) -> tuple[float, float]:
    warning = float(warning_threshold)
    error = float(error_threshold)
    if not 0 <= warning < error <= 1:
        raise ValueError(
            "품질 임계치는 0 <= warning_threshold < error_threshold <= 1 이어야 "
            f"합니다: warning={warning} error={error}"
        )
    return warning, error


def _record_expectation(
    *, column: str, mostly: float, rule: str, severity: str = "error"
):
    return gx.expectations.ExpectColumnValuesToBeInSet(
        column=column,
        value_set=[True],
        mostly=mostly,
        meta={"quality_rule": rule, "severity": severity},
    )


def _expectations(
    warning_threshold: float, error_threshold: float
) -> list:
    error_mostly = 1.0 - error_threshold
    return [
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1,
            meta={"quality_rule": "row_count", "severity": "error"},
        ),
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=list(_EXPECTED_COLUMNS),
            exact_match=True,
            meta={"quality_rule": _RULE_EXTRA_COLUMNS, "severity": "warning"},
        ),
        _record_expectation(
            column=MISSING_OR_TYPE_VALID_COLUMN,
            mostly=error_mostly,
            rule=_RULE_MISSING_OR_TYPE,
        ),
        _record_expectation(
            column=VALUE_VALID_COLUMN,
            mostly=error_mostly,
            rule=_RULE_INVALID_VALUE,
        ),
        _record_expectation(
            column=SERVICE_TIER_VALID_COLUMN,
            mostly=error_mostly,
            rule=_RULE_INVALID_SERVICE_TIER,
        ),
        _record_expectation(
            column=RECORD_VALID_COLUMN,
            mostly=1.0 - warning_threshold,
            rule=_RULE_RECORD_WARNING,
            severity="warning",
        ),
        _record_expectation(
            column=RECORD_VALID_COLUMN,
            mostly=error_mostly,
            rule=_RULE_RECORD_ERROR,
        ),
    ]


def _validate_gx_batch(
    dataframe: DataFrame,
    expectations: list,
    *,
    data_docs_location: str | None = None,
):
    """기존 SparkSession의 DataFrame을 검증하고 같은 결과로 Data Docs를 만듭니다."""
    temporary_docs = tempfile.TemporaryDirectory() if data_docs_location else None
    docs_root = Path(temporary_docs.name) if temporary_docs else None
    try:
        context = gx.get_context(
            mode="ephemeral",
            project_config=data_docs_config(docs_root) if docs_root else None,
        )
        context.variables.progress_bars = {"globally": False}
        batch_definition = (
            context.data_sources.add_spark(name="monthly_taxi_trip_silver_source")
            .add_dataframe_asset(name="monthly_taxi_trip_silver_asset")
            .add_batch_definition_whole_dataframe("monthly_taxi_trip_silver_batch")
        )
        suite = context.suites.add_or_update(
            gx.ExpectationSuite(
                name="monthly_taxi_trip_silver_suite",
                expectations=expectations,
            )
        )
        validation = gx.ValidationDefinition(
            name="monthly_taxi_trip_silver_validation",
            data=batch_definition,
            suite=suite,
        ).run(
            batch_parameters={"dataframe": dataframe},
            result_format="SUMMARY",
        )
        if docs_root and data_docs_location:
            context.build_data_docs(site_names=["local_site"])
            bucket, prefix = parse_s3_uri(data_docs_location)
            upload_data_docs(docs_root, bucket=bucket, prefix=prefix)
            logger.info("gx_data_docs updated path=%s", data_docs_location)
        return validation
    finally:
        if temporary_docs:
            temporary_docs.cleanup()


def _result_by_rule(validation) -> dict:
    results = {}
    for result in validation.results:
        rule = result.expectation_config.meta.get("quality_rule")
        if rule:
            results[rule] = result
    required = {
        "row_count",
        _RULE_MISSING_OR_TYPE,
        _RULE_INVALID_VALUE,
        _RULE_INVALID_SERVICE_TIER,
        _RULE_RECORD_WARNING,
        _RULE_RECORD_ERROR,
        _RULE_EXTRA_COLUMNS,
    }
    missing = sorted(required - results.keys())
    if missing:
        raise RuntimeError(f"GX 품질 결과에 규칙이 누락되었습니다: {missing}")
    return results


def _unexpected_count(result) -> int:
    value = dict(result.result).get("unexpected_count")
    if value is None:
        raise RuntimeError(
            "GX 품질 결과에 unexpected_count가 없습니다: "
            f"{result.expectation_config.meta.get('quality_rule')}"
        )
    return int(value)


def validate_monthly_taxi_trip_records(
    dataframe: DataFrame,
    *,
    warning_threshold: float,
    error_threshold: float,
    data_docs_location: str | None = None,
) -> SparkGXCounts:
    """전체 Spark DataFrame을 GX로 검사하고 행 단위 위반 건수를 반환합니다."""
    warning_threshold, error_threshold = _thresholds(
        warning_threshold, error_threshold
    )
    validation = _validate_gx_batch(
        dataframe,
        _expectations(warning_threshold, error_threshold),
        data_docs_location=data_docs_location,
    )
    results = _result_by_rule(validation)
    record_result = results[_RULE_RECORD_ERROR]
    total = int(dict(record_result.result).get("element_count") or 0)
    if total == 0:
        raise ValueError("Silver 후보 레코드가 0건입니다")

    invalid = _unexpected_count(record_result)
    invalid_ratio = invalid / total
    extra_columns = tuple(
        sorted(set(dataframe.columns) - set(_EXPECTED_COLUMNS))
    )
    counts = SparkGXCounts(
        total=total,
        valid=total - invalid,
        invalid=invalid,
        missing_or_type_mismatch=_unexpected_count(
            results[_RULE_MISSING_OR_TYPE]
        ),
        invalid_value=_unexpected_count(results[_RULE_INVALID_VALUE]),
        invalid_service_tier=_unexpected_count(
            results[_RULE_INVALID_SERVICE_TIER]
        ),
        extra_columns=extra_columns,
        invalid_ratio=invalid_ratio,
        warning=invalid > 0 and invalid_ratio >= warning_threshold,
        warning_threshold=warning_threshold,
        error_threshold=error_threshold,
    )
    logger.info(
        "GX Spark 전체 레코드 검증: total=%d valid=%d invalid=%d ratio=%.4f",
        counts.total,
        counts.valid,
        counts.invalid,
        counts.invalid_ratio,
    )
    if counts.warning:
        logger.warning(
            "GX Spark 품질 경고: 불합격 비율 %.2f%% (경고 임계치 %.2f%%)",
            counts.invalid_ratio * 100,
            warning_threshold * 100,
        )
    # GX mostly는 허용 비율과 같은 경계를 성공으로 보므로, 팀 계약인 `>=` 실패를
    # GX가 계산한 unexpected_count로 명시적으로 판정합니다.
    if invalid_ratio >= error_threshold:
        raise ValueError(
            f"불합격 비율이 {invalid_ratio:.2%}로 임계치"
            f"({error_threshold:.2%}) 이상입니다"
        )
    return counts


__all__ = [
    "MISSING_OR_TYPE_VALID_COLUMN",
    "RECORD_VALID_COLUMN",
    "SERVICE_TIER_VALID_COLUMN",
    "SparkGXCounts",
    "VALUE_VALID_COLUMN",
    "validate_monthly_taxi_trip_records",
]
