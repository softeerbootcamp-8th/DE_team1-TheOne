"""월별 택시 운행 데이터 Raw-to-Silver DAG의 실행·검증 함수."""

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
from airflow.sdk import task

from main.airflow.common.monthly_bronze import (
    SILVER_PART_PATTERN,
    silver_part_paths,
    silver_version_path,
    validate_monthly_parquet_bronze,
)
from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_failure_callback
from shared.airflow.common.slack_quality_warning import send_quality_warning
from shared.airflow.common.validation import (
    S3Location,
    parquet_file,
    parse_location,
    parse_handler_result,
    parse_year_month,
    publish_success_marker,
    run_gx_validation,
)
from shared.common.s3_reader import list_keys
from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as SCHEMA
from schema.silver import (
    CLEAN_MONTHLY_TAXI_TRIP_REQUIRED_NON_NULL as SILVER_REQUIRED_NON_NULL,
    CLEAN_MONTHLY_TAXI_TRIP_SCHEMA as SILVER_SCHEMA,
)


logger = logging.getLogger(__name__)

for path in (
    PROJECT_ROOT / "main" / "lambda",
    PROJECT_ROOT / "main" / "spark",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

DEFAULT_BRONZE_DIR = os.getenv(
    "BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")
)
DEFAULT_SILVER_DIR = os.getenv(
    "SILVER_DIR", str(PROJECT_ROOT / "data" / "silver" / "monthly_taxi_trip")
)
# Bronze 한 달에서 버려도 되는 행의 비율. 넘으면 원천이 바뀐 것으로 보고 멈춥니다.
# 0.2 는 초기 관측치를 넉넉히 감싸려고 둔 값이라, 원천 스키마가 통째로 어긋나도
# 통과할 만큼 느슨했습니다. 실측 불합격률이 1% 미만이라 5% 로 조입니다 (#508).
MONTHLY_TAXI_TRIP_ERROR_THRESHOLD = 0.05
MONTHLY_TAXI_TRIP_WARNING_THRESHOLD = 0.01
DEFAULT_API_BASE_URL = "http://10.0.10.81:8091"


def _schema_signature(schema: pa.Schema, *, logical_timestamp: bool = False) -> str:
    """GX가 한 행짜리 품질 요약에서 비교할 Parquet 스키마 문자열을 만듭니다."""
    fields = []
    for field in schema:
        if logical_timestamp and pa.types.is_timestamp(field.type):
            field_type = (
                "timestamp"
                if field.type.tz is None
                else f"timestamp[tz={field.type.tz}]"
            )
        else:
            field_type = str(field.type)
        fields.append(f"{field.name}:{field_type}")
    return "|".join(fields)


def _bronze_quality_summary(parquet_file, required_columns):
    """Spark와 같은 유효성 조건을 Parquet 배치별로 계산합니다.

    물리 스키마 전체 일치는 확인하지 않습니다 — 원천이 MONTHLY_TAXI_TRIP_SCHEMA 보다
    많은 컬럼을 보내도(#529 진행 중) required_columns 만 있으면 검증을 계속합니다.
    """
    import pandas as pd

    schema = parquet_file.schema_arrow
    row_count = parquet_file.metadata.num_rows
    missing_columns = [name for name in required_columns if name not in schema.names]
    extra_columns = sorted(set(schema.names) - set(required_columns))
    invalid_rows = 0

    if row_count and not missing_columns:
        for batch in parquet_file.iter_batches(columns=required_columns):
            frame = batch.to_pandas()
            pickup = pd.to_datetime(frame["pickup_datetime"], errors="coerce")
            dropoff = pd.to_datetime(frame["dropoff_datetime"], errors="coerce")
            pickup_location = pd.to_numeric(frame["PULocationID"], errors="coerce")
            dropoff_location = pd.to_numeric(frame["DOLocationID"], errors="coerce")
            trip_miles = pd.to_numeric(frame["trip_miles"], errors="coerce")
            trip_time = pd.to_numeric(frame["trip_time"], errors="coerce")
            driver_pay = pd.to_numeric(frame["driver_pay"], errors="coerce")
            tips = pd.to_numeric(frame["tips"], errors="coerce")
            required_non_null = [
                name for name in required_columns if name in SILVER_REQUIRED_NON_NULL
            ]
            present = frame[required_non_null].notna().all(axis=1)
            for name in (
                "taxi_id",
                "hvfhs_license_num",
                "pickup_zone",
                "dropoff_zone",
                "estimated_service_tier",
            ):
                present &= frame[name].astype("string").str.strip().ne("").fillna(False)
            valid_time = pickup.notna() & dropoff.notna() & pickup.lt(dropoff)
            valid_range = (
                pickup_location.notna()
                & dropoff_location.notna()
                & trip_miles.gt(0)
                & trip_miles.le(1000)
                & trip_time.gt(0)
                & trip_time.le(86400)
                & driver_pay.ge(0)
                & driver_pay.le(5000)
                & tips.ge(0)
                & tips.le(5000)
            )
            valid_service_tier = (
                frame["hvfhs_license_num"].eq("HV0003")
                & frame["estimated_service_tier"].isin(["Standard", "Comfort"])
            ) | (
                frame["hvfhs_license_num"].eq("HV0005")
                & frame["estimated_service_tier"].isin(
                    ["Standard", "Extra Comfort"]
                )
            )
            valid = present & valid_time & valid_range & valid_service_tier
            invalid_rows += int((~valid).sum())

    return pd.DataFrame(
        [
            {
                "row_count": row_count,
                "schema_signature": _schema_signature(schema),
                "missing_required_columns": ",".join(missing_columns),
                "extra_columns": ",".join(extra_columns),
                "invalid_required_row_count": invalid_rows,
                "invalid_required_row_ratio": (
                    invalid_rows / row_count
                    if row_count and not missing_columns
                    else None
                ),
            }
        ]
    )


def _silver_quality_summary(parquet_files, required_columns):
    """Silver 월 전체 행 수·논리 스키마·필수값 NULL 건수를 요약합니다."""
    import pandas as pd

    row_count = 0
    schema_signatures = set()
    null_counts = {column: 0 for column in required_columns}
    # 루프 변수 이름을 `parquet_file` 로 두면 이 모듈이 import 한 같은 이름의 함수를
    # 가립니다. 지금은 루프 안에서 그 함수를 안 부르니 결과가 맞지만, 나중에 부르려
    # 하면 조용히 루프 변수를 받습니다.
    for file in parquet_files:
        schema = file.schema_arrow
        row_count += file.metadata.num_rows
        schema_signatures.add(_schema_signature(schema, logical_timestamp=True))
        for column in required_columns:
            if column not in schema.names:
                null_counts[column] = None
            elif null_counts[column] is not None:
                null_counts[column] += sum(
                    batch.column(column).null_count
                    for batch in file.iter_batches(columns=[column])
                )
    return pd.DataFrame(
        [
            {
                "row_count": row_count,
                "schema_signature": ",".join(sorted(schema_signatures)),
                **{
                    f"{column}_null_count": value
                    for column, value in null_counts.items()
                },
            }
        ]
    )


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    """월별 택시 운행 데이터를 Bronze에 저장합니다."""
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        "year": params.get("year"),
        "month": params.get("month"),
    }
    if params.get("service_area") is not None:
        event["service_area"] = params["service_area"]
    logger.info("raw_to_bronze 작업 시작: event=%s", event)
    result = lambda_handler_for("monthly_taxi_trip_raw_to_bronze")(event=event)
    logger.info("raw_to_bronze 작업 완료: result=%s", result)
    return result


def existing_silver_partitions(
    silver_dir: str | Path | S3Location,
) -> list[str]:
    """지금 있는 `year_month=` 파티션 이름들. Spark 쓰기 **전에** 찍어 둡니다.

    #165 는 정적 overwrite 가 **기존에 있던** 다른 달을 지운 사고였습니다. 그러니
    감시해야 할 것은 "쓰기 전에 있던 것이 쓰기 후에도 있는가" 입니다. 쓰기 후의 모양만
    보고 판단하면(예전처럼 "직전 달이 있어야 한다") 과거 달을 새로 채우는 정상 백필을
    구분할 수 없습니다 — 어느 달을 넣든 그 직전 달은 없기 마련이라 항상 막혔습니다.

    **지역 계층(#674)에 이 함수 자체는 인자가 필요 없습니다.** 두 호출부가 모두
    `version_path.parent.parent` 로 루트를 유도하므로, writer 가 지역 경로로 옮겨지면
    before/after 양쪽이 **자동으로 같은 지역 범위**가 됩니다(지역별 독립 감시가 되는
    것이 옳은 의미이기도 합니다). 한쪽만 지역 스코프가 되면 집합이 어긋나 가드가
    거짓 통과하거나 거짓 실패하므로, 호출부를 바꿀 때 **두 곳을 함께** 보세요.
    """
    if isinstance(silver_dir, S3Location):
        prefix = f"{silver_dir.key.rstrip('/')}/"
        keys = list_keys(silver_dir.bucket, prefix)
        published = set()
        for marker in keys:
            if "/source_collected_at=" not in marker or not marker.endswith("/_SUCCESS"):
                continue
            version_prefix = marker.removesuffix("_SUCCESS")
            if any(
                key.startswith(version_prefix)
                and "/" not in key.removeprefix(version_prefix)
                and SILVER_PART_PATTERN.fullmatch(Path(key).name)
                for key in keys
            ):
                published.add(marker.removeprefix(prefix).split("/", 1)[0])
        return sorted(published)

    root = Path(silver_dir)
    if not root.is_dir():
        return []
    existing = []
    for partition in root.glob("year_month=*"):
        if not partition.is_dir():
            continue
        has_published_version = any(
            (version / "_SUCCESS").is_file()
            and any(version.glob("part-*.parquet"))
            for version in partition.glob("source_collected_at=*")
            if version.is_dir()
        )
        if has_published_version:
            existing.append(partition.name)
    return sorted(existing)


@task(
    task_id="validate_bronze",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_bronze_task(result: dict, **context) -> dict:
    """파일 경계를 확인한 뒤 Bronze 데이터 품질을 GX로 검증합니다."""
    params = context.get("params", {})
    # Param이 없는 호출(단위 테스트 등)에서도 기존 리터럴과 같게 동작해야 합니다.
    configured_threshold = params.get("error_threshold")
    error_threshold = float(
        configured_threshold
        if configured_threshold is not None
        else MONTHLY_TAXI_TRIP_ERROR_THRESHOLD
    )
    summary = _bronze_quality_result(result, params, list(SCHEMA.names))
    import great_expectations as gx

    expectations = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="row_count", min_value=1
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="missing_required_columns", value_set=[""]
        ),
    ]
    if summary["invalid_required_row_ratio"].notna().all():
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="invalid_required_row_ratio",
                    min_value=0,
                    max_value=MONTHLY_TAXI_TRIP_WARNING_THRESHOLD,
                    strict_max=True,
                    meta={"severity": "warning"},
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="invalid_required_row_ratio",
                    min_value=0,
                    max_value=error_threshold,
                    strict_max=True,
                ),
            ]
        )
    expectations.append(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="extra_columns",
            value_set=[""],
            meta={"severity": "warning"},
        )
    )
    run_gx_validation(
        summary,
        expectations,
        suite_name="monthly_taxi_trip_bronze_suite",
        layer="bronze",
    )
    invalid_ratio = float(summary.at[0, "invalid_required_row_ratio"])
    extra_columns = [
        column for column in summary.at[0, "extra_columns"].split(",") if column
    ]
    if invalid_ratio >= MONTHLY_TAXI_TRIP_WARNING_THRESHOLD or extra_columns:
        send_quality_warning(
            context,
            dataset="monthly_taxi_trip",
            year_month=result["year_month"],
            invalid_rows=int(summary.at[0, "invalid_required_row_count"]),
            row_count=int(summary.at[0, "row_count"]),
            invalid_ratio=invalid_ratio,
            extra_columns=extra_columns,
        )
    bronze_path = parse_handler_result(result, expected_locations=1).locations[0]
    publish_success_marker(bronze_path.parent)
    service_area = params.get("service_area")
    version_path = silver_version_path(
        DEFAULT_SILVER_DIR,
        result,
        service_area=service_area,
    )
    silver_root = version_path.parent.parent
    # Spark 쓰기 전 상태입니다. validate_silver 가 이것과 비교해 #165 재발을 봅니다.
    return {
        **result,
        "silver_version_path": str(version_path),
        "silver_partitions_before": existing_silver_partitions(silver_root),
    }


def _bronze_quality_result(
    result: dict,
    params: dict,
    required_columns: list[str],
):
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    path, _ = validate_monthly_parquet_bronze(
        result,
        dataset_dir="monthly_taxi_trip",
        base_dir=base_dir,
        service_area=params.get("service_area"),
    )
    try:
        source = parquet_file(path)
    except RuntimeError as exc:
        raise ValueError(
            f"Parquet 을 읽지 못했습니다 (다운로드가 잘렸을 수 있음): {path}"
        ) from exc

    summary = _bronze_quality_summary(source, required_columns)
    return summary


@task(
    task_id="validate_silver",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_silver_task(raw_result: dict, **context) -> None:
    """Spark 실행이 만든 Silver 버전 파일을 직접 열어서 확인합니다."""
    parsed = parse_handler_result(raw_result, expected_locations=1)
    # 반환값을 쓰지 않습니다 — 이 호출 자체가 검증입니다. YYYY-MM 이 아니면
    # ValueError 로 막습니다. 대입으로 두면 미사용 변수로 보여 지워질 수 있습니다.
    parse_year_month(raw_result.get("year_month"), field="year_month")
    bronze_rows = parquet_file(parsed.locations[0]).metadata.num_rows

    version_path = parse_location(raw_result["silver_version_path"])
    expected_schema = SILVER_SCHEMA
    required_columns = list(SILVER_SCHEMA.names)
    part_paths = silver_part_paths(version_path)
    if not part_paths:
        raise ValueError(f"Silver part 파일이 없습니다: {version_path}")
    parquet_files = [parquet_file(path) for path in part_paths]
    summary = _silver_quality_summary(parquet_files, required_columns)
    import great_expectations as gx

    expectations = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="row_count", min_value=1
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="schema_signature",
            value_set=[
                _schema_signature(expected_schema, logical_timestamp=True)
            ],
        ),
        # NULL 건수는 전 컬럼을 요약에 담아 Data Docs 에서 보이게 두고, 검사는
        # 필수값 계약이 있는 컬럼에만 겁니다.
        *(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=f"{column}_null_count", value_set=[0]
            )
            for column in required_columns
            if column in SILVER_REQUIRED_NON_NULL
        ),
    ]
    run_gx_validation(
        summary,
        expectations,
        suite_name="monthly_taxi_trip_silver_suite",
        layer="silver",
    )

    silver_rows = int(summary["row_count"].sum())
    if silver_rows > bronze_rows:
        raise ValueError(
            f"Silver 행 수가 Bronze 보다 많습니다: {silver_rows} > {bronze_rows}"
        )

    # #165 재발 감시 — 쓰기 전에 있던 파티션이 사라졌는지만 봅니다. 이번에 쓴 달은
    # 당연히 새로 생기므로 비교 대상이 아닙니다.
    before = set(raw_result.get("silver_partitions_before") or [])
    after = set(existing_silver_partitions(version_path.parent.parent))
    # 재처리 중인 현재 월은 writer가 기존 marker를 먼저 지워 아직
    # `after`에 보이지 않습니다. 다른 월이 사라진 경우만 #165 재발입니다.
    current_partition = version_path.parent.name
    lost = sorted(before - after - {current_partition})
    if lost:
        raise ValueError(
            f"쓰기 전에 있던 Silver 파티션이 사라졌습니다 (#165 재발): {lost}"
        )

    publish_success_marker(version_path)
