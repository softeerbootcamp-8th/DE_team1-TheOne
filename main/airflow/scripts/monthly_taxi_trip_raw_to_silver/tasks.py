"""월별 택시 운행 데이터 Raw-to-Silver DAG의 실행·검증 함수."""

import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
from airflow.sdk import task
from airflow.task.trigger_rule import TriggerRule

from main.airflow.common.monthly_bronze import (
    SILVER_PART_PATTERN,
    silver_part_paths,
    silver_version_path,
    validate_monthly_parquet_bronze,
)
from shared.airflow.common.lambda_invoke import invoke_lambda
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_failure_callback
from shared.airflow.common.slack_quality_warning import send_quality_warning
from shared.airflow.common.validation import (
    S3Location,
    parquet_file,
    parse_location,
    parse_handler_result,
    parse_year_month,
    run_quality_gate,
)
from shared.common.gx_data_docs import (
    GX_VALIDATION_SUMMARY_FILE_NAME,
    mirrored_data_docs_prefix,
)
from shared.common.s3_reader import get_object_bytes, list_keys, parse_s3_uri
from shared.common.success_marker import recon_key, recon_path
from schema.silver import CLEAN_MONTHLY_TAXI_TRIP_SCHEMA as SILVER_SCHEMA


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
# Spark가 정제한 Silver 후보 레코드의 경고·실패 비율입니다. Airflow는 DAG Param
# 기본값과 Spark가 남긴 reconciliation 경고를 읽을 때만 이 값을 사용합니다.
MONTHLY_TAXI_TRIP_ERROR_THRESHOLD = 0.05
MONTHLY_TAXI_TRIP_WARNING_THRESHOLD = 0.01


def _schema_signature(schema: pa.Schema, *, logical_timestamp: bool = False) -> str:
    """Spark part 파일의 논리 스키마 계약을 비교할 문자열을 만듭니다."""
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


def _read_recon(version_path: Path | S3Location) -> dict:
    """Spark 가 남긴 `_RECON.json` 을 읽습니다.

    없으면 실패시킵니다. 없는 걸 통과시키면 옛 코드로 돈 실행이 조용히 검사를
    건너뛰고, 그게 바로 이 검사를 넣는 이유였던 "조용히 틀린 값" 입니다.
    """
    if isinstance(version_path, S3Location):
        key = recon_key(version_path.key)
        body = get_object_bytes(version_path.bucket, key)
        if not body:
            raise ValueError(
                f"reconciliation sidecar 가 없습니다: s3://{version_path.bucket}/{key}"
            )
        return json.loads(body)
    path = recon_path(version_path)
    if not path.is_file():
        raise ValueError(f"reconciliation sidecar 가 없습니다: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _reconcile_silver(
    version_path: Path | S3Location, bronze_rows: int, silver_rows: int
) -> dict:
    """Bronze 와 Silver 의 차이가 Spark 가 보고한 제외 건수로 설명되는지 봅니다.

        Bronze = Silver + 제외

    Spark 안에서 `invalid = total - valid` 는 항등식입니다. 여기서 의미가 생기는
    건 Bronze·Silver 행 수를 **Airflow 가 parquet 메타데이터로 따로 세서** 맞대기
    때문입니다. 계산과 저장 사이에서 행이 사라지면 그때만 어긋납니다.

    예전 검사는 `silver_rows > bronze_rows` 뿐이라 Bronze 100만 건이 Silver 1건이
    되어도 통과했습니다.
    """
    recon = _read_recon(version_path)
    excluded = int(recon["invalid"])
    expected_bronze = silver_rows + excluded
    if bronze_rows != expected_bronze:
        raise ValueError(
            "택시 운행 reconciliation 실패: "
            f"bronze={bronze_rows} silver={silver_rows} excluded={excluded} "
            f"(기대 bronze={expected_bronze}) — 사유별 "
            f"NULL/타입={recon.get('missing_or_type_mismatch')} "
            f"값범위={recon.get('invalid_value')} "
            f"등급={recon.get('invalid_service_tier')}"
        )
    logger.info(
        "reconciliation %s",
        json.dumps(
            {
                "dataset": "monthly_taxi_trip",
                "input_rows": bronze_rows,
                "output_rows": silver_rows,
                "excluded_rows": excluded,
                "rule": "input = output + excluded",
                "status": "passed",
            },
            ensure_ascii=False,
        ),
    )
    return recon


def _gx_summary_location(version_path: Path | S3Location) -> Path | S3Location:
    if isinstance(version_path, S3Location):
        return S3Location(
            version_path.bucket,
            f"{version_path.key.rstrip('/')}/{GX_VALIDATION_SUMMARY_FILE_NAME}",
        )
    return version_path / GX_VALIDATION_SUMMARY_FILE_NAME


def _read_gx_summary(version_path: Path | S3Location) -> dict:
    location = _gx_summary_location(version_path)
    try:
        body = (
            get_object_bytes(location.bucket, location.key)
            if isinstance(location, S3Location)
            else location.read_bytes()
        )
    except Exception as exc:
        raise ValueError(f"Spark GX 검증 요약이 없습니다: {location}") from exc
    try:
        summary = json.loads(body)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Spark GX 검증 요약이 JSON이 아닙니다: {location}") from exc
    required = {"success", "total", "valid", "invalid", "extra_columns"}
    missing = sorted(required - summary.keys())
    if missing:
        raise ValueError(f"Spark GX 검증 요약 필드가 누락되었습니다: {missing}")
    if not isinstance(summary["success"], bool):
        raise ValueError("Spark GX 검증 요약 success는 bool이어야 합니다")
    for field in ("total", "valid", "invalid"):
        if isinstance(summary[field], bool) or not isinstance(summary[field], int):
            raise ValueError(f"Spark GX 검증 요약 {field}은 정수여야 합니다")
    if not isinstance(summary["extra_columns"], list) or not all(
        isinstance(column, str) for column in summary["extra_columns"]
    ):
        raise ValueError("Spark GX 검증 요약 extra_columns는 문자열 목록이어야 합니다")
    return summary


def _expected_gx_docs_location(version_path: S3Location) -> str:
    prefix = mirrored_data_docs_prefix(
        version_path.key,
        layer="silver",
        dataset="monthly_taxi_trip",
        data_is_file=False,
    )
    return f"s3://{version_path.bucket}/{prefix}"


def _report_gx_validation(raw_result: dict) -> dict:
    version_path = parse_location(raw_result["silver_version_path"])
    summary = _read_gx_summary(version_path)
    docs_path = summary.get("data_docs_path")
    if isinstance(version_path, S3Location):
        expected_docs = _expected_gx_docs_location(version_path)
        if docs_path != expected_docs:
            raise ValueError(
                f"GX Data Docs 경로가 계약과 다릅니다: expected={expected_docs} actual={docs_path}"
            )
        bucket, prefix = parse_s3_uri(docs_path)
        try:
            index = get_object_bytes(bucket, f"{prefix.rstrip('/')}/index.html")
        except Exception as exc:
            raise ValueError(f"GX Data Docs index가 없습니다: {docs_path}/index.html") from exc
        if not index:
            raise ValueError(f"GX Data Docs index가 비어 있습니다: {docs_path}/index.html")
    elif docs_path is not None:
        raise ValueError(f"로컬 Spark GX 결과가 S3 Data Docs를 가리킵니다: {docs_path}")

    log = logger.info if summary["success"] else logger.error
    log(
        "gx_validation_report dataset=monthly_taxi_trip layer=silver "
        "success=%s total=%s valid=%s invalid=%s extra_columns=%s data_docs=%s",
        summary["success"],
        summary["total"],
        summary["valid"],
        summary["invalid"],
        summary["extra_columns"],
        docs_path or "disabled(local)",
    )
    return summary


@task(
    task_id="report_gx_validation",
    trigger_rule=TriggerRule.ALL_DONE,
    retries=0,
    on_failure_callback=slack_failure_callback,
)
def report_gx_validation_task(raw_result: dict) -> dict:
    """Spark 성공·실패와 무관하게 GX 결과와 Data Docs 위치를 Airflow에 노출합니다."""
    return _report_gx_validation(raw_result)


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    """월별 택시 운행 데이터를 Bronze에 저장합니다."""
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params["api_base_url"],
        "year": params.get("year"),
        "month": params.get("month"),
    }
    if params.get("service_area") is not None:
        event["service_area"] = params["service_area"]
    logger.info("raw_to_bronze 작업 시작: event=%s", event)
    result = invoke_lambda(
        "monthly_taxi_trip_raw_to_bronze",
        package="main.aws_lambda.functions",
        event=event,
        local_event={
            "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        },
    )
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
    state = {"result": result}
    return run_quality_gate(
        lambda: parse_handler_result(
            state["result"], expected_locations=1
        ).locations[0].parent,
        lambda: _validate_bronze(state, context),
        layer="bronze",
        context=context,
    )


def _validate_bronze(state: dict, context: dict) -> dict:
    """Bronze의 manifest·체크섬·파일 경계만 검증합니다."""
    result = state["result"]
    params = context.get("params", {})
    validate_monthly_parquet_bronze(
        result,
        dataset_dir="monthly_taxi_trip",
        base_dir=params.get("base_dir") or DEFAULT_BRONZE_DIR,
        service_area=params.get("service_area"),
    )
    service_area = params.get("service_area")
    version_path = silver_version_path(
        DEFAULT_SILVER_DIR,
        result,
        service_area=service_area,
    )
    silver_root = version_path.parent.parent
    # Spark 쓰기 전 상태입니다. validate_silver 가 이것과 비교해 #165 재발을 봅니다.
    validated = {
        **result,
        "silver_version_path": str(version_path),
        "silver_partitions_before": existing_silver_partitions(silver_root),
    }
    state["result"] = validated
    return validated


@task(
    task_id="validate_silver",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_silver_task(raw_result: dict, **context) -> None:
    version_path = parse_location(raw_result["silver_version_path"])
    run_quality_gate(
        version_path,
        lambda: _validate_silver(raw_result, context),
        layer="silver",
        context=context,
    )


def _validate_silver(raw_result: dict, context: dict | None = None) -> None:
    """Spark GX 이후 Silver 파일 계약과 reconciliation을 확인합니다."""
    parsed = parse_handler_result(raw_result, expected_locations=1)
    # 반환값을 쓰지 않습니다 — 이 호출 자체가 검증입니다. YYYY-MM 이 아니면
    # ValueError 로 막습니다. 대입으로 두면 미사용 변수로 보여 지워질 수 있습니다.
    parse_year_month(raw_result.get("year_month"), field="year_month")
    bronze_rows = parquet_file(parsed.locations[0]).metadata.num_rows

    version_path = parse_location(raw_result["silver_version_path"])
    part_paths = silver_part_paths(version_path)
    if not part_paths:
        raise ValueError(f"Silver part 파일이 없습니다: {version_path}")
    parquet_files = [parquet_file(path) for path in part_paths]
    expected_signature = _schema_signature(
        SILVER_SCHEMA, logical_timestamp=True
    )
    actual_signatures = {
        _schema_signature(file.schema_arrow, logical_timestamp=True)
        for file in parquet_files
    }
    if actual_signatures != {expected_signature}:
        raise ValueError(
            "Silver 스키마가 계약과 다릅니다: "
            f"expected={expected_signature} actual={sorted(actual_signatures)}"
        )
    silver_rows = sum(file.metadata.num_rows for file in parquet_files)
    if silver_rows < 1:
        raise ValueError("Silver 레코드가 0건입니다")
    recon = _reconcile_silver(version_path, bronze_rows, silver_rows)
    total = int(recon.get("total") or bronze_rows)
    invalid = int(recon["invalid"])
    invalid_ratio = invalid / total if total else 0.0
    warning_threshold = float(
        recon.get("warning_threshold", MONTHLY_TAXI_TRIP_WARNING_THRESHOLD)
    )
    if invalid_ratio >= warning_threshold:
        send_quality_warning(
            context or {},
            dataset="monthly_taxi_trip",
            year_month=raw_result["year_month"],
            invalid_rows=invalid,
            row_count=total,
            invalid_ratio=invalid_ratio,
            extra_columns=list(recon.get("extra_columns") or []),
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
