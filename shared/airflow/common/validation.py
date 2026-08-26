from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
import io
import json
import logging
import math
import mimetypes
import os
from pathlib import Path, PurePosixPath
import tempfile
from uuid import uuid4

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from shared.airflow.common.slack_quality_warning import send_gx_quality_warning
from shared.common.s3_reader import get_object_bytes, get_object_stream
from shared.common.success_marker import (
    marker_key,
    marker_path,
    quarantine_marker_key,
    quarantine_marker_path,
)


logger = logging.getLogger(__name__)

# GX 는 import 시점에 문서용 레지스트리를 만들며 등록 못 한 함수를 INFO 로 209줄 남깁니다.
# 억제는 그 import 보다 먼저 걸려야 효과가 있는데, DAG 들이 GX 를 태스크 본문에서 직접
# import 하므로 여기(모듈 최상단)가 확실히 앞섭니다 — DAG 는 이 모듈을 최상단에서 씁니다.
logging.getLogger("great_expectations").setLevel(logging.WARNING)

_CONTAINER_ROOT = Path("/opt/airflow/project-root")
_PROJECT_ROOT = (
    _CONTAINER_ROOT
    if _CONTAINER_ROOT.exists()
    else Path(__file__).resolve().parents[3]
)
_DEFAULT_DATA_DOCS_DIR = _PROJECT_ROOT / "data" / "gx_data_docs"


S3_SCHEME = "s3://"
REQUIRED_NULL_WARNING_RATIO = 0.01
REQUIRED_NULL_ERROR_RATIO = 0.05


@dataclass(frozen=True)
class S3Location:
    """S3 URI 를 보존하는 위치 타입.

    `Path("s3://bucket/key")` 는 중복 슬래시가 접혀 `s3:/bucket/key` 가 됩니다.
    그 상태로 `is_file()` 을 부르면 로컬 파일시스템을 조회해 항상 실패합니다.
    Path 와 같은 `.name`/`.stem`/`.suffix` 를 제공해 호출부를 그대로 둡니다.
    """

    bucket: str
    key: str

    def __str__(self) -> str:
        return f"{S3_SCHEME}{self.bucket}/{self.key}"

    @property
    def name(self) -> str:
        return PurePosixPath(self.key).name

    @property
    def stem(self) -> str:
        return PurePosixPath(self.key).stem

    @property
    def suffix(self) -> str:
        return PurePosixPath(self.key).suffix

    @property
    def parent(self) -> "S3Location":
        """상위 prefix. `*_from_partition(path.parent)` 이 `.name` 만 쓰므로 맞춰줍니다."""
        parent = PurePosixPath(self.key).parent
        if str(parent) in (".", "/"):
            raise ValueError(f"상위 prefix 가 없습니다: {self}")
        return S3Location(self.bucket, str(parent))

    def size(self) -> int:
        """객체 크기. 로컬의 `path.stat().st_size` 자리에 씁니다."""
        stream, length = get_object_stream(self.bucket, self.key)
        stream.close()
        return length


def parse_location(value: str) -> Path | S3Location:
    if not value.startswith(S3_SCHEME):
        return Path(value)
    bucket, _, key = value[len(S3_SCHEME):].partition("/")
    if not bucket or not key:
        raise ValueError(f"S3 URI 형식이 아닙니다: {value}")
    return S3Location(bucket, key)


def gx_data_docs_location(
    data_location: S3Location, *, layer: str, dataset: str
) -> S3Location:
    """실제 S3 파티션을 `logs/gx-data-docs` 아래에 그대로 미러링합니다."""
    parts = PurePosixPath(data_location.key).parts
    try:
        layer_index = next(
            index
            for index in range(len(parts) - 1)
            if parts[index:index + 2] == (layer, dataset)
        )
    except StopIteration as exc:
        raise ValueError(
            f"S3 데이터 경로에 {layer}/{dataset} 계층이 없습니다: {data_location}"
        ) from exc
    partitions = parts[layer_index + 2:-1]
    if not partitions:
        raise ValueError(f"S3 데이터 경로에 버전 파티션이 없습니다: {data_location}")
    key = PurePosixPath(
        "logs", "gx-data-docs", layer, dataset, *partitions
    ).as_posix()
    return S3Location(data_location.bucket, key)


def location_size(location: Path | S3Location) -> int:
    """로컬은 stat(), S3 는 ContentLength. 호출부가 분기하지 않게 감쌉니다."""
    if isinstance(location, S3Location):
        return location.size()
    return location.stat().st_size


def layout_tail(
    location: Path | S3Location | str,
    segments: int = 3,
    service_area: str | None = None,
) -> str:
    """파티션과 파일명만 잘라냅니다.

    layout 규칙 비교에 씁니다. 로컬은 base_dir, S3 는 bucket/prefix 로 앞부분이
    달라서 절대경로끼리 비교할 수 없지만, 규칙 위반은 뒤쪽 파티션 경로에서 드러납니다.

    `service_area` 를 주면 세그먼트 수를 **하나 늘립니다**. 지역 계층(#674)이 들어가면
    `<dataset>/year_month=<ym>/<file>` 이던 tail 이
    `service_area=<sa>/year_month=<ym>/<file>` 로 밀려 **데이터셋명이 빠집니다** —
    비교하는 두 경로가 같은 빌더로 만들어지니 통과는 하지만, 검사가 조용히 약해집니다.
    지역을 넘겨 데이터셋명을 계속 포함시킵니다.

    ⚠️ 기대 경로를 만드는 빌더(`silver_file` 등)와 **같은 `service_area` 를** 넘겨야
    합니다. 한쪽만 넘기면 tail 길이가 어긋나 항상 실패합니다.
    """
    if service_area is not None:
        segments += 1
    parts = str(location).replace("\\", "/").rstrip("/").split("/")
    return "/".join(parts[-segments:])


@dataclass(frozen=True)
class HandlerResult:
    row_count: int
    locations: tuple[Path | S3Location, ...]


def parse_handler_result(
    result: object,
    *,
    expected_locations: int | None = None,
    expected_rows: int | None = None,
) -> HandlerResult:
    if not isinstance(result, dict):
        raise TypeError("Handler 결과가 dict가 아닙니다.")

    row_count = result.get("row_count")
    # bool은 int의 하위 타입이므로 명시적으로 제외합니다.
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count <= 0:
        raise ValueError("row_count는 1 이상의 정수여야 합니다.")
    if expected_rows is not None and row_count != expected_rows:
        raise ValueError(f"row_count는 {expected_rows}이어야 합니다.")

    raw_locations = result.get("locations")
    if not isinstance(raw_locations, list) or not raw_locations:
        raise ValueError("locations는 비어 있지 않은 경로 목록이어야 합니다.")
    if not all(isinstance(value, str) and value for value in raw_locations):
        raise ValueError("locations에는 빈 경로가 없어야 합니다.")
    if expected_locations is not None and len(raw_locations) != expected_locations:
        raise ValueError(f"locations에는 경로가 {expected_locations}개 있어야 합니다.")

    return HandlerResult(row_count, tuple(map(parse_location, raw_locations)))


def parse_iso_date(value: object, field: str = "collected_date") -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field}는 문자열이어야 합니다.")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field}는 YYYY-MM-DD 형식이어야 합니다.") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field}는 YYYY-MM-DD 형식이어야 합니다.")
    return parsed


def parse_year_month(value: object, field: str = "collected_month") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field}는 문자열이어야 합니다.")
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ValueError(f"{field}는 YYYY-MM 형식이어야 합니다.") from exc
    if parsed.strftime("%Y-%m") != value:
        raise ValueError(f"{field}는 YYYY-MM 형식이어야 합니다.")
    return value


def require_file(path: Path | S3Location) -> Path | S3Location:
    if isinstance(path, S3Location):
        try:
            stream, size = get_object_stream(path.bucket, path.key)
        except Exception as exc:  # NoSuchKey 등 — 메시지를 로컬 경로와 같게 맞춥니다.
            raise FileNotFoundError(f"적재 파일이 없습니다: {path}") from exc
        stream.close()
        if size == 0:
            raise ValueError(f"적재 파일이 비어 있습니다: {path}")
        return path
    if not path.is_file():
        raise FileNotFoundError(f"적재 파일이 없습니다: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"적재 파일이 비어 있습니다: {path}")
    return path


def publish_success_marker(directory: Path | S3Location) -> None:
    """격리 상태를 지우고 검증이 끝난 디렉터리를 공개합니다."""
    if isinstance(directory, S3Location):
        client = boto3.client("s3")
        client.delete_object(
            Bucket=directory.bucket,
            Key=quarantine_marker_key(directory.key),
        )
        client.put_object(
            Bucket=directory.bucket,
            Key=marker_key(directory.key),
            Body=b"",
        )
        return
    Path(directory).mkdir(parents=True, exist_ok=True)
    quarantine_marker_path(directory).unlink(missing_ok=True)
    marker_path(directory).touch()


def publish_quarantine_marker(
    directory: Path | S3Location,
    *,
    run_id: str,
    layer: str,
    reason: str,
    retryable: bool = False,
    failed_at: datetime | None = None,
) -> None:
    """성공 상태를 지우고 품질 실패 원인을 JSON 종결 상태로 기록합니다."""
    timestamp = failed_at or datetime.now(timezone.utc)
    payload = json.dumps(
        {
            "failed_at": timestamp.isoformat(),
            "layer": layer,
            "reason": reason,
            "retryable": retryable,
            "run_id": run_id,
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    if isinstance(directory, S3Location):
        client = boto3.client("s3")
        client.delete_object(
            Bucket=directory.bucket,
            Key=marker_key(directory.key),
        )
        client.put_object(
            Bucket=directory.bucket,
            Key=quarantine_marker_key(directory.key),
            Body=payload,
        )
        return

    Path(directory).mkdir(parents=True, exist_ok=True)
    marker_path(directory).unlink(missing_ok=True)
    quarantine = quarantine_marker_path(directory)
    temporary = quarantine.with_name(f".{quarantine.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(payload)
        temporary.replace(quarantine)
    finally:
        temporary.unlink(missing_ok=True)


def _run_id(context: dict) -> str:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    value = (
        context.get("run_id")
        or getattr(dag_run, "run_id", None)
        or getattr(task_instance, "run_id", None)
    )
    return str(value or "unknown")


def run_quality_gate(
    directory: Path | S3Location | Callable[[], Path | S3Location],
    validator: Callable[[], object],
    *,
    layer: str,
    context: dict,
):
    """검증 결과를 상호 배타적인 성공·격리 marker로 전환합니다."""
    try:
        result = validator()
    except Exception as exc:
        target = directory() if callable(directory) else directory
        try:
            publish_quarantine_marker(
                target,
                run_id=_run_id(context),
                layer=layer,
                reason=f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            logger.exception("품질 격리 marker 기록 실패: %s", target)
        raise

    target = directory() if callable(directory) else directory
    publish_success_marker(target)
    return result


def require_success_marker(directory: Path | S3Location) -> None:
    if isinstance(directory, S3Location):
        key = marker_key(directory.key)
        try:
            stream, _ = get_object_stream(directory.bucket, key)
        except Exception as exc:
            raise FileNotFoundError(
                f"완료 marker가 없습니다: s3://{directory.bucket}/{key}"
            ) from exc
        stream.close()
        return
    marker = marker_path(directory)
    if not marker.is_file():
        raise FileNotFoundError(f"완료 marker가 없습니다: {marker}")


def parquet_file(path: Path | S3Location) -> pq.ParquetFile:
    require_file(path)
    if path.suffix != ".parquet":
        raise ValueError(f"Parquet 파일이 아닙니다: {path}")
    try:
        if isinstance(path, S3Location):
            return pq.ParquetFile(
                io.BytesIO(get_object_bytes(path.bucket, path.key))
            )
        return pq.ParquetFile(path)
    except (OSError, pa.ArrowInvalid) as exc:
        raise RuntimeError(f"Parquet 파일을 읽지 못했습니다: {path}") from exc


def read_parquet(path: Path | S3Location) -> pa.Table:
    try:
        return parquet_file(path).read()
    except FileNotFoundError:
        raise
    except (OSError, pa.ArrowInvalid) as exc:
        raise RuntimeError(f"Parquet 파일을 읽지 못했습니다: {path}") from exc


def _failure_column(failure) -> str:
    kwargs = failure.expectation_config.kwargs
    expectation_type = failure.expectation_config.type
    return (
        kwargs.get("column")
        or (
            "/".join(map(str, kwargs.get("column_list") or []))
            if expectation_type == "expect_compound_columns_to_be_unique"
            else ""
        )
        or "/".join(
            str(column)
            for column in (kwargs.get("column_A"), kwargs.get("column_B"))
            if column
        )
        or "table"
    )


def _data_docs_config(root: Path):
    from great_expectations.data_context.types.base import DataContextConfig

    stores = {
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(root / ".gx_store" / "expectations"),
            },
        },
        "validation_results_store": {
            "class_name": "ValidationResultsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(root / ".gx_store" / "validations"),
            },
        },
        # Runtime DataFrame의 datasource ID는 다음 Context에서 복원할 수 없습니다.
        "validation_definition_store": {
            "class_name": "ValidationDefinitionStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
    }
    data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(root),
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        }
    }
    return DataContextConfig(
        config_version=4,
        expectations_store_name="expectations_store",
        validation_results_store_name="validation_results_store",
        checkpoint_store_name="checkpoint_store",
        stores=stores,
        data_docs_sites=data_docs_sites,
        analytics_enabled=False,
    )


class _DataDocsLock:
    def __init__(self, root: Path):
        self._root = root
        self._handle = None

    def __enter__(self):
        import fcntl

        self._root.mkdir(parents=True, exist_ok=True)
        self._handle = (self._root / ".build.lock").open("a+")
        fcntl.flock(self._handle, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        import fcntl

        assert self._handle is not None
        fcntl.flock(self._handle, fcntl.LOCK_UN)
        self._handle.close()


_GX_REQUIRED_RECORD_VALID = "__gx_required_record_valid"
_GX_EXTRA_COLUMNS = "__gx_extra_columns"


def table_quality_summary(
    table: pa.Table,
    expected_schema: pa.Schema,
    required_non_null: set[str] | frozenset[str],
):
    """작은 Arrow 적재 결과를 관측용 한 행 지표로 요약합니다."""
    import pandas as pd

    expected_names = set(expected_schema.names)
    unknown_required = set(required_non_null) - expected_names
    if unknown_required:
        raise ValueError(f"기대 스키마에 없는 필수 컬럼: {sorted(unknown_required)}")

    actual = {field.name: field.type for field in table.schema}
    extra = sorted(set(actual) - expected_names)
    missing = sorted(set(required_non_null) - set(actual))
    mismatched = sorted(
        f"{field.name}:{actual[field.name]}!={field.type}"
        for field in expected_schema
        if field.name in required_non_null
        and field.name in actual
        and actual[field.name] != field.type
    )
    structurally_valid = not missing and not mismatched

    invalid_count = 0
    if table.num_rows and structurally_valid:
        invalid = pa.array([False] * table.num_rows)
        for name in sorted(required_non_null):
            values = table[name].combine_chunks()
            column_invalid = pc.is_null(values)
            if pa.types.is_string(values.type):
                column_invalid = pc.or_(
                    column_invalid,
                    pc.fill_null(
                        pc.equal(pc.utf8_trim_whitespace(values), ""),
                        False,
                    ),
                )
            elif pa.types.is_floating(values.type):
                column_invalid = pc.or_(
                    column_invalid,
                    pc.fill_null(pc.is_nan(values), False),
                )
            invalid = pc.or_(invalid, column_invalid)
        invalid_count = int(pc.sum(pc.cast(invalid, pa.int64())).as_py() or 0)

    return pd.DataFrame(
        [{
            "row_count": table.num_rows,
            "extra_columns": ",".join(extra),
            "missing_columns": ",".join(missing),
            "type_mismatch_columns": ",".join(mismatched),
            "required_invalid_record_count": invalid_count,
            "required_invalid_record_ratio": (
                invalid_count / table.num_rows
                if table.num_rows and structurally_valid
                else None
            ),
        }]
    )


def _minimum_valid_ratio(max_invalid_ratio: float) -> float:
    if max_invalid_ratio == 0:
        return 1.0
    return math.nextafter(1 - max_invalid_ratio, 1.0)


def _required_record_validity(
    table: pa.Table, required_non_null: set[str] | frozenset[str]
) -> pa.Array:
    valid = pa.array([True] * table.num_rows)
    for name in sorted(required_non_null):
        values = table[name].combine_chunks()
        invalid = pc.is_null(values)
        if pa.types.is_string(values.type):
            invalid = pc.or_(
                invalid,
                pc.fill_null(
                    pc.equal(pc.utf8_trim_whitespace(values), ""),
                    False,
                ),
            )
        elif pa.types.is_floating(values.type):
            invalid = pc.or_(
                invalid,
                pc.fill_null(pc.is_nan(values), False),
            )
        valid = pc.and_(valid, pc.invert(invalid))
    return valid


def run_table_gx_validation(
    table: pa.Table,
    expected_schema: pa.Schema,
    required_non_null: set[str] | frozenset[str],
    *,
    dataset: str,
    layer: str,
    data_location: S3Location,
    context: dict,
    required_warning_ratio: float | None,
    required_error_ratio: float,
    record_extra_columns: bool = False,
) -> None:
    """운영 S3의 작은 Parquet 전체 레코드에 공통 품질 정책을 적용합니다.

    ``table_quality_summary`` 는 로그용 관측 지표일 뿐 GX 판정 입력이 아닙니다.
    GX에는 실제 Silver 레코드와 레코드별 필수값 판정 컬럼을 전달합니다.
    """
    import great_expectations as gx

    if not 0 <= required_error_ratio <= 1 or (
        required_warning_ratio is not None
        and not 0 <= required_warning_ratio < required_error_ratio
    ):
        raise ValueError(
            "필수값 임계치는 0 <= warning < error <= 1 이어야 합니다"
        )

    expected_names = set(expected_schema.names)
    unknown_required = set(required_non_null) - expected_names
    if unknown_required:
        raise ValueError(f"기대 스키마에 없는 필수 컬럼: {sorted(unknown_required)}")

    actual = {field.name: field.type for field in table.schema}
    reserved = {_GX_REQUIRED_RECORD_VALID, _GX_EXTRA_COLUMNS} & set(actual)
    if reserved:
        raise ValueError(f"GX 예약 컬럼을 원본에서 사용할 수 없습니다: {sorted(reserved)}")
    missing = sorted(set(required_non_null) - set(actual))
    if missing:
        raise ValueError(f"missing_columns={','.join(missing)}")
    mismatched = sorted(
        f"{field.name}:{actual[field.name]}!={field.type}"
        for field in expected_schema
        if field.name in required_non_null
        and field.name in actual
        and actual[field.name] != field.type
    )
    if mismatched:
        raise ValueError(f"type_mismatch_columns={','.join(mismatched)}")

    summary = table_quality_summary(table, expected_schema, required_non_null)
    logger.info(
        "table_quality_summary dataset=%s layer=%s metrics=%s",
        dataset,
        layer,
        summary.to_json(orient="records"),
    )

    frame = table.append_column(
        _GX_REQUIRED_RECORD_VALID,
        _required_record_validity(table, required_non_null),
    ).to_pandas()
    extra = sorted(set(actual) - expected_names)
    frame[_GX_EXTRA_COLUMNS] = ""
    if len(frame) and extra:
        frame.loc[frame.index[0], _GX_EXTRA_COLUMNS] = ",".join(extra)

    error_mostly = _minimum_valid_ratio(required_error_ratio)
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        *(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column,
                mostly=error_mostly,
            )
            for column in sorted(required_non_null)
        ),
    ]
    if record_extra_columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=_GX_EXTRA_COLUMNS,
                value_set=[""],
                meta={"severity": "warning", "notify": False},
            )
        )
    if required_warning_ratio is not None:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=_GX_REQUIRED_RECORD_VALID,
                value_set=[True],
                mostly=_minimum_valid_ratio(required_warning_ratio),
                meta={"severity": "warning"},
            )
        )
    expectations.append(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column=_GX_REQUIRED_RECORD_VALID,
            value_set=[True],
            mostly=error_mostly,
        )
    )
    docs = gx_data_docs_location(data_location, layer=layer, dataset=dataset)
    warnings = run_gx_validation(
        frame,
        expectations,
        suite_name=f"{dataset}_{layer}_suite",
        layer=layer,
        data_docs_s3_location=docs,
    )
    if warnings:
        send_gx_quality_warning(
            context,
            dataset=dataset,
            layer=layer,
            partition=docs.name,
            warnings=warnings,
        )


def run_file_gx_validation(
    *,
    size_bytes: int,
    minimum_bytes: int,
    dataset: str,
    layer: str,
    data_location: S3Location,
) -> None:
    """Parquet이 아닌 운영 원본은 파일 크기 지표를 GX로 판정합니다."""
    import great_expectations as gx
    import pandas as pd

    run_gx_validation(
        pd.DataFrame({"size_bytes": [size_bytes]}),
        [
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="size_bytes", min_value=minimum_bytes
            )
        ],
        suite_name=f"{dataset}_{layer}_suite",
        layer=layer,
        data_docs_s3_location=gx_data_docs_location(
            data_location, layer=layer, dataset=dataset
        ),
    )


def _upload_data_docs(root: Path, target: S3Location) -> None:
    client = boto3.client("s3")
    paths = sorted(
        root.rglob("*"),
        key=lambda path: path.relative_to(root).as_posix() == "index.html",
    )
    for path in paths:
        relative_path = path.relative_to(root)
        if (
            not path.is_file()
            or path.name == ".build.lock"
            or ".gx_store" in relative_path.parts
        ):
            continue
        relative = relative_path.as_posix()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        client.upload_file(
            str(path),
            target.bucket,
            f"{target.key}/{relative}",
            ExtraArgs={"ContentType": content_type},
        )


def run_gx_validation(
    dataframe,
    expectations,
    *,
    suite_name: str,
    layer: str,
    data_docs_dir: str | Path | None = None,
    data_docs_s3_location: S3Location | None = None,
) -> tuple[str, ...]:
    """GX Suite를 실행하고 결과 로그와 정적 Data Docs를 공통 발행합니다.

    Suite 설정은 파일 저장을 거치므로 JSON으로 왕복 가능한 값을 사용합니다.
    특히 ``InSet``의 날짜 값은 DataFrame과 Expectation 모두 ISO 문자열로 맞춥니다.
    """
    # DAG import 단계에서는 GX를 불러오지 않고 Validation Task 실행 시점에만 사용합니다.
    # (로그 억제는 모듈 최상단에서 이미 걸었습니다 — 여기서 걸면 import 보다 늦습니다.)
    import great_expectations as gx

    configured_dir = os.getenv("GX_DATA_DOCS_DIR")
    env_docs_enabled = os.getenv("GX_DATA_DOCS_ENABLED", "true").lower() not in {
        "0",
        "false",
        "no",
    }
    docs_enabled = data_docs_dir is not None or env_docs_enabled
    if data_docs_s3_location is not None:
        docs_enabled = env_docs_enabled
    temporary_docs = (
        tempfile.TemporaryDirectory()
        if docs_enabled and data_docs_s3_location is not None
        else None
    )
    docs_root = None
    if docs_enabled:
        docs_root = (
            Path(temporary_docs.name)
            if temporary_docs is not None
            else Path(
                data_docs_dir or configured_dir or _DEFAULT_DATA_DOCS_DIR
            ).resolve()
        )

    try:
        with _DataDocsLock(docs_root) if docs_root else nullcontext():
            context = gx.get_context(
                mode="ephemeral",
                project_config=(
                    _data_docs_config(docs_root) if docs_root is not None else None
                ),
            )
            context.variables.progress_bars = {"globally": False}

            name_prefix = suite_name.removesuffix("_suite")
            batch_definition = (
                context.data_sources.add_pandas(name=f"{name_prefix}_source")
                .add_dataframe_asset(name=f"{name_prefix}_asset")
                .add_batch_definition_whole_dataframe(f"{name_prefix}_batch")
            )
            suite = context.suites.add_or_update(
                gx.ExpectationSuite(name=suite_name, expectations=expectations)
            )
            validation = gx.ValidationDefinition(
                name=f"{name_prefix}_validation",
                data=batch_definition,
                suite=suite,
            ).run(
                batch_parameters={"dataframe": dataframe},
                result_format="SUMMARY",
            )

            failed_results = [
                result for result in validation.results if not result.success
            ]
            warning_failures = [
                result
                for result in failed_results
                if result.expectation_config.meta.get("severity") == "warning"
            ]
            failures = [
                result
                for result in failed_results
                if result not in warning_failures
            ]
            warning_messages = []
            for failure in failed_results:
                result = dict(failure.result)
                observed_value = result.get("observed_value")
                if observed_value is None:
                    observed_value = result.get("partial_unexpected_list")
                if observed_value is None:
                    observed_value = "unavailable"
                is_warning = failure in warning_failures
                log = logger.warning if is_warning else logger.error
                column = _failure_column(failure)
                log(
                    "gx_validation %s layer=%s expectation=%s column=%s "
                    "unexpected_count=%s observed_value=%s",
                    "warning" if is_warning else "failed",
                    layer,
                    failure.expectation_config.type,
                    column,
                    result.get("unexpected_count"),
                    observed_value,
                )
                if is_warning and failure.expectation_config.meta.get(
                    "notify", True
                ):
                    warning_messages.append(
                        f"{failure.expectation_config.type}[{column}]={observed_value}"
                    )

            if docs_root is not None:
                try:
                    context.build_data_docs(site_names=["local_site"])
                    if data_docs_s3_location is not None:
                        _upload_data_docs(docs_root, data_docs_s3_location)
                        logger.info(
                            "gx_data_docs updated path=%s",
                            data_docs_s3_location,
                        )
                    else:
                        logger.info(
                            "gx_data_docs updated path=%s",
                            docs_root / "index.html",
                        )
                except Exception:
                    if not failures:
                        raise
                    logger.exception("gx_data_docs build failed path=%s", docs_root)

            if failures:
                rules = ", ".join(
                    f"{failure.expectation_config.type}"
                    f"[{_failure_column(failure)}]"
                    for failure in failures
                )
                raise ValueError(f"GX 검증 실패 layer={layer}: {rules}")

            logger.info(
                "gx_validation passed layer=%s expectations=%s warnings=%s",
                layer,
                validation.statistics["evaluated_expectations"],
                len(warning_failures),
            )
            return tuple(warning_messages)
    finally:
        if temporary_docs is not None:
            temporary_docs.cleanup()
