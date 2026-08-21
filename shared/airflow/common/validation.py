from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime
import io
import logging
import os
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq

from shared.common.s3_reader import get_object_bytes, get_object_stream


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


def location_size(location: Path | S3Location) -> int:
    """로컬은 stat(), S3 는 ContentLength. 호출부가 분기하지 않게 감쌉니다."""
    if isinstance(location, S3Location):
        return location.size()
    return location.stat().st_size


def layout_tail(location: Path | S3Location | str, segments: int = 3) -> str:
    """파티션과 파일명만 잘라냅니다.

    layout 규칙 비교에 씁니다. 로컬은 base_dir, S3 는 bucket/prefix 로 앞부분이
    달라서 절대경로끼리 비교할 수 없지만, 규칙 위반은 뒤쪽 파티션 경로에서 드러납니다.
    """
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


def read_parquet(path: Path | S3Location) -> pa.Table:
    require_file(path)
    if path.suffix != ".parquet":
        raise ValueError(f"Parquet 파일이 아닙니다: {path}")
    try:
        if isinstance(path, S3Location):
            return pq.ParquetFile(
                io.BytesIO(get_object_bytes(path.bucket, path.key))
            ).read()
        return pq.ParquetFile(path).read()
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


def run_gx_validation(
    dataframe,
    expectations,
    *,
    suite_name: str,
    layer: str,
    data_docs_dir: str | Path | None = None,
) -> None:
    """GX Suite를 실행하고 결과 로그와 정적 Data Docs를 공통 발행합니다.

    Suite 설정은 파일 저장을 거치므로 JSON으로 왕복 가능한 값을 사용합니다.
    특히 ``InSet``의 날짜 값은 DataFrame과 Expectation 모두 ISO 문자열로 맞춥니다.
    """
    # DAG import 단계에서는 GX를 불러오지 않고 Validation Task 실행 시점에만 사용합니다.
    # (로그 억제는 모듈 최상단에서 이미 걸었습니다 — 여기서 걸면 import 보다 늦습니다.)
    import great_expectations as gx

    configured_dir = os.getenv("GX_DATA_DOCS_DIR")
    docs_enabled = data_docs_dir is not None or os.getenv(
        "GX_DATA_DOCS_ENABLED", "true"
    ).lower() not in {"0", "false", "no"}
    docs_root = None
    if docs_enabled:
        docs_root = Path(
            data_docs_dir or configured_dir or _DEFAULT_DATA_DOCS_DIR
        ).resolve()

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
            result for result in failed_results if result not in warning_failures
        ]
        for failure in failed_results:
            result = dict(failure.result)
            observed_value = result.get("observed_value")
            if observed_value is None:
                observed_value = result.get("partial_unexpected_list")
            if observed_value is None:
                observed_value = "unavailable"
            is_warning = failure in warning_failures
            log = logger.warning if is_warning else logger.error
            log(
                "gx_validation %s layer=%s expectation=%s column=%s "
                "unexpected_count=%s observed_value=%s",
                "warning" if is_warning else "failed",
                layer,
                failure.expectation_config.type,
                _failure_column(failure),
                result.get("unexpected_count"),
                observed_value,
            )

        if docs_root is not None:
            try:
                context.build_data_docs(site_names=["local_site"])
                logger.info("gx_data_docs updated path=%s", docs_root / "index.html")
            except Exception:
                if not failures:
                    raise
                logger.exception("gx_data_docs build failed path=%s", docs_root)

        if failures:
            rules = ", ".join(
                f"{failure.expectation_config.type}[{_failure_column(failure)}]"
                for failure in failures
            )
            raise ValueError(f"GX 검증 실패 layer={layer}: {rules}")

        logger.info(
            "gx_validation passed layer=%s expectations=%s warnings=%s",
            layer,
            validation.statistics["evaluated_expectations"],
            len(warning_failures),
        )
