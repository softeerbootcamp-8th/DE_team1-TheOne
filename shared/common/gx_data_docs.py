"""GX Data Docs의 경로·정적 파일 발행 규칙."""

import mimetypes
from pathlib import Path, PurePosixPath

import boto3


GX_VALIDATION_SUMMARY_FILE_NAME = "_GX_VALIDATION.json"


def mirrored_data_docs_prefix(
    data_key: str,
    *,
    layer: str,
    dataset: str,
    data_is_file: bool,
) -> str:
    """데이터 파티션 경로를 ``logs/gx-data-docs`` 아래에 그대로 미러링합니다."""
    parts = PurePosixPath(data_key).parts
    try:
        layer_index = next(
            index
            for index in range(len(parts) - 1)
            if parts[index:index + 2] == (layer, dataset)
        )
    except StopIteration as exc:
        raise ValueError(
            f"S3 데이터 경로에 {layer}/{dataset} 계층이 없습니다: {data_key}"
        ) from exc

    end = -1 if data_is_file else None
    partitions = parts[layer_index + 2:end]
    if not partitions:
        raise ValueError(f"S3 데이터 경로에 버전 파티션이 없습니다: {data_key}")
    return PurePosixPath(
        "logs", "gx-data-docs", layer, dataset, *partitions
    ).as_posix()


def data_docs_config(root: Path):
    """현재 validation 결과로 정적 Data Docs를 만들 GX context 설정."""
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
        # Runtime DataFrame datasource는 다음 Context에서 복원할 수 없습니다.
        "validation_definition_store": {
            "class_name": "ValidationDefinitionStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {"class_name": "InMemoryStoreBackend"},
        },
    }
    return DataContextConfig(
        config_version=4,
        expectations_store_name="expectations_store",
        validation_results_store_name="validation_results_store",
        checkpoint_store_name="checkpoint_store",
        stores=stores,
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": False,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": str(root),
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        analytics_enabled=False,
    )


def upload_data_docs(root: Path, *, bucket: str, prefix: str) -> None:
    """정적 문서를 S3에 올리고 새 index를 마지막에 공개합니다."""
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
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        client.upload_file(
            str(path),
            bucket,
            f"{prefix.rstrip('/')}/{relative_path.as_posix()}",
            ExtraArgs={"ContentType": content_type},
        )
