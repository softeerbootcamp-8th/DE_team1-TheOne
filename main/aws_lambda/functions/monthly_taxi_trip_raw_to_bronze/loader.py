"""HVFHV+taxi_id Parquet을 Bronze에 보존합니다."""

from pipeline_core.loader import Loader

from main.aws_lambda.common.monthly_dataset import build_bronze_loader
from .extractor import DATASET

DATASET_DIR = "hvfhv"


def build_loader(
    storage: str,
    base_dir: str,
    bucket: str | None = None,
    *,
    dry_run: bool = False,
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    return build_bronze_loader(
        storage,
        base_dir,
        DATASET,
        DATASET_DIR,
        bucket=bucket,
        dry_run=dry_run,
    )
