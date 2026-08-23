"""보유 차량 원본 Parquet을 Bronze에 보존합니다."""

from pipeline_core.loader import Loader

from main.aws_lambda.common.monthly_dataset import build_bronze_loader
from .extractor import DATASET


def build_loader(
    storage: str,
    base_dir: str,
    bucket: str | None = None,
    service_area: str | None = None,
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    return build_bronze_loader(
        storage,
        base_dir,
        DATASET,
        DATASET,
        bucket=bucket,
        service_area=service_area,
    )
