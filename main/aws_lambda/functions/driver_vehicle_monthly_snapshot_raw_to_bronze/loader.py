"""기사 차량 월별 스냅샷 원본 Parquet을 Bronze에 보존합니다."""

from main.aws_lambda.common.monthly_dataset import SyntheticDatasetLoader
from .extractor import DATASET


class DriverVehicleMonthlySnapshotBronzeLoader(SyntheticDatasetLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, DATASET)
