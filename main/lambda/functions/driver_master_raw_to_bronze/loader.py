"""기사 데이터 원본 Parquet을 Bronze에 보존합니다."""

from shared.lambda_runtime.common.monthly_dataset import SyntheticDatasetLoader
from .source_snapshot import DATASET


class CompanySnapshotBronzeLoader(SyntheticDatasetLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, "driver_vehicle_leases")
