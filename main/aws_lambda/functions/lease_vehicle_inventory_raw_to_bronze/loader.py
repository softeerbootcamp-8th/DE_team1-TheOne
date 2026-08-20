"""보유 차량 원본 Parquet을 Bronze에 보존합니다."""

from main.aws_lambda.common.monthly_dataset import MonthlyParquetBronzeLoader
from .extractor import DATASET


class LeaseVehicleInventoryBronzeLoader(MonthlyParquetBronzeLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, DATASET)
