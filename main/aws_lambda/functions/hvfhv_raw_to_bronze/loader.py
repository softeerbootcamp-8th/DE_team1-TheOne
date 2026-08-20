"""HVFHV+taxi_id Parquet을 Bronze에 보존합니다."""

from main.aws_lambda.common.monthly_dataset import MonthlyParquetBronzeLoader
from .extractor import DATASET


class HvfhvBronzeLoader(MonthlyParquetBronzeLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, "hvfhv")
