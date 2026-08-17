"""HVFHV+taxi_id Parquet을 Bronze에 보존합니다."""

from ..common.monthly_dataset import SyntheticDatasetLoader
from .extractor import DATASET


class HvfhvBronzeLoader(SyntheticDatasetLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, "hvfhv")
