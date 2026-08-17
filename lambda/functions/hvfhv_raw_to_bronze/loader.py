"""HVFHV+taxi_id Parquet을 Bronze에 보존합니다."""

from ..common.synthetic_release import SyntheticReleaseDatasetLoader
from .extractor import DATASET


class HvfhvBronzeLoader(SyntheticReleaseDatasetLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, "hvfhv")
