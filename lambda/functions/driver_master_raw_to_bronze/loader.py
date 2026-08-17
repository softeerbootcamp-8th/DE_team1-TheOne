"""기사 데이터 원본 Parquet을 Bronze에 보존합니다."""

from ..common.synthetic_release import SyntheticReleaseDatasetLoader
from .source_snapshot import DATASET


class CompanySnapshotBronzeLoader(SyntheticReleaseDatasetLoader):
    def __init__(self, base_dir: str):
        super().__init__(base_dir, DATASET, "driver_vehicle_leases")
