"""기사 데이터 Parquet을 데이터 제공 경로에서 수집합니다."""

from ..common.synthetic_release import SyntheticReleaseDatasetExtractor


DATASET = "driver_vehicle_leases"


class CompanySnapshotExtractor(SyntheticReleaseDatasetExtractor):
    """기존 기사 데이터 수집 진입점을 새 제공 경로로 교체합니다."""

    def __init__(
        self,
        api_base_url: str,
        year_month: str | None = None,
        *,
        timeout: int = 180,
    ):
        super().__init__(api_base_url, DATASET, year_month, timeout=timeout)
