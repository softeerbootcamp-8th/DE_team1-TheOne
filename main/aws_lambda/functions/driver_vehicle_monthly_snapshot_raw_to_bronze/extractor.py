"""기사 차량 월별 스냅샷 Parquet을 데이터 제공 경로에서 수집합니다."""

from main.aws_lambda.common.monthly_dataset import SyntheticDatasetExtractor


DATASET = "driver_vehicle_monthly_snapshot"


class DriverVehicleMonthlySnapshotExtractor(SyntheticDatasetExtractor):
    def __init__(
        self,
        api_base_url: str,
        year_month: str | None = None,
        *,
        timeout: int = 180,
    ):
        super().__init__(api_base_url, DATASET, year_month, timeout=timeout)
