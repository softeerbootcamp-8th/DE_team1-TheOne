"""보유 차량 Parquet을 데이터 제공 경로에서 수집합니다."""

from main.aws_lambda.common.monthly_dataset import SyntheticDatasetExtractor


DATASET = "lease_vehicle_inventory"


class LeaseVehicleInventoryExtractor(SyntheticDatasetExtractor):
    def __init__(
        self,
        api_base_url: str,
        year_month: str | None = None,
        *,
        timeout: int = 180,
    ):
        super().__init__(api_base_url, DATASET, year_month, timeout=timeout)
