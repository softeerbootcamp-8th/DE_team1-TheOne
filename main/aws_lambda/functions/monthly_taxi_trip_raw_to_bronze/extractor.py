"""월별 택시 운행 Parquet을 데이터 제공 경로에서 수집합니다."""

from main.aws_lambda.common.monthly_dataset import MonthlyParquetAPIExtractor


DATASET = "monthly_taxi_trip"


class MonthlyTaxiTripExtractor(MonthlyParquetAPIExtractor):
    def __init__(
        self,
        api_base_url: str,
        year_month: str | None = None,
        *,
        service_area: str | None = None,
        timeout: int = 180,
    ):
        super().__init__(api_base_url, DATASET, year_month, service_area=service_area,
                         timeout=timeout)
