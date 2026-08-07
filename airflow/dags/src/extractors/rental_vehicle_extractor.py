from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class RentalVehicleExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        렌탈 인가제 커스텀 차량 정보 수집 (연 1회)
        - execution_date의 연도를 기준으로 해당 연도의 데이터를 수집합니다.
        """
        target_year = execution_date.strftime("%Y")
        print(f"[RentalVehicleExtractor] {target_year}년도 렌탈 인가제 차량 정보를 추출합니다.")
        # TODO: 실제 로직 구현
        
        return f"/tmp/rental_vehicles_{target_year}.csv"
