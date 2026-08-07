from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class VehicleSpecExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        차종별 제원(연비, 정비, 주행거리) 수집 (연 1회)
        - execution_date의 연도를 기준으로 데이터를 수집합니다.
        """
        target_year = execution_date.strftime("%Y")
        print(f"[VehicleSpecExtractor] {target_year}년도 차종별 제원 데이터를 추출합니다.")
        # TODO: 실제 로직 구현
        
        return f"/tmp/vehicle_specs_{target_year}.csv"
