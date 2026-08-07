from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class UberEligibleExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        Uber Eligible List 수집 (연 1회)
        - execution_date의 연도를 기준으로 데이터를 수집합니다.
        """
        target_year = execution_date.strftime("%Y")
        print(f"[UberEligibleExtractor] {target_year}년도 Uber Eligible List를 추출합니다.")
        # TODO: 실제 로직 구현
        
        return f"/tmp/uber_eligible_{target_year}.csv"
