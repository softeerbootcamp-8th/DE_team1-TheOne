from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class NYAvgFareExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        뉴욕 평균 요금 (뉴욕주 평균 전기 요금 등) 수집 (연 1회)
        - execution_date의 연도를 기준으로 데이터를 수집합니다.
        """
        target_year = execution_date.strftime("%Y")
        print(f"[NYAvgFareExtractor] {target_year}년도 뉴욕 평균 요금 데이터를 추출합니다.")
        # TODO: 실제 로직 구현
        
        return f"/tmp/ny_avg_fare_{target_year}.csv"
