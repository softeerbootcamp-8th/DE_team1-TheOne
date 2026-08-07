from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class HVFHVExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        HVFHV 운행 기록 데이터 수집 (월 1회)
        - execution_date의 연/월을 기준으로 해당 월의 데이터를 수집합니다.
        """
        target_year_month = execution_date.strftime("%Y-%m")
        print(f"[HVFHVExtractor] {target_year_month} 데이터를 추출합니다.")
        # TODO: 실제 다운로드 로직 구현
        
        return f"/tmp/hvfhv_{target_year_month}.parquet"
