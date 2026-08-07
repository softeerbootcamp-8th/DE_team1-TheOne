from datetime import datetime
from typing import Any
from .base_extractor import BaseExtractor

class TaxiZoneExtractor(BaseExtractor):
    def extract(self, execution_date: datetime, **kwargs) -> Any:
        """
        택시존 265개 구역 정보 수집 (최초 1회)
        - execution_date는 무시하고 항상 동일한 정적 데이터를 수집합니다.
        """
        print("[TaxiZoneExtractor] 택시존 265개 구역 정보를 추출합니다.")
        # TODO: 실제 파일 다운로드 로직 구현
        
        return "/tmp/taxi_zones.csv"
