from abc import ABC, abstractmethod
from typing import Any
from datetime import datetime

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, execution_date:datetime, **kwargs) -> Any:
        """
        데이터를 추출하는 공통 인터페이스입니다.
            
        Returns:
            추출된 데이터 또는 데이터가 저장된 파일의 경로 등
        """
        pass
