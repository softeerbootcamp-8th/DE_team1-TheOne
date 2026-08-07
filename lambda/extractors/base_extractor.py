from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, execution_date:datetime, **kwargs) -> Any:
        """
        output: 추출된 데이터
        """
        pass
