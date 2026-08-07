from abc import ABC, abstractmethod
from typing import Any

class BaseLoader(ABC):
    @abstractmethod
    def load(self, data: Any, *args, **kwargs) -> None:
        """
        추출된 데이터를 목적지에 적재하는 공통 인터페이스입니다.
        
        Args:
            data: 메모리에 로드된 데이터 객체 또는 데이터가 저장된 파일의 경로
        """
        pass
