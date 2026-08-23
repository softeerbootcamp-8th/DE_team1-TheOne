from abc import ABC, abstractmethod
from typing import Any


class Extractor(ABC):
    """
    데이터 추출 용도, 동작 수행 후 결과를 반환

    Lambda: 외부 API/크롤링 결과 반환 
    Spark: Bronze/Silver 파티션 읽어 DataFrame 반환
    """

    name: str = ""

    @abstractmethod
    def extract(self) -> Any:
        raise NotImplementedError
