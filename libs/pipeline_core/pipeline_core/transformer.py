from abc import ABC, abstractmethod
from typing import Any


class Transformer(ABC):
    """
    추출 데이터 형태 변환 (Clean 등)

    Bronze->Silver 정제, Silver->Gold 집계에 사용.
    """

    @abstractmethod
    def transform(self, data: Any) -> Any:
        raise NotImplementedError


class ChainedTransformer(Transformer):
    """여러 Transformer를 순서대로 적용"""

    def __init__(self, transformers: list[Transformer]):
        self._transformers = transformers

    def transform(self, data: Any) -> Any:
        for transformer in self._transformers:
            data = transformer.transform(data)
        return data
