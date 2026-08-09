from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class WriteResult:
    """
    적재 결과

    location: 파일 경로 또는 테이블 식별자 등 적재된 위치 (값: 구현체가 결정)
    """

    location: str
    row_count: int


class Loader(ABC):
    """
    데이터를 저장소에 적재

    스키마, 파티션 규칙은 구현체 생성자에서 명시적으로 받음(예: LocalParquetLoader(dataset, schema, partition_by=[...])). 
    """

    @abstractmethod
    def write(self, data: Any) -> WriteResult:
        raise NotImplementedError
