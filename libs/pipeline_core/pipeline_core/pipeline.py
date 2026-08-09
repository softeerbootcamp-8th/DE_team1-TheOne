import logging
import time
from dataclasses import dataclass
from typing import Optional

from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult
from pipeline_core.transformer import Transformer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    extractor_name: str # 어떤 데이터셋 
    write_result: WriteResult 
    duration_seconds: float # 얼마나 걸림


class Pipeline:
    """
    Extractor -> [Transformer] -> Loader 순서 실행

    실패 시 로그 남기고 예외 재발생
    재시도,실패 알림은 Airflow에서 진행
    """

    def __init__(self, extractor: Extractor, loader: Loader, transformer: Optional[Transformer] = None):
        self._extractor = extractor
        self._loader = loader
        self._transformer = transformer

    def run(self) -> PipelineResult:
        name = self._extractor.name or type(self._extractor).__name__
        started = time.monotonic()
        logger.info("pipeline start: %s", name)
        try:
            data = self._extractor.extract()
            if self._transformer is not None:
                data = self._transformer.transform(data)
            write_result = self._loader.write(data)
        except Exception:
            logger.exception("pipeline failed: %s", name)
            raise
        duration = time.monotonic() - started
        logger.info(
            "pipeline done: %s rows=%d duration=%.2fs",
            name,
            write_result.row_count,
            duration,
        )
        return PipelineResult(extractor_name=name, write_result=write_result, duration_seconds=duration)
