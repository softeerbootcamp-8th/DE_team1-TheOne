import logging

import pytest

from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult
from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer


class FakeExtractor(Extractor):
    name = "fake_extractor"

    def __init__(self, data):
        self._data = data

    def extract(self):
        return self._data


class FakeLoader(Loader):
    def __init__(self):
        self.written = None

    def write(self, data):
        self.written = data
        return WriteResult(location="/tmp/fake", row_count=len(data))


class UpperTransformer(Transformer):
    def transform(self, data):
        return [item.upper() for item in data]


class FailingExtractor(Extractor):
    name = "failing_extractor"

    def extract(self):
        raise RuntimeError("boom")


def test_pipeline_without_transformer_passes_data_through():
    loader = FakeLoader()
    pipeline = Pipeline(FakeExtractor(["a", "b"]), loader)

    result = pipeline.run()

    assert loader.written == ["a", "b"]
    assert result.write_result.row_count == 2
    assert result.extractor_name == "fake_extractor"
    assert result.duration_seconds >= 0


def test_pipeline_with_transformer_applies_transform_before_load():
    loader = FakeLoader()
    pipeline = Pipeline(FakeExtractor(["a", "b"]), loader, transformer=UpperTransformer())

    pipeline.run()

    assert loader.written == ["A", "B"]


def test_pipeline_falls_back_to_class_name_when_name_unset():
    class Unnamed(Extractor):
        def extract(self):
            return []

    result = Pipeline(Unnamed(), FakeLoader()).run()

    assert result.extractor_name == "Unnamed"


def test_pipeline_reraises_and_logs_on_extractor_failure(caplog):
    pipeline = Pipeline(FailingExtractor(), FakeLoader())

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="boom"):
            pipeline.run()

    assert "failing_extractor" in caplog.text
