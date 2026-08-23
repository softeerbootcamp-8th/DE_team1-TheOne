import pytest

from pipeline_core.extractor import Extractor
from pipeline_core.transformer import ChainedTransformer, Transformer
from pipeline_core.loader import Loader, WriteResult


def test_extractor_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        Extractor()


def test_extractor_subclass_must_implement_extract():
    class Incomplete(Extractor):
        pass

    with pytest.raises(TypeError):
        Incomplete()


def test_extractor_subclass_works():
    class Dummy(Extractor):
        name = "dummy"

        def extract(self):
            return {"a": 1}

    assert Dummy().extract() == {"a": 1}


def test_transformer_subclass_works():
    class Upper(Transformer):
        def transform(self, data):
            return data.upper()

    assert Upper().transform("hi") == "HI"


def test_chained_transformer_applies_in_order():
    class Upper(Transformer):
        def transform(self, data):
            return data.upper()

    class ExclaimBang(Transformer):
        def transform(self, data):
            return data + "!"

    result = ChainedTransformer([Upper(), ExclaimBang()]).transform("hi")
    assert result == "HI!"


def test_loader_subclass_returns_write_result():
    class FakeLoader(Loader):
        def write(self, data):
            return WriteResult(location="/tmp/fake.parquet", row_count=len(data))

    result = FakeLoader().write([1, 2, 3])
    assert result == WriteResult(location="/tmp/fake.parquet", row_count=3)
