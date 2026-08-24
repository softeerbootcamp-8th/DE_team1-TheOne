"""연료비 통합 Loader 의 파일 단위 원자적 교체를 검증합니다. 이슈 #518.

`sub` 쪽 같은 이름 파일에 있던 케이스인데, 통합 Loader 가 main 으로 오면서 함께
옮겼습니다 — sub 테스트가 main 모듈을 import 하면 경계가 거꾸로 뚫립니다.

교체 도중 실패해도 **기존 파일이 그대로 남고 임시 파일이 안 쌓여야** 합니다. 안 그러면
반쯤 쓰인 Parquet 이 남아 하류가 그걸 읽고, 그건 에러가 아니라 이상한 숫자로 나타납니다.
"""

from pathlib import Path

import pyarrow as pa
import pytest

from main.aws_lambda.functions.eia_fuel_price_silver.loader import (
    SCHEMA,
    EiaFuelPriceSilverLoader,
)


def _row() -> dict:
    values = {}
    for field in SCHEMA:
        if pa.types.is_string(field.type):
            values[field.name] = "value"
        elif pa.types.is_integer(field.type):
            values[field.name] = 2020
        elif pa.types.is_floating(field.type):
            values[field.name] = 1.0
        elif pa.types.is_date(field.type):
            from datetime import date

            values[field.name] = date(2026, 8, 13)
        else:
            values[field.name] = None
    return values


def test_교체실패는_기존파일을_보존하고_tmp를_정리한다(tmp_path, monkeypatch):
    loader, data = EiaFuelPriceSilverLoader(str(tmp_path), "2026-08", "NYC"), [_row()]
    loader.write(data)
    originals = {path: path.read_bytes() for path in tmp_path.rglob("*.parquet")}
    attempted_sources = []

    def fail_replace(source, target):
        attempted_sources.append(source)
        raise OSError("교체 실패")

    monkeypatch.setattr(Path, "replace", fail_replace)
    for _ in range(2):
        with pytest.raises(OSError, match="교체 실패"):
            loader.write(data)

    # 두 번의 시도가 서로 다른 임시 파일을 썼는지 — 같은 이름을 재사용하면 동시에
    # 두 번 돌 때 서로의 임시 파일을 덮어씁니다.
    assert len(set(attempted_sources)) == 2
    assert {path: path.read_bytes() for path in originals} == originals
    assert not [path for path in tmp_path.rglob("*") if path.suffix == ".tmp"]
