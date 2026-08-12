"""Gas Price HTML 원문 스냅샷 저장 시나리오.

1. HTML을 수집시각 기반 경로에 원문 그대로 저장
2. 같은 수집시각의 스냅샷을 다시 쓰면 기존 원문을 보존하고 실패
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from functions.common import gas_price_layout as layout
from functions.gas_price_raw_to_bronze.snapshot import GasPriceSnapshotLoader


COLLECTED_AT = datetime(2026, 8, 12, 3, 4, 5, 123456, tzinfo=timezone.utc)
HTML = "<html><body>AAA price 원문</body></html>\n"


def test_HTML을_수집시각_경로에_원문_그대로_저장한다(tmp_path):
    result = GasPriceSnapshotLoader(str(tmp_path), COLLECTED_AT).write(HTML)
    path = Path(result.location)

    assert path == layout.snapshot_file(str(tmp_path), COLLECTED_AT)
    assert path.read_text(encoding="utf-8") == HTML
    assert result.row_count == 1


def test_같은_수집시각의_스냅샷은_덮어쓰지_않는다(tmp_path):
    loader = GasPriceSnapshotLoader(str(tmp_path), COLLECTED_AT)
    path = Path(loader.write(HTML).location)

    with pytest.raises(FileExistsError):
        loader.write("바뀐 원문")

    assert path.read_text(encoding="utf-8") == HTML
