"""차량 대장 HTML·이미지 원문 스냅샷 저장 시나리오.

1. HTML과 이미지 bytes를 수집시각별 경로에 그대로 저장
2. 같은 수집시각·URL의 스냅샷은 기존 원문을 보존하고 실패
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from functions.common import vehicle_catalog_layout as layout
from functions.vehicle_catalog_raw_to_bronze.snapshot import (
    VehicleCatalogHtmlSnapshotLoader,
    VehicleCatalogImageSnapshotLoader,
)


COLLECTED_AT = datetime(2026, 8, 12, 3, 4, 5, 123456, tzinfo=timezone.utc)
IMAGE_URL = "https://example.com/wp-content/uploads/card.png"
HTML = "<html><body>차량 대장 원문</body></html>\n"
IMAGE_BYTES = b"\x89PNG\r\n\x1a\nraw-card-image"


def test_HTML과_이미지를_수집시각_경로에_원문_그대로_저장한다(tmp_path):
    html_result = VehicleCatalogHtmlSnapshotLoader(
        str(tmp_path), COLLECTED_AT
    ).write(HTML)
    image_result = VehicleCatalogImageSnapshotLoader(
        str(tmp_path), COLLECTED_AT, IMAGE_URL
    ).write(IMAGE_BYTES)

    html_path = Path(html_result.location)
    image_path = Path(image_result.location)
    assert html_path == layout.html_snapshot_file(str(tmp_path), COLLECTED_AT)
    assert image_path == layout.image_snapshot_file(
        str(tmp_path), COLLECTED_AT, IMAGE_URL
    )
    assert html_path.read_text(encoding="utf-8") == HTML
    assert image_path.read_bytes() == IMAGE_BYTES
    assert html_result.row_count == image_result.row_count == 1


@pytest.mark.parametrize("kind", ["html", "image"])
def test_같은_수집시각의_스냅샷은_덮어쓰지_않는다(tmp_path, kind):
    if kind == "html":
        loader = VehicleCatalogHtmlSnapshotLoader(str(tmp_path), COLLECTED_AT)
        original, changed = HTML, "바뀐 HTML"
    else:
        loader = VehicleCatalogImageSnapshotLoader(
            str(tmp_path), COLLECTED_AT, IMAGE_URL
        )
        original, changed = IMAGE_BYTES, b"changed-image"

    path = Path(loader.write(original).location)
    with pytest.raises(FileExistsError):
        loader.write(changed)

    saved = path.read_text(encoding="utf-8") if kind == "html" else path.read_bytes()
    assert saved == original
