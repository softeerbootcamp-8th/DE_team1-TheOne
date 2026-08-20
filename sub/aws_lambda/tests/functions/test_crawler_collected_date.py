"""크롤러 4종이 지정 일자로 적재하는지 확인합니다 (#585).

전에는 `datetime.now()` 로 고정이라 과거 파티션을 되살릴 수 없었습니다. 파티션 경로와
행의 `collected_at`, 반환 `collected_date` 가 **셋 다** 지정 일자여야 합니다 — Bronze
검증이 행에서 뽑은 날짜와 파티션 날짜가 같은지 보기 때문에 하나만 움직이면 죽습니다.

네 크롤러가 같은 실수를 하기 쉬워(각자 now() 를 부르던 자리) 한 파일에서 함께 봅니다.
"""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sub.aws_lambda.functions.uber_eligible_vehicles_source_to_raw import (
    extractor as uber_extractor,
    handler as uber_handler,
)
from sub.aws_lambda.functions.lyft_eligible_vehicles_source_to_raw import (
    extractor as lyft_extractor,
    handler as lyft_handler,
)

COLLECTED_DATE = "2026-05-01"
UBER_PAYLOAD = {"Kia": {"NIRO": "2019 (UberX)"}}
# Lyft 는 FAQ 페이지 구조를 그대로 받습니다 (uber 의 {제조사: {모델: 원문}} 과 다름).
LYFT_PAYLOAD = {
    "componentType": "FAQ",
    "displayName": lyft_extractor.VEHICLE_FAQ_NAME,
    "entries": [
        {
            "componentType": "FAQEntry",
            "question": "Kia",
            "answer": "__NIRO__ - 2019 (Extra Comfort)",
        }
    ],
}

CRAWLERS = [
    pytest.param(uber_handler, uber_extractor, UBER_PAYLOAD, "uber_eligible_vehicles", id="uber"),
    pytest.param(lyft_handler, lyft_extractor, LYFT_PAYLOAD, "lyft_eligible_vehicles", id="lyft"),
]


def _stub(monkeypatch, module, payload):
    # uber 는 fetch(city_slug, timeout), lyft 는 fetch(timeout) 로 시그니처가 다릅니다.
    monkeypatch.setattr(module, "fetch", lambda *args, **kwargs: payload)


@pytest.mark.parametrize(("handler", "extractor", "payload", "dataset"), CRAWLERS)
def test_지정_일자로_파티션과_행이_함께_간다(
    handler, extractor, payload, dataset, monkeypatch, tmp_path
):
    _stub(monkeypatch, extractor, payload)

    result = handler.lambda_handler(
        {"base_dir": str(tmp_path), "collected_date": COLLECTED_DATE}
    )

    assert result["collected_date"] == COLLECTED_DATE
    location = Path(result["locations"][0])
    assert f"collected_date={COLLECTED_DATE}" in str(location)

    written = pq.ParquetFile(location).read().to_pylist()
    # 행의 collected_at 이 파티션과 다른 날이면 Bronze 검증이 죽습니다.
    assert {row["collected_at"].strftime("%Y-%m-%d") for row in written} == {COLLECTED_DATE}


@pytest.mark.parametrize(("handler", "extractor", "payload", "dataset"), CRAWLERS)
def test_비우면_예전처럼_실행_시각을_쓴다(
    handler, extractor, payload, dataset, monkeypatch, tmp_path
):
    """기본 동작이 바뀌면 매일 도는 스케줄이 엉뚱한 날짜로 쌓입니다."""
    _stub(monkeypatch, extractor, payload)

    result = handler.lambda_handler({"base_dir": str(tmp_path)})

    from datetime import datetime, timezone

    assert result["collected_date"] == f"{datetime.now(timezone.utc):%Y-%m-%d}"


@pytest.mark.parametrize(("handler", "extractor", "payload", "dataset"), CRAWLERS)
def test_형식이_틀리면_적재하지_않고_실패한다(
    handler, extractor, payload, dataset, monkeypatch, tmp_path
):
    """조용히 오늘로 떨어지면 백필이 엉뚱한 파티션에 쌓입니다."""
    _stub(monkeypatch, extractor, payload)

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        handler.lambda_handler({"base_dir": str(tmp_path), "collected_date": "2026-5-1"})

    assert list(tmp_path.iterdir()) == []
