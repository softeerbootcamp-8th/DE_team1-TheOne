"""Monthly Taxi Trip GX 결과 판정 시나리오(Java 없이 결과 계약만 검증).

1. GX에 받은 DataFrame 객체를 그대로 전달하고 사유별 unexpected_count를 보존
2. 경고 이상·실패 미만은 경고 reconciliation으로 반환
3. 실패 임계치와 같은 불량률부터 명시적으로 실패
"""

from types import SimpleNamespace

import pytest

from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver import (
    quality,
)


def _result(rule: str, *, total: int = 100, unexpected: int | None = 0):
    payload = {"element_count": total}
    if unexpected is not None:
        payload["unexpected_count"] = unexpected
    return SimpleNamespace(
        expectation_config=SimpleNamespace(meta={"quality_rule": rule}),
        result=payload,
    )


def _validation(
    *,
    total: int,
    invalid: int,
    missing_or_type: int = 0,
    invalid_value: int = 0,
    invalid_tier: int = 0,
):
    return SimpleNamespace(
        results=[
            _result("row_count", total=total, unexpected=None),
            _result(
                "missing_or_type_mismatch",
                total=total,
                unexpected=missing_or_type,
            ),
            _result("invalid_value", total=total, unexpected=invalid_value),
            _result(
                "invalid_service_tier",
                total=total,
                unexpected=invalid_tier,
            ),
            _result("record_warning", total=total, unexpected=invalid),
            _result("record_error", total=total, unexpected=invalid),
        ]
    )


def test_GX에_전체_DataFrame객체를_그대로_넘기고_경고건수를_보존한다(
    monkeypatch,
):
    dataframe = object()
    captured = {}

    def fake_validate(actual, expectations):
        captured["dataframe"] = actual
        captured["expectations"] = expectations
        return _validation(
            total=100,
            invalid=2,
            missing_or_type=1,
            invalid_value=1,
        )

    monkeypatch.setattr(quality, "_validate_gx_batch", fake_validate)

    counts = quality.validate_monthly_taxi_trip_records(
        dataframe,
        warning_threshold=0.01,
        error_threshold=0.05,
    )

    assert captured["dataframe"] is dataframe
    assert len(captured["expectations"]) == 6
    assert counts.total == 100
    assert counts.valid == 98
    assert counts.invalid == 2
    assert counts.missing_or_type_mismatch == 1
    assert counts.invalid_value == 1
    assert counts.invalid_ratio == 0.02
    assert counts.warning is True


def test_GX불량률이_실패임계치와_같으면_실패한다(monkeypatch):
    monkeypatch.setattr(
        quality,
        "_validate_gx_batch",
        lambda dataframe, expectations: _validation(
            total=20,
            invalid=1,
            invalid_tier=1,
        ),
    )

    with pytest.raises(ValueError, match="불합격 비율이 5.00%"):
        quality.validate_monthly_taxi_trip_records(
            object(),
            warning_threshold=0.01,
            error_threshold=0.05,
        )


def test_경고임계치가_0이어도_불량이_없으면_경고하지_않는다(monkeypatch):
    monkeypatch.setattr(
        quality,
        "_validate_gx_batch",
        lambda dataframe, expectations: _validation(total=10, invalid=0),
    )

    counts = quality.validate_monthly_taxi_trip_records(
        object(),
        warning_threshold=0,
        error_threshold=0.05,
    )

    assert counts.warning is False
