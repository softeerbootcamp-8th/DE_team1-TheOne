"""Monthly Taxi Trip GX 결과 판정 시나리오(Java 없이 결과 계약만 검증).

1. GX에 받은 DataFrame 객체를 그대로 전달하고 사유별 unexpected_count를 보존
2. 경고 이상·실패 미만은 경고 reconciliation으로 반환
3. 실패 임계치와 같은 불량률부터 명시적으로 실패
4. 추가 컬럼과 GX 결과를 Airflow가 읽을 JSON sidecar에 기록
5. 같은 GX validation 결과로 Data Docs를 지정한 S3 prefix에 발행
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
            _result("extra_columns", total=total, unexpected=None),
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
    dataframe = SimpleNamespace(columns=list(quality._EXPECTED_COLUMNS))
    captured = {}

    def fake_validate(actual, expectations, **kwargs):
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
    assert len(captured["expectations"]) == 7
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
        lambda dataframe, expectations, **kwargs: _validation(
            total=20,
            invalid=1,
            invalid_tier=1,
        ),
    )

    with pytest.raises(ValueError, match="불합격 비율이 5.00%"):
        quality.validate_monthly_taxi_trip_records(
            SimpleNamespace(columns=list(quality._EXPECTED_COLUMNS)),
            warning_threshold=0.01,
            error_threshold=0.05,
        )


def test_경고임계치가_0이어도_불량이_없으면_경고하지_않는다(monkeypatch):
    monkeypatch.setattr(
        quality,
        "_validate_gx_batch",
        lambda dataframe, expectations, **kwargs: _validation(total=10, invalid=0),
    )

    counts = quality.validate_monthly_taxi_trip_records(
        SimpleNamespace(columns=list(quality._EXPECTED_COLUMNS)),
        warning_threshold=0,
        error_threshold=0.05,
    )

    assert counts.warning is False


def test_추가컬럼과_검증결과를_counts로_돌려준다(monkeypatch):
    """GX 결과는 파일로 내보내지 않고 반환값으로만 넘깁니다.

    호출부(transformer)가 이 값을 `_RECON.json` 에 실어 Airflow 로 보냅니다 — 예전에는
    같은 값을 `_GX_VALIDATION.json` 으로 한 번 더 썼습니다(#1120).
    """
    monkeypatch.setattr(
        quality,
        "_validate_gx_batch",
        lambda dataframe, expectations, **kwargs: _validation(total=10, invalid=0),
    )
    dataframe = SimpleNamespace(
        columns=[*quality._EXPECTED_COLUMNS, "airport_fee", "congestion_fee"]
    )

    counts = quality.validate_monthly_taxi_trip_records(
        dataframe,
        warning_threshold=0.01,
        error_threshold=0.05,
        data_docs_location="s3://de-theone/logs/gx-data-docs/silver/monthly_taxi_trip/service_area=NYC",
    )

    assert counts.extra_columns == ("airport_fee", "congestion_fee")
    assert counts.total == 10
    assert counts.invalid == 0
    assert counts.error_threshold == 0.05


def test_요약파일_인자를_더는_받지_않는다():
    """`summary_location` 을 남겨두면 옛 호출부가 조용히 통과합니다."""
    import inspect

    parameters = inspect.signature(
        quality.validate_monthly_taxi_trip_records
    ).parameters
    assert "summary_location" not in parameters


def test_추가컬럼은_Data_Docs용_GX_warning_expectation으로_기록한다():
    expectations = quality._expectations(0.01, 0.05)
    extra = next(
        expectation
        for expectation in expectations
        if expectation.meta.get("quality_rule") == "extra_columns"
    )

    assert extra.meta["severity"] == "warning"
    assert set(extra.column_set) == set(quality._EXPECTED_COLUMNS)


def test_같은_GX결과로_Data_Docs를_S3에_발행한다(monkeypatch):
    calls = []

    class FakeDataSources:
        def add_spark(self, **kwargs):
            return self

        def add_dataframe_asset(self, **kwargs):
            return self

        def add_batch_definition_whole_dataframe(self, *args):
            return object()

    class FakeSuites:
        def add_or_update(self, suite):
            return suite

    class FakeContext:
        data_sources = FakeDataSources()
        suites = FakeSuites()
        variables = SimpleNamespace(progress_bars=None)

        def build_data_docs(self, **kwargs):
            calls.append(("build", kwargs))

    class FakeDefinition:
        def __init__(self, **kwargs):
            pass

        def run(self, **kwargs):
            return "validation"

    monkeypatch.setattr(quality, "data_docs_config", lambda root: object())
    monkeypatch.setattr(quality.gx, "get_context", lambda **kwargs: FakeContext())
    monkeypatch.setattr(quality.gx, "ValidationDefinition", FakeDefinition)
    monkeypatch.setattr(
        quality,
        "upload_data_docs",
        lambda root, **kwargs: calls.append(("upload", kwargs)),
    )

    result = quality._validate_gx_batch(
        object(),
        [],
        data_docs_location=(
            "s3://de-theone/logs/gx-data-docs/silver/monthly_taxi_trip/"
            "service_area=NYC/year_month=2026-08/source_collected_at=x"
        ),
    )

    assert result == "validation"
    assert calls == [
        ("build", {"site_names": ["local_site"]}),
        (
            "upload",
            {
                "bucket": "de-theone",
                "prefix": (
                    "logs/gx-data-docs/silver/monthly_taxi_trip/"
                    "service_area=NYC/year_month=2026-08/source_collected_at=x"
                ),
            },
        ),
    ]
