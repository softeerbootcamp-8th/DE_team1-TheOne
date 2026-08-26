"""운영 S3 적재 GX 공통 품질 정책 시나리오.

1. 한 행짜리 요약이 아니라 실제 적재 행을 GX Batch로 전달
2. 필수값은 컬럼 셀이 아니라 하나라도 비어 있는 레코드 비율로 계산
3. 데이터셋 정책에 따라 필수값 0% 또는 1%/5%를 적용
4. 선택 컬럼의 존재·타입·NULL은 판정하지 않음
5. 필수 컬럼 누락·타입 불일치는 비율과 무관하게 하드 실패
6. Data Docs는 실제 데이터 파티션을 보존한 S3 prefix에 발행
7. 추가 컬럼은 GX 경고로 기록하되 Slack 알림은 보내지 않음
8. summary는 관측 지표이며 성공·실패 판정에 관여하지 않음
"""

import pandas as pd
import pyarrow as pa
import pytest

from shared.airflow.common import validation
from shared.airflow.common.validation import (
    S3Location,
    gx_data_docs_location,
    run_gx_validation,
    run_table_gx_validation,
)


SCHEMA = pa.schema(
    [("driver_id", pa.string()), ("weekly_fee", pa.float64()), ("note", pa.string())]
)
REQUIRED = {"driver_id", "weekly_fee"}
DATA = S3Location(
    "de-theone",
    "bronze/sample/service_area=NYC/year_month=2026-08/"
    "collected_at=20260825T010203123456Z/data.parquet",
)


def _table(rows: int, *, invalid_rows: int = 0):
    records = []
    for index in range(rows):
        records.append(
            {
                "driver_id": None if index < invalid_rows else f"D{index}",
                # 같은 행에서 필수값 두 개가 깨져도 레코드 한 건이어야 합니다.
                "weekly_fee": None if index < invalid_rows else 100.0,
                "note": None,
            }
        )
    return pa.Table.from_pylist(records, schema=SCHEMA)


def test_GX는_요약한행이_아니라_실제적재행을_검증한다(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        validation,
        "run_gx_validation",
        lambda dataframe, expectations, **kwargs: captured.update(
            dataframe=dataframe,
            expectations=expectations,
        )
        or (),
    )

    run_table_gx_validation(
        _table(100, invalid_rows=1),
        SCHEMA,
        REQUIRED,
        dataset="sample",
        layer="bronze",
        data_location=DATA,
        context={},
        required_warning_ratio=0.01,
        required_error_ratio=0.05,
    )

    frame = captured["dataframe"]
    assert len(frame) == 100
    assert frame.loc[99, "driver_id"] == "D99"
    assert frame["__gx_required_record_valid"].value_counts().to_dict() == {
        True: 99,
        False: 1,
    }
    assert any(
        expectation.column == "driver_id"
        for expectation in captured["expectations"]
        if hasattr(expectation, "column")
    )


def test_GX는_1행summary가_아니라_전체레코드를_직접_판정한다(monkeypatch):
    table = _table(3)
    seen = {}
    monkeypatch.setattr(
        validation,
        "table_quality_summary",
        lambda *args, **kwargs: pd.DataFrame(
            [{"row_count": 0, "required_invalid_record_ratio": 1.0}]
        ),
    )
    monkeypatch.setattr(
        validation,
        "run_gx_validation",
        lambda dataframe, expectations, **kwargs: seen.update(
            dataframe=dataframe.copy(), expectations=expectations
        )
        or (),
    )

    run_table_gx_validation(
        table,
        SCHEMA,
        REQUIRED,
        dataset="sample",
        layer="silver",
        data_location=S3Location(
            "de-theone",
            "silver/sample/service_area=NYC/year_month=2026-08/"
            "source_collected_at=20260825T010203123456Z/data.parquet",
        ),
        context={},
        required_warning_ratio=None,
        required_error_ratio=0,
    )

    dataframe = seen["dataframe"]
    assert len(dataframe) == 3
    assert dataframe["driver_id"].tolist() == ["D0", "D1", "D2"]
    assert "row_count" not in dataframe.columns
    assert "required_invalid_record_ratio" not in dataframe.columns
    assert dataframe["__gx_required_record_valid"].tolist() == [True] * 3


@pytest.mark.parametrize(
    ("rows", "invalid_rows", "warns", "fails"),
    [(200, 1, False, False), (100, 1, True, False), (20, 1, False, True)],
    ids=["0.5퍼센트", "1퍼센트", "5퍼센트"],
)
def test_필수값_불량레코드_1퍼센트부터경고_5퍼센트부터실패(
    monkeypatch, rows, invalid_rows, warns, fails
):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")
    sent = []
    monkeypatch.setattr(
        validation,
        "send_gx_quality_warning",
        lambda context, **values: sent.append(values),
    )

    call = lambda: run_table_gx_validation(
        _table(rows, invalid_rows=invalid_rows),
        SCHEMA,
        REQUIRED,
        dataset="sample",
        layer="bronze",
        data_location=DATA,
        context={"run_id": "scheduled__2026-08-25"},
        required_warning_ratio=0.01,
        required_error_ratio=0.05,
    )
    if fails:
        with pytest.raises(ValueError, match="GX 검증 실패"):
            call()
    else:
        call()

    assert bool(sent) is warns


def test_선택컬럼은_누락되어도_품질지표에서_제외한다(monkeypatch):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")
    table = pa.table({"driver_id": ["D1"], "weekly_fee": [100.0]})

    run_table_gx_validation(
        table,
        SCHEMA,
        REQUIRED,
        dataset="sample",
        layer="bronze",
        data_location=DATA,
        context={},
        required_warning_ratio=0.01,
        required_error_ratio=0.05,
    )


def test_추가컬럼은_GX에_기록하되_Slack은_보내지_않는다(
    monkeypatch, caplog
):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")
    table = _table(1).append_column("source_note", pa.array(["upstream"]))
    sent = []
    monkeypatch.setattr(
        validation,
        "send_gx_quality_warning",
        lambda context, **values: sent.append(values),
    )

    with caplog.at_level("WARNING"):
        run_table_gx_validation(
            table,
            SCHEMA,
            REQUIRED,
            dataset="sample",
            layer="bronze",
            data_location=DATA,
            context={},
            required_warning_ratio=None,
            required_error_ratio=0,
            record_extra_columns=True,
        )

    assert "gx_validation warning layer=bronze" in caplog.text
    assert "column=__gx_extra_columns" in caplog.text
    assert sent == []


def test_필수문자열공백과_실수NaN은_실제행검증에서_실패한다(monkeypatch):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")
    table = pa.Table.from_pylist(
        [{"driver_id": "  ", "weekly_fee": float("nan"), "note": None}],
        schema=SCHEMA,
    )

    with pytest.raises(ValueError, match="GX 검증 실패"):
        run_table_gx_validation(
            table,
            SCHEMA,
            REQUIRED,
            dataset="sample",
            layer="bronze",
            data_location=DATA,
            context={},
            required_warning_ratio=None,
            required_error_ratio=0,
        )


def test_필수값_경고임계치는_실패임계치보다_작아야한다():
    with pytest.raises(ValueError, match="0 <= warning < error <= 1"):
        run_table_gx_validation(
            _table(10),
            SCHEMA,
            REQUIRED,
            dataset="sample",
            layer="bronze",
            data_location=DATA,
            context={},
            required_warning_ratio=0.05,
            required_error_ratio=0.01,
        )


@pytest.mark.parametrize("layer", ["bronze", "silver"])
def test_기사와_리스는_계층과무관하게_필수값한건이면_실패한다(
    monkeypatch, layer
):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")

    with pytest.raises(ValueError, match="GX 검증 실패"):
        run_table_gx_validation(
            _table(200, invalid_rows=1),
            SCHEMA,
            REQUIRED,
            dataset="sample",
            layer=layer,
            data_location=S3Location(
                "de-theone",
                DATA.key
                if layer == "bronze"
                else DATA.key.replace("bronze/", "silver/").replace(
                    "collected_at=", "source_collected_at="
                ),
            ),
            context={},
            required_warning_ratio=None,
            required_error_ratio=0,
        )


@pytest.mark.parametrize(
    ("wrong", "rule"),
    [
        (pa.table({"driver_id": ["D1"], "note": ["ok"]}), "missing_columns"),
        (
            pa.table({"driver_id": ["D1"], "weekly_fee": ["100"], "note": ["ok"]}),
            "type_mismatch_columns",
        ),
    ],
    ids=["누락", "타입불일치"],
)
def test_필수컬럼_구조불일치는_하드실패한다(monkeypatch, wrong, rule):
    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "false")

    with pytest.raises(ValueError, match=rule):
        run_table_gx_validation(
            wrong,
            SCHEMA,
            REQUIRED,
            dataset="sample",
            layer="bronze",
            data_location=DATA,
            context={},
            required_warning_ratio=0.01,
            required_error_ratio=0.05,
        )


def test_Data_Docs_S3경로는_실제데이터_파티션을_보존한다():
    target = gx_data_docs_location(DATA, layer="bronze", dataset="sample")

    assert target == S3Location(
        "de-theone",
        "logs/gx-data-docs/bronze/sample/service_area=NYC/year_month=2026-08/"
        "collected_at=20260825T010203123456Z",
    )


def test_Data_Docs를_지정한_S3_prefix에_업로드한다(monkeypatch):
    import great_expectations as gx

    monkeypatch.setenv("GX_DATA_DOCS_ENABLED", "true")
    uploaded = []

    class FakeS3:
        def upload_file(self, filename, bucket, key, ExtraArgs=None):
            uploaded.append((bucket, key, ExtraArgs))

    monkeypatch.setattr(validation.boto3, "client", lambda name: FakeS3())
    target = gx_data_docs_location(DATA, layer="bronze", dataset="sample")

    run_gx_validation(
        pd.DataFrame({"id": [1]}),
        [gx.expectations.ExpectTableRowCountToEqual(value=1)],
        suite_name="s3_docs_suite",
        layer="bronze",
        data_docs_s3_location=target,
    )

    assert uploaded[-1] == (
        "de-theone",
        f"{target.key}/index.html",
        {"ContentType": "text/html"},
    )
