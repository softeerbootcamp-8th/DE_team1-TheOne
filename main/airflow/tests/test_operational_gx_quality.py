"""운영 S3 적재 GX 공통 품질 정책 시나리오.

1. 필수값은 컬럼 셀이 아니라 하나라도 비어 있는 레코드 비율로 계산
2. 데이터셋 정책에 따라 필수값 0% 또는 1%/5%를 적용
3. 선택 컬럼의 존재·타입·NULL은 판정하지 않음
4. 필수 컬럼 누락·타입 불일치는 비율과 무관하게 하드 실패
5. Data Docs는 실제 데이터 파티션을 보존한 S3 prefix에 발행
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
    table_quality_summary,
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


def test_필수값은_복수컬럼이_깨져도_레코드한건으로_계산한다():
    summary = table_quality_summary(_table(100, invalid_rows=1), SCHEMA, REQUIRED)

    assert summary.at[0, "required_invalid_record_count"] == 1
    assert summary.at[0, "required_invalid_record_ratio"] == 0.01


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
    summary = table_quality_summary(table, SCHEMA, REQUIRED)

    assert "note_null_ratio" not in summary.columns
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
