"""공통 Validation primitive와 GX 실행·Data Docs 발행 계약을 검증합니다.

1. 정상 GX 검증은 성공 로그와 Suite/Validation HTML을 남긴다.
2. 실패 GX 검증은 표준 로그·예외와 실패 HTML을 남긴다.
3. 단일·복합 컬럼 규칙의 대상을 표준 형식으로 표시한다.
4. 여러 DAG의 Suite와 Validation 결과를 같은 Data Docs에 누적한다.
5. Data Docs 빌드 실패 시 데이터 검증 결과를 우선해 예외를 전달한다.
6. 경고 severity GX 실패는 Data Docs에 남기되 파이프라인을 중단하지 않는다.
7. Suite에 저장할 날짜 InSet 값은 ISO 문자열로 왕복한다.
"""

import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from shared.airflow.common.validation import (
    parse_handler_result,
    parse_iso_date,
    parse_year_month,
    read_parquet,
    require_file,
    run_gx_validation,
)


def test_handler_result를_경로와_행수로_변환한다():
    parsed = parse_handler_result(
        {"row_count": 2, "locations": ["a", "b"]}, expected_locations=2
    )
    assert parsed.row_count == 2
    assert parsed.locations == (Path("a"), Path("b"))


@pytest.mark.parametrize("row_count", [True, 0, -1, "1", None])
def test_row_count가_양의_정수가_아니면_실패한다(row_count):
    with pytest.raises(ValueError, match="row_count"):
        parse_handler_result({"row_count": row_count, "locations": ["a"]})


@pytest.mark.parametrize("locations", [None, [], [""], [Path("a")]])
def test_locations가_문자열_경로_목록이_아니면_실패한다(locations):
    with pytest.raises(ValueError, match="locations"):
        parse_handler_result({"row_count": 1, "locations": locations})


def test_날짜와_월을_엄격한_형식으로_파싱한다():
    assert parse_iso_date("2026-08-12").isoformat() == "2026-08-12"
    assert parse_year_month("2026-08") == "2026-08"
    with pytest.raises(ValueError):
        parse_iso_date("2026-8-12")
    with pytest.raises(ValueError):
        parse_year_month("2026-8")


def test_파일_존재와_비어있지_않음을_확인한다(tmp_path):
    path = tmp_path / "data"
    with pytest.raises(FileNotFoundError):
        require_file(path)
    path.touch()
    with pytest.raises(ValueError, match="비어"):
        require_file(path)


def test_parquet을_읽고_손상된_파일은_명시적으로_실패한다(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    assert read_parquet(path).to_pylist() == [{"id": 1}]
    path.write_text("broken")
    with pytest.raises(RuntimeError, match="Parquet"):
        read_parquet(path)


def _html_text(root: Path) -> str:
    return "\n".join(path.read_text() for path in root.rglob("*.html"))


def test_GX_정상_검증은_통과_로그와_Data_Docs를_남긴다(tmp_path, caplog):
    import great_expectations as gx

    docs = tmp_path / "docs"
    with caplog.at_level(logging.INFO, logger="common.validation"):
        run_gx_validation(
            pd.DataFrame({"id": [1]}),
            [gx.expectations.ExpectTableRowCountToEqual(value=1)],
            suite_name="common_success_suite",
            layer="bronze",
            data_docs_dir=docs,
        )

    assert "gx_validation passed layer=bronze expectations=1" in caplog.text
    assert f"gx_data_docs updated path={docs / 'index.html'}" in caplog.text
    assert (docs / "index.html").is_file()
    assert list((docs / ".gx_store" / "expectations").glob("*.json"))
    assert list((docs / ".gx_store" / "validations").rglob("*.json"))
    html = _html_text(docs)
    assert "common_success_suite" in html
    assert "Must have exactly" in html
    assert "Succeeded" in html


def test_GX_실패는_표준_로그와_예외와_실패_Data_Docs를_남긴다(
    tmp_path, caplog
):
    import great_expectations as gx

    docs = tmp_path / "docs"
    with caplog.at_level(logging.ERROR, logger="common.validation"):
        with pytest.raises(
            ValueError, match=r"expect_table_row_count_to_equal\[table\]"
        ):
            run_gx_validation(
                pd.DataFrame({"id": [1, 2]}),
                [gx.expectations.ExpectTableRowCountToEqual(value=1)],
                suite_name="common_failure_suite",
                layer="silver",
                data_docs_dir=docs,
            )

    assert "gx_validation failed layer=silver" in caplog.text
    assert "expectation=expect_table_row_count_to_equal" in caplog.text
    assert "column=table" in caplog.text
    assert "observed_value=2" in caplog.text
    assert (docs / "index.html").is_file()
    assert "common_failure_suite" in _html_text(docs)


def test_GX_경고규칙_실패는_로그와_Data_Docs를_남기고_계속한다(
    tmp_path, caplog
):
    import great_expectations as gx

    docs = tmp_path / "docs"
    with caplog.at_level(logging.WARNING, logger="common.validation"):
        run_gx_validation(
            pd.DataFrame({"invalid_ratio": [0.023]}),
            [
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="invalid_ratio",
                    max_value=0.01,
                    strict_max=True,
                    meta={"severity": "warning"},
                )
            ],
            suite_name="common_warning_suite",
            layer="bronze",
            data_docs_dir=docs,
        )

    assert "gx_validation warning layer=bronze" in caplog.text
    assert "observed_value=[0.023]" in caplog.text
    assert "common_warning_suite" in _html_text(docs)


def test_GX_실패_규칙의_대상_컬럼을_표준_형식으로_표시한다(tmp_path):
    import great_expectations as gx

    dataframe = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=["b", "a"]
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(column="a"),
        gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["a", "b"]),
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="a", column_B="b"
        ),
    ]

    with pytest.raises(ValueError) as exc_info:
        run_gx_validation(
            dataframe,
            expectations,
            suite_name="common_columns_suite",
            layer="silver",
            data_docs_dir=tmp_path / "docs",
        )

    message = str(exc_info.value)
    assert "expect_table_row_count_to_equal[table]" in message
    assert "expect_table_columns_to_match_ordered_list[table]" in message
    assert "expect_column_values_to_be_unique[a]" in message
    assert "expect_compound_columns_to_be_unique[a/b]" in message
    assert "expect_column_pair_values_a_to_be_greater_than_b[a/b]" in message


def test_여러_DAG의_Data_Docs를_같은_루트에_누적한다(tmp_path):
    import great_expectations as gx

    docs = tmp_path / "docs"
    for suite_name in ("first_dag_suite", "second_dag_suite"):
        run_gx_validation(
            pd.DataFrame({"id": [1]}),
            [gx.expectations.ExpectTableRowCountToEqual(value=1)],
            suite_name=suite_name,
            layer="bronze",
            data_docs_dir=docs,
        )

    html = _html_text(docs)
    assert "first_dag_suite" in html
    assert "second_dag_suite" in html
    assert len(list((docs / ".gx_store" / "expectations").glob("*.json"))) == 2
    assert len(list((docs / ".gx_store" / "validations").rglob("*.json"))) == 2


@pytest.mark.parametrize(
    ("row_count", "expected_error"),
    [(1, RuntimeError), (2, ValueError)],
    ids=["검증 성공이면 Data Docs 예외 전달", "검증 실패면 GX 예외 보존"],
)
def test_Data_Docs_빌드_실패는_데이터_검증_결과를_우선한다(
    tmp_path, monkeypatch, caplog, row_count, expected_error
):
    import great_expectations as gx

    original_get_context = gx.get_context

    def context_with_broken_docs(*args, **kwargs):
        context = original_get_context(*args, **kwargs)

        def fail_to_build(*args, **kwargs):
            raise RuntimeError("Data Docs unavailable")

        monkeypatch.setattr(context, "build_data_docs", fail_to_build)
        return context

    monkeypatch.setattr(gx, "get_context", context_with_broken_docs)
    docs = tmp_path / "docs"

    with caplog.at_level(logging.ERROR, logger="common.validation"):
        with pytest.raises(expected_error) as exc_info:
            run_gx_validation(
                pd.DataFrame({"id": list(range(row_count))}),
                [gx.expectations.ExpectTableRowCountToEqual(value=1)],
                suite_name=f"broken_docs_{row_count}_suite",
                layer="silver",
                data_docs_dir=docs,
            )

    assert list((docs / ".gx_store" / "validations").rglob("*.json"))
    if expected_error is RuntimeError:
        assert "Data Docs unavailable" in str(exc_info.value)
    else:
        assert "expect_table_row_count_to_equal[table]" in str(exc_info.value)
        assert "gx_data_docs build failed" in caplog.text


def test_GX_InSet_날짜값은_ISO_문자열로_저장하고_검증한다(tmp_path):
    import great_expectations as gx

    docs = tmp_path / "docs"
    run_gx_validation(
        pd.DataFrame({"collected_date": ["2026-08-13"]}),
        [
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="collected_date", value_set=["2026-08-13"]
            )
        ],
        suite_name="iso_date_suite",
        layer="bronze",
        data_docs_dir=docs,
    )

    suite_json = next((docs / ".gx_store" / "expectations").glob("*.json"))
    suite = json.loads(suite_json.read_text())
    assert suite["expectations"][0]["kwargs"]["value_set"] == ["2026-08-13"]
