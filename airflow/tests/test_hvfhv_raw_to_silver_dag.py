"""HVFHV Raw -> Bronze -> Silver DAG의 대상 연월 계산과 태스크 계약을 확인합니다.

시나리오:

1. DAG 구조 — dag_id, task 2개, raw_to_bronze -> bronze_to_silver 의존 순서
2. [필수] 1월 실행 시 직전 달이 전년 12월
3. [필수] params.year/month 지정 시 자동 계산을 무시하고 month가 0패딩됨
4. [필수] logical_date가 naive여도 UTC로 간주하고 죽지 않음
5. params가 year만/month만 있으면 자동 계산으로 떨어짐
6. bronze_to_silver의 bash_command에 --error_threshold 0.2와 xcom_pull 템플릿 인자가 그대로 들어감
"""

from datetime import datetime, timezone

import pytest

from dags import hvfhv_raw_to_silver_dag as dag_module

DAG = dag_module.hvfhv_dag
DAG_ID = "hvfhv_raw_to_silver_pipeline"
resolve_target_year_month = dag_module.resolve_target_year_month


# --- DAG 구조 -------------------------------------------------------------


def test_DAG_는_두_태스크를_갖고_raw_to_bronze_다음에_bronze_to_silver_가_온다():
    assert DAG.dag_id == DAG_ID
    assert set(DAG.task_ids) == {"raw_to_bronze", "bronze_to_silver"}
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"bronze_to_silver"}
    assert DAG.get_task("bronze_to_silver").upstream_task_ids == {"raw_to_bronze"}


# --- 대상 연월 자동 계산 -----------------------------------------------------


def test_1월_실행시_직전_달은_전년도_12월이다():
    logical_date = datetime(2024, 1, 10, tzinfo=timezone.utc)

    assert resolve_target_year_month(logical_date, {}) == ("2023", "12")


def test_logical_date가_naive여도_UTC로_간주하고_죽지_않는다():
    """[필수] tz 정보가 없는 logical_date가 들어와도 aware 값과 동일하게 계산돼야 합니다."""
    naive_logical_date = datetime(2024, 1, 10)

    assert resolve_target_year_month(naive_logical_date, {}) == ("2023", "12")


# --- 수동 파라미터 우선순위 ---------------------------------------------------


def test_수동_파라미터가_자동계산보다_우선하고_month가_0패딩된다():
    """[필수] 재처리 시 0패딩이 안 되면 Lambda가 잘못된 S3 prefix를 찾습니다."""
    logical_date = datetime(2024, 1, 10, tzinfo=timezone.utc)

    result = resolve_target_year_month(logical_date, {"year": "2030", "month": "3"})

    assert result == ("2030", "03")


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"year": "2030"}, id="year만 지정"),
        pytest.param({"month": "05"}, id="month만 지정"),
    ],
)
def test_params가_한쪽만_있으면_자동계산으로_떨어진다(params):
    logical_date = datetime(2024, 1, 10, tzinfo=timezone.utc)

    assert resolve_target_year_month(logical_date, params) == ("2023", "12")


# --- bronze_to_silver bash_command 계약 --------------------------------------


def test_bronze_to_silver_bash_command에_error_threshold와_xcom_pull이_들어간다():
    bash_command = DAG.get_task("bronze_to_silver").bash_command

    assert "--error_threshold 0.2" in bash_command
    assert "xcom_pull(task_ids='raw_to_bronze')['year']" in bash_command
    assert "xcom_pull(task_ids='raw_to_bronze')['month']" in bash_command
