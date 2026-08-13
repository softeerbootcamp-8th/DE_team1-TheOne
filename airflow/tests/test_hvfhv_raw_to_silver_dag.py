"""HVFHV Raw -> Bronze -> Silver DAG의 대상 연월 계산과 태스크 계약을 확인합니다.

시나리오:

1. DAG 구조 — dag_id, task 4개, raw_to_bronze -> validate_bronze -> bronze_to_silver -> validate_silver 의존 순서
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


def test_DAG_는_네_태스크를_갖고_raw_to_bronze_validate_bronze_bronze_to_silver_validate_silver_순서다():
    assert DAG.dag_id == DAG_ID
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze", "validate_silver"}
    assert DAG.get_task("validate_bronze").upstream_task_ids == {"raw_to_bronze"}
    assert DAG.get_task("validate_bronze").downstream_task_ids == {"bronze_to_silver"}
    assert DAG.get_task("bronze_to_silver").upstream_task_ids == {"validate_bronze"}
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.get_task("validate_silver").upstream_task_ids == {"raw_to_bronze", "bronze_to_silver"}


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


# --- 수집 가능한 연월 선택 (#345) -----------------------------------------
#
# TLC 는 두 달쯤 늦게 공개해서 직전 달 원본은 존재한 적이 없습니다. 달력으로
# 정하면 스케줄 실행이 매번 죽습니다. 네트워크를 타지 않도록 존재 확인 함수만
# 가짜로 바꾸고, 선택 로직 자체는 진짜를 돌립니다.

resolve_collectable_year_month = dag_module.resolve_collectable_year_month
AUG_2026 = datetime(2026, 8, 12, tzinfo=timezone.utc)


def availability(*published: str):
    """`published` 에 있는 연월만 TLC 에 올라와 있다고 답하는 가짜."""

    def _is_available(year_str: str, month_str: str) -> bool:
        return f"{year_str}-{month_str}" in published

    return _is_available


def collected(base_dir, *year_months: str):
    for year_month in year_months:
        partition = base_dir / "hvfhv" / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / "20260813T000000Z.parquet").touch()
    return str(base_dir)


def test_직전_달이_아직_공개되지_않았으면_공개된_최신_달을_고른다(tmp_path):
    base_dir = collected(tmp_path)  # 받은 것 없음

    target = resolve_collectable_year_month(
        AUG_2026, {}, base_dir, availability("2026-06", "2026-05")
    )

    assert target == ("2026", "06")  # 직전 달인 2026-07 이 아님


def test_이미_받은_달은_다시_받지_않는다(tmp_path):
    """새 달이 나올 때까지 매달 수백 MB 를 다시 받으면 파티션에 파일만 쌓입니다."""
    base_dir = collected(tmp_path, "2026-06")

    target = resolve_collectable_year_month(
        AUG_2026, {}, base_dir, availability("2026-06", "2026-05")
    )

    assert target == ("2026", "05")


def test_새로_받을_달이_없으면_None_이다(tmp_path):
    """아직 공개되지 않은 것은 오류가 아닙니다 — 태스크는 skip 됩니다."""
    base_dir = collected(tmp_path, "2026-06", "2026-05")

    target = resolve_collectable_year_month(
        AUG_2026, {}, base_dir, availability("2026-06", "2026-05")
    )

    assert target is None


def test_조회는_정해진_개월_수에서_멈춘다(tmp_path):
    """원본이 통째로 사라져도 HEAD 를 무한히 던지지 않아야 합니다."""
    asked: list[str] = []

    def _record(year_str: str, month_str: str) -> bool:
        asked.append(f"{year_str}-{month_str}")
        return False

    assert resolve_collectable_year_month(AUG_2026, {}, str(tmp_path), _record) is None
    assert len(asked) == dag_module.MAX_MONTH_LOOKBACK
    assert asked[0] == "2026-07"  # 직전 달부터 거슬러 올라갑니다


def test_수동_연월은_공개_여부와_무관하게_그대로_쓴다(tmp_path):
    """백필은 이미 받은 달을 다시 받는 것이 목적이라 두 조건을 다 건너뜁니다."""
    base_dir = collected(tmp_path, "2024-03")

    def _never(year_str: str, month_str: str) -> bool:
        raise AssertionError("수동 지정이면 공개 여부를 묻지 않아야 합니다")

    target = resolve_collectable_year_month(
        AUG_2026, {"year": "2024", "month": "3"}, base_dir, _never
    )

    assert target == ("2024", "03")
