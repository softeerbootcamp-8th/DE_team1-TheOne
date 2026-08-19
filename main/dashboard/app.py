"""기사별 월간 차량 추천 대시보드.

data/gold/ 의 CSV(`driver_car_suggestion`, `driver_aggregation`, `monthly_report`)만 읽는다.
세 데이터셋 모두 `year_month` 단일 그레인 — `main/spark/jobs/silver_to_gold/job.py` 산출물.
"""

import os
from pathlib import Path

import pandas as pd
import streamlit as st

GOLD_DIR = Path(
    os.environ.get(
        "GOLD_DIR",
        Path(__file__).resolve().parents[2] / "data" / "gold",
    )
)

SUGGESTION_COLUMNS = {
    "driver_id": "기사 ID",
    "manufacturer": "추천 제조사",
    "model_name": "추천 모델",
    "model_year": "추천 연식",
    "recommendation_reason": "추천 사유",
    "expected_net_profit_increase": "기사 예상 월 순수익 증가",
    "expected_revenue_increase": "회사 월 렌탈 객단가 증가",
}


def _read_partitions(root: Path, dataset: str) -> pd.DataFrame:
    """`year_month=` 파티션 전체를 이어붙인다 — 컬럼에도 `year_month` 가 그대로 들어있다."""
    paths = sorted(root.glob(f"{dataset}/year_month=*/{dataset}.csv"))
    if not paths:
        return pd.DataFrame()
    return pd.concat((pd.read_csv(p) for p in paths), ignore_index=True)


@st.cache_data
def load(dataset: str) -> pd.DataFrame:
    return _read_partitions(GOLD_DIR, dataset)


def recommendation_scope(
    suggestion: pd.DataFrame,
    aggregation: pd.DataFrame,
    period: str,
    threshold: float,
) -> pd.DataFrame:
    """Gold 월간 리포트와 같은 기준을 통과한 기사에 현재 차량 정보를 붙입니다."""
    current = aggregation.rename(
        columns={
            "manufacturer": "current_manufacturer",
            "model_name": "current_model_name",
            "model_year": "current_model_year",
            "monthly_lease_fee": "current_monthly_lease_fee",
            "monthly_fuel_cost": "current_monthly_fuel_cost",
            "monthly_net_profit": "current_monthly_net_profit",
        }
    )
    eligible = suggestion[
        (suggestion["year_month"] == period)
        & (suggestion["expected_net_profit_increase"] >= threshold)
        & (suggestion["expected_revenue_increase"] > 0)
    ]
    return (
        eligible.merge(current, on=["driver_id", "year_month"], how="inner")
        .sort_values("expected_net_profit_increase", ascending=False)
        .reset_index(drop=True)
    )


def render() -> None:
    st.set_page_config(page_title="기사 차량 추천", layout="wide")
    st.title("기사별 월간 차량 추천")

    report = load("monthly_report")
    suggestion = load("driver_car_suggestion")
    aggregation = load("driver_aggregation")

    if report.empty or suggestion.empty or aggregation.empty:
        st.error("data/gold 가 비어 있습니다. main/spark/jobs/silver_to_gold/job.py 를 먼저 실행하세요.")
        st.stop()

    period = st.selectbox("월", sorted(report["year_month"].unique(), reverse=True))
    report_row = report[report["year_month"] == period].iloc[0]
    threshold = float(report_row["threshold_profit_increase"])
    scope = recommendation_scope(suggestion, aggregation, period, threshold)

    avg_profit_increase = (
        scope["expected_net_profit_increase"].mean() if not scope.empty else 0.0
    )
    avg_revenue_increase = (
        scope["expected_revenue_increase"].mean() if not scope.empty else 0.0
    )
    total_revenue_increase = scope["expected_revenue_increase"].sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("추천 대상 기사", f"{len(scope)}명")
    c2.metric("기사 1인당 예상 월 순수익 증가", f"${avg_profit_increase:,.2f}")
    c3.metric("회사 평균 월 렌탈 객단가 증가", f"${avg_revenue_increase:,.2f}")
    c4.metric("회사 월 렌탈 매출 총 증가", f"${total_revenue_increase:,.0f}")
    st.caption(
        f"기사 예상 월 순수익 ${threshold:,.0f} 이상 · "
        "회사 월 렌탈 객단가 $0 초과"
    )

    st.subheader("차량 추천 리스트")
    st.caption("기사 순수익 기준과 회사 월 렌탈 객단가 상승 조건을 모두 통과한 기사만 표시합니다.")
    display = scope[list(SUGGESTION_COLUMNS)].rename(columns=SUGGESTION_COLUMNS)
    display = display.round(2)
    event = st.dataframe(
        display,
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=f"suggestion_table_{period}",
    )

    st.subheader("기사 상세")
    if scope.empty:
        st.info("이 달에는 매출 증가액이 있는 추천 대상 기사가 없습니다.")
        return

    selected_rows = event.selection.rows if event and event.selection else []
    if not selected_rows:
        st.info("리스트에서 행을 클릭하면 기사 상세가 표시됩니다.")
        return

    picked = scope.iloc[selected_rows[0]]
    d1, d2, d3 = st.columns(3)
    d1.metric("추천 차량", f"{picked['manufacturer']} {picked['model_name']}")
    d2.metric("기사 예상 월 순수익 증가", f"${picked['expected_net_profit_increase']:,.2f}")
    d3.metric("회사 월 렌탈 객단가 증가", f"${picked['expected_revenue_increase']:,.2f}")
    st.info(f"추천 사유: {picked['recommendation_reason']}")

    st.write(
        "현재 차량: "
        f"{picked['current_manufacturer']} {picked['current_model_name']} "
        f"({int(picked['current_model_year'])}) → "
        f"추천 차량: {picked['manufacturer']} {picked['model_name']} "
        f"({int(picked['model_year'])})"
    )

    e1, e2, e3 = st.columns(3)
    e1.metric(
        "월 리스료",
        f"${picked['recommended_monthly_lease_fee']:,.2f}",
        delta=f"${picked['recommended_monthly_lease_fee'] - picked['current_monthly_lease_fee']:,.2f}",
        delta_color="inverse",
    )
    e2.metric(
        "월 연료비",
        f"${picked['expected_monthly_fuel_cost']:,.2f}",
        delta=f"${picked['expected_monthly_fuel_cost'] - picked['current_monthly_fuel_cost']:,.2f}",
        delta_color="inverse",
    )
    e3.metric(
        "월 순이익",
        f"${picked['expected_monthly_net_profit']:,.2f}",
        delta=f"${picked['expected_net_profit_increase']:,.2f}",
    )

    st.caption(
        f"현재 순이익 ${picked['current_monthly_net_profit']:,.2f} · "
        f"월 주행 {picked['monthly_mileage']:,.1f} mile · "
        f"플랫폼 정산 ${picked['monthly_driver_pay']:,.2f} · "
        f"팁 ${picked['monthly_tips']:,.2f}"
    )


if __name__ == "__main__":
    render()
