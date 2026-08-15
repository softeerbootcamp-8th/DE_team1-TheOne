"""기사별 월간 차량 추천 대시보드.

data/gold/ 의 CSV(`driver_car_suggestion`, `driver_aggregation`, `monthly_report`)만 읽는다.
세 데이터셋 모두 `year_month` 단일 그레인 — `spark/jobs/silver_to_gold/job.py` 산출물.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

GOLD_DIR = Path(__file__).resolve().parents[1] / "data" / "gold"

HOUR_BLOCKS = ["00_03", "03_06", "06_09", "09_12", "12_15", "15_18", "18_21", "21_24"]

SUGGESTION_COLUMNS = {
    "driver_id": "기사 ID",
    "service_tier": "서비스 등급",
    "recommended_make_key": "추천 제조사",
    "recommended_model_key": "추천 모델",
    "recommended_model_year": "추천 연식",
    "recommendation_reason": "추천 사유",
    "expected_net_profit_increase": "예상 순이익 증가액",
    "expected_revenue_increase": "예상 매출 증가액",
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


def hourly_ratio_frame(agg_row: pd.Series) -> pd.DataFrame:
    """시간대별 운행 비중을 막대차트 입력 모양으로."""
    return pd.DataFrame(
        {"운행 비중": [round(agg_row[f"ratio_{block}"], 2) for block in HOUR_BLOCKS]}, index=HOUR_BLOCKS
    )


def top_zone_frame(agg_row: pd.Series) -> pd.DataFrame:
    """상위 3개 zone 을 순위별 표 모양으로."""
    return pd.DataFrame(
        {
            "zone_id": [agg_row["top1_zone_id"], agg_row["top2_zone_id"], agg_row["top3_zone_id"]],
            "비중": [
                round(agg_row["top1_zone_ratio"], 2),
                round(agg_row["top2_zone_ratio"], 2),
                round(agg_row["top3_zone_ratio"], 2),
            ],
        },
        index=["1위", "2위", "3위"],
    )


def render() -> None:
    st.set_page_config(page_title="기사 차량 추천", layout="wide")
    st.title("기사별 월간 차량 추천")

    report = load("monthly_report")
    suggestion = load("driver_car_suggestion")
    aggregation = load("driver_aggregation")

    if report.empty or suggestion.empty:
        st.error("data/gold 가 비어 있습니다. spark/jobs/silver_to_gold/job.py 를 먼저 실행하세요.")
        st.stop()

    period = st.selectbox("월", sorted(report["year_month"].unique(), reverse=True))
    report_row = report[report["year_month"] == period].iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("추천 대상 기사", f"{int(report_row['recommended_driver_count'])}명")
    c2.metric("기사 1인당 평균 순이익 증가", f"${report_row['avg_net_profit_increase_per_driver']:,.2f}")
    c3.metric("기사 1인당 평균 매출 증가", f"${report_row['avg_revenue_increase_per_driver']:,.2f}")
    c4.metric("총 매출 증가", f"${report_row['total_revenue_increase']:,.0f}")
    st.caption(f"순이익 증가 임계값 ${report_row['threshold_profit_increase']:,.0f} 이상인 기사만 집계")

    scope = (
        suggestion[(suggestion["year_month"] == period) & (suggestion["expected_revenue_increase"] > 0)]
        .sort_values("expected_revenue_increase", ascending=False)
        .reset_index(drop=True)
    )

    st.subheader("차량 추천 리스트")
    st.caption("매출 증가액 > $0 인 기사만 표시, 매출 증가액 내림차순. 행을 선택하면 기사 상세가 표시됩니다.")
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
    driver = picked["driver_id"]
    detail = aggregation[(aggregation["driver_id"] == driver) & (aggregation["year_month"] == period)]

    d1, d2, d3 = st.columns(3)
    d1.metric("추천 차량", f"{picked['recommended_make_key']} {picked['recommended_model_key']}")
    d2.metric("예상 월 순이익 증가", f"${picked['expected_net_profit_increase']:,.2f}")
    d3.metric("예상 월 매출 증가", f"${picked['expected_revenue_increase']:,.2f}")
    st.caption(picked["recommendation_reason"])

    if detail.empty:
        st.info("`driver_aggregation` 에 이 기사·월의 운행 데이터가 없습니다.")
        return

    agg_row = detail.iloc[0]
    st.bar_chart(hourly_ratio_frame(agg_row))
    st.table(top_zone_frame(agg_row))

    e1, e2, e3 = st.columns(3)
    e1.metric("현재 월 순이익", f"${agg_row['monthly_net_profit']:,.2f}")
    e2.metric("현재 월 렌트료", f"${agg_row['monthly_rental_fee']:,.2f}")
    e3.metric("현재 월 연료비", f"${agg_row['monthly_fuel_cost']:,.2f}")


if __name__ == "__main__":
    render()
