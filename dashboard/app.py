"""차량 교체 이득 콜 리스트 대시보드 (주간 / 월간).

data/gold/ 의 Parquet 만 읽는다. 주간 그레인은 마트를 그대로 그리고, 월간 그레인만
``rollup_month`` 로 합산한다 — 그 함수 docstring 의 ponytail 주석 참고.
"""

from pathlib import Path

import pandas as pd
import streamlit as st

GOLD_DIR = Path(__file__).resolve().parents[1] / "data" / "gold"

# 콜 대상 자격 — 기사의 월 이득이 이 값 이상이어야 한다. 주간 뷰에는 안 쓴다(주간 최대
# 이득이 $408 수준이라 월 기준을 그대로 씌우면 전원 탈락한다).
MIN_MONTHLY_GAIN_USD = 600.0

# 월 합산이 가능한 금액 컬럼 (주간 값의 단순 합).
SUM_COLS = [
    "driver_net_gain_usd", "company_arpu_gain_usd",
    "gain_from_fuel_usd", "gain_from_tier_usd", "cost_from_lease_usd",
]
CALL_COLUMNS = [
    "rank", "driver_id", "current_vehicle_label", "recommended_vehicle_label",
    "driver_net_gain_usd", "company_arpu_gain_usd", "miles",
    "top_pickup_borough", "tenure_days", "reason_text",
]


def month_key(week_start: pd.Series) -> pd.Series:
    """주차가 속한 달. ISO 관례대로 그 주의 목요일 기준 — 2025-12-29 주는 목요일이
    2026-01-01 이라 2026-01 로 묶인다. week_start 의 달로 묶으면 1월 운행 대부분이
    2025-12 로 새어 나간다."""
    return (pd.to_datetime(week_start.astype(str)) + pd.Timedelta(days=3)).dt.to_period("M").astype(str)


@st.cache_data
def load(dataset: str) -> pd.DataFrame:
    """디렉터리째 읽는다 — week_start 는 hive 파티션 키라 파일 하나씩 읽으면 사라진다."""
    path = GOLD_DIR / dataset
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if "week_start" in frame.columns:
        frame["month"] = month_key(frame["week_start"])
    return frame


def eligible(frame: pd.DataFrame, min_gain: float) -> pd.DataFrame:
    """콜 대상 자격 — 기사 이득이 기준 이상 + 회사 객단가도 늘고 + 실행 가능.

    기사별 1위 차량만 담긴 프레임을 받는다(주간은 rank_in_driver=1, 월간은 합산 후 1위).
    """
    return frame[
        (frame["driver_net_gain_usd"] >= min_gain)
        & (frame["company_arpu_gain_usd"] > 0)
        & frame["is_feasible"]
    ]


def reason_text(row) -> str:
    """양수인 기여 항목만 큰 순으로. gold_mart_top_customers.reason_text 와 같은 규칙."""
    parts = [
        ("등급 상승", row["gain_from_tier_usd"]),
        ("연료비 절감", row["gain_from_fuel_usd"]),
        ("렌트료 절감", -row["cost_from_lease_usd"]),
    ]
    parts = sorted([p for p in parts if p[1] > 0], key=lambda p: -p[1])
    return ", ".join(f"{name} +${value:,.1f}" for name, value in parts)


def rollup(swap_scope: pd.DataFrame, fleet: pd.DataFrame, min_gain: float):
    """한 기간(주 또는 월)의 교체 시뮬레이션을 기사 단위로 합산해 콜 리스트와 KPI를 만든다.

    주간·월간이 같은 경로를 탄다. 주간은 기간이 1주라 합산이 항등이고, 월간만 4~5주가
    실제로 합쳐진다 — 그래서 월간 1위 차량은 주간 1위와 다를 수 있다.

    ponytail: gold_mart_* 의 정의(자격 3조건 → 기사별 1위)를 여기서 되풀이한다. 원래 그
    정의는 spark 쪽 transformer 한 곳에만 있어야 하지만 그 소스가 지금 저장소에 없다
    (`spark/jobs/silver_to_gold/vehicle_swap/` 에 __pycache__ 만 남음). 마트가 자격자
    전원을 담게 다시 구워지면 이 함수는 지우고 마트를 그대로 읽는다.
    """
    best = (
        swap_scope.groupby(
            ["driver_id", "make_key", "model_key", "current_make_key", "current_model_key"],
            as_index=False, observed=True,
        )
        .agg(
            **{column: (column, "sum") for column in SUM_COLS},
            tier_upgraded=("tier_upgraded", "any"),
            is_feasible=("is_feasible", "all"),
        )
    )
    rank = best.groupby("driver_id")["driver_net_gain_usd"].rank("first", ascending=False)
    best = best[rank == 1]

    facts = fleet.groupby("driver_id", as_index=False, observed=True).agg(
        miles=("total_miles", "sum"),
        tenure_days=("tenure_days", "max"),
        net_earnings_usd=("net_earnings_usd", "sum"),
        top_pickup_borough=("top_pickup_borough", lambda s: s.mode().iat[0]),
    )

    pool = eligible(best, min_gain)
    call_list = (
        pool.sort_values("driver_net_gain_usd", ascending=False)
        .merge(facts, on="driver_id", how="left")
        .reset_index(drop=True)
    )
    call_list["rank"] = call_list.index + 1
    call_list["current_vehicle_label"] = call_list["current_make_key"] + " " + call_list["current_model_key"]
    call_list["recommended_vehicle_label"] = call_list["make_key"] + " " + call_list["model_key"]
    call_list["reason_text"] = call_list.apply(reason_text, axis=1) if not call_list.empty else ""

    kpi = {
        "total_arpu_gain_usd": call_list["company_arpu_gain_usd"].sum(),
        "target_customer_count": len(call_list),
        "avg_driver_net_gain_usd": call_list["driver_net_gain_usd"].mean(),
        "tier_upgrade_count": int(best["tier_upgraded"].sum()),
        "avg_fleet_net_earnings_usd": facts["net_earnings_usd"].mean(),
        "active_driver_count": fleet["driver_id"].nunique(),
    }
    return kpi, call_list, best


st.set_page_config(page_title="차량 교체 이득", layout="wide")
st.title("차량 교체 콜 리스트")

kpi_mart = load("gold_mart_kpi_weekly")  # 기간 목록과 빈 데이터 확인용. 콜 리스트는 swap 에서 만든다.
swap = load("gold_fct_vehicle_swap_sim")
weekly = load("gold_fct_driver_weekly")
dim = load("gold_dim_vehicle_option")

if kpi_mart.empty:
    st.error("data/gold 가 비어 있습니다. spark/jobs/silver_to_gold/vehicle_swap/job.py 를 먼저 실행하세요.")
    st.stop()

grain = st.radio("집계 단위", ["주간", "월간"], horizontal=True)

if grain == "월간":
    period = st.selectbox("월", sorted(kpi_mart["month"].unique(), reverse=True))
    column, unit, min_gain = "month", "월", MIN_MONTHLY_GAIN_USD
    scope_weeks = kpi_mart[kpi_mart["month"] == period]["week_start"].astype(str)
    st.caption(f"{period} — {len(scope_weeks)}개 주차 합산 ({scope_weeks.min()} ~ {scope_weeks.max()})")
else:
    period = st.selectbox("주차", sorted(kpi_mart["week_start"].astype(str).unique(), reverse=True))
    # 주간에는 월 기준 이득을 씌우지 않는다 — 주간 최대 이득이 $408 수준이라 전원 탈락한다.
    column, unit, min_gain = "week_start", "주", 0.0

swap_scope = swap[swap[column].astype(str) == period]
baseline_scope = weekly[weekly[column].astype(str) == period]
kpi, call_list, detail_source = rollup(swap_scope, baseline_scope, min_gain)
rule = f"기사 {unit} 이득 ≥ ${min_gain:,.0f}"

listed = int(kpi["target_customer_count"])
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("회사 객단가 증가", f"${kpi['total_arpu_gain_usd']:,.0f}")
c2.metric("대상 기사", f"{listed}명")
c3.metric("1인당 평균 기사 이득", f"${kpi['avg_driver_net_gain_usd']:,.1f}")
c4.metric("등급 상승", f"{int(kpi['tier_upgrade_count'])}건")
c5.metric("운행 기사", f"{int(kpi['active_driver_count'])}명")
st.caption(
    f"자격 기준: {rule} + 회사 객단가 증가 > $0 + 실행 가능(is_feasible). "
    "카드와 콜 리스트 모두 자격자 전원 기준입니다."
)

st.subheader("콜 리스트")
st.dataframe(
    call_list[CALL_COLUMNS].rename(
        columns={
            "company_arpu_gain_usd": f"회사 객단가 증가($/{unit})",
            "driver_net_gain_usd": f"기사 이득($/{unit})",
            "miles": f"주행거리(mile/{unit})",
        }
    ),
    width="stretch",
    hide_index=True,
)

st.subheader("기여도 분해")
if call_list.empty:
    st.info("이 기간에는 기사도 회사도 이득인 조합이 없습니다.")
else:
    driver = st.selectbox("기사", call_list["driver_id"].tolist())
    picked = detail_source[detail_source["driver_id"] == driver]
    if not picked.empty:
        detail = picked.iloc[0]
        st.bar_chart(
            pd.DataFrame(
                {
                    f"USD/{unit}": [
                        detail["gain_from_fuel_usd"],
                        detail["gain_from_tier_usd"],
                        -detail["cost_from_lease_usd"],
                    ]
                },
                index=["연비", "등급 상승", "렌트료"],
            )
        )

with st.expander("후보 차량 12종"):
    st.caption("`spec_match_level` 이 MODEL 이 아니면 제원이 폴백된 값입니다.")
    st.dataframe(
        dim[[
            "make_key", "model_key", "weekly_price_usd", "combined_mpg",
            "energy_cost_per_mile_usd", "is_uber_comfort_eligible",
            "is_lyft_extra_comfort_eligible", "spec_match_level",
        ]],
        width="stretch",
        hide_index=True,
    )

baseline_weeks = int(baseline_scope["baseline_week_count"].min())
if baseline_weeks < 4:
    st.warning(
        f"기준선이 {baseline_weeks}주짜리입니다. 4주 중앙값이 아니라 추천이 흔들릴 수 있습니다."
    )

st.caption(
    "등급 상승 매출 프리미엄은 `estimated_service_tier`(OD 중앙값의 1.15배 이상)에서 역산한 "
    "**상한 추정치**입니다. 실제 승급으로 매출이 이만큼 오른다는 근거가 아닙니다. "
    f"에너지 단가 기준일: {dim['energy_price_date'].iloc[0]}"
)
