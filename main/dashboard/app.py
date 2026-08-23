"""기사별 월간 차량 추천 대시보드.

Gold 3종(`driver_car_suggestion`, `driver_aggregation`, `monthly_report`)을 읽는다.
`DASHBOARD_DATA_SOURCE` 환경변수로 로컬 CSV(기본)/RDS를 전환한다 — `datasource.py` 참고.
세 데이터셋 모두 `service_area`, `year_month` 그레인 — Gold job 산출물.

화면 구성은 위에서 아래로 한 줄기다.
    필터 한 줄 → 히어로(회사 총 매출 증가) → 지표 타일 → 분포·차종 → 선정 게이트 → 리스트 → 기사 상세
필터는 화면 전체를 한 번에 스코프한다 (차트별 필터를 두지 않는다).
"""

import pandas as pd
import streamlit as st

import charts
import theme
from datasource import build_data_source

_DATA_SOURCE = build_data_source()

SUGGESTION_COLUMNS = {
    "driver_id": "기사 ID",
    "manufacturer": "추천 제조사",
    "model_name": "추천 모델",
    "model_year": "추천 연식",
    "recommendation_reason": "추천 사유",
    "expected_net_profit_increase": "기사 예상 월 순수익 증가",
    "expected_revenue_increase": "회사 월 렌탈 객단가 증가",
}


@st.cache_data(ttl=5)
def load(dataset: str) -> pd.DataFrame:
    return _DATA_SOURCE.load(dataset)


def recommendation_scope(
    suggestion: pd.DataFrame,
    aggregation: pd.DataFrame,
    service_area: str,
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
        (suggestion["service_area"] == service_area)
        & (suggestion["year_month"] == period)
        & (suggestion["expected_net_profit_increase"] >= threshold)
        & (suggestion["expected_revenue_increase"] > 0)
    ]
    return (
        eligible.merge(
            current,
            on=["service_area", "driver_id", "year_month"],
            how="inner",
        )
        .sort_values("expected_net_profit_increase", ascending=False)
        .reset_index(drop=True)
    )


# ── 표시 헬퍼 ──────────────────────────────────────────────────────────────────


def _money(value: float) -> str:
    """큰 값은 줄여 쓴다 — 히어로·타일용."""
    magnitude = abs(value)
    if magnitude >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    if magnitude >= 10_000:
        return f"${value / 1_000:,.1f}K"
    return f"${value:,.0f}"


def _head(period: str, generated_rows: int) -> None:
    st.markdown(
        f"""
        <div class="dash-head">
          <div>
            <h1>기사별 월간 차량 추천</h1>
            <p>Gold 산출물 기준 예상치 · 기사 순수익과 회사 렌탈 매출이 함께 늘어나는 교체 후보</p>
          </div>
          <div style="display:flex; gap:.5rem; align-items:center;">
            <span class="pill">{period}</span>
            <span class="pill pill--ghost">기사 {generated_rows:,}명 분석</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="sect"><h2>{title}</h2><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )


def _hero(
    total_revenue_increase: float,
    driver_count: int,
    avg_profit: float,
    analyzed: int,
) -> None:
    share = driver_count / analyzed if analyzed else 0.0
    st.markdown(
        f"""
        <div class="hero">
          <div>
            <p class="hero__label">예상 월 렌탈 매출 증가액</p>
            <p class="hero__value">{_money(total_revenue_increase)}</p>
            <p class="hero__note">
              추천 대상 기사 {driver_count:,}명이 <b>모두 교체할 경우</b>
              · 기사 1인당 예상 월 순수익 +${avg_profit:,.0f}
            </p>
          </div>
          <div class="hero__side">
            <p class="hero__label">교체 제안 비율</p>
            <p class="hero__side-value">{share:.1%}</p>
            <p class="hero__note">분석 대상 {analyzed:,}명 기준</p>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _aggregates(scope: pd.DataFrame) -> dict[str, float]:
    if scope.empty:
        return {"count": 0, "avg_profit": 0.0, "avg_revenue": 0.0, "total_revenue": 0.0}
    return {
        "count": len(scope),
        "avg_profit": float(scope["expected_net_profit_increase"].mean()),
        "avg_revenue": float(scope["expected_revenue_increase"].mean()),
        "total_revenue": float(scope["expected_revenue_increase"].sum()),
    }


def _previous_period(periods: list[str], period: str) -> str | None:
    """정렬된 월 목록에서 바로 앞 달. 없으면 None — 델타를 숨기는 신호."""
    ordered = sorted(periods)
    index = ordered.index(period)
    return ordered[index - 1] if index > 0 else None


def _delta(current: float, previous: float | None, money: bool = True) -> str | None:
    if previous is None:
        return None
    diff = current - previous
    return f"{diff:+,.0f}" if not money else f"${diff:+,.0f}"


# ── 화면 ───────────────────────────────────────────────────────────────────────


def render() -> None:
    st.set_page_config(
        page_title="기사 차량 추천",
        page_icon="🚕",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    mode = theme.mode()
    theme.inject_css(mode)

    report = load("monthly_report")
    suggestion = load("driver_car_suggestion")
    aggregation = load("driver_aggregation")

    if report.empty or suggestion.empty or aggregation.empty:
        st.error(
            "data/gold 가 비어 있습니다. "
            "main/spark/jobs/silver_to_gold/job.py 를 먼저 실행하세요."
        )
        st.stop()

    # 제목은 필터보다 위에 보여야 하니 자리를 먼저 잡고, 값이 정해진 뒤 채운다.
    head_slot = st.container()

    # ── 필터 한 줄: 아래 모든 카드·차트·표가 이 값으로 스코프된다 ──
    f1, f2, f3 = st.columns([1, 1, 2], vertical_alignment="bottom")
    service_area = f1.selectbox("지역", sorted(report["service_area"].unique()))
    area_report = report[report["service_area"] == service_area]
    periods = sorted(area_report["year_month"].unique(), reverse=True)
    period = f2.selectbox("월", periods)

    report_row = area_report[area_report["year_month"] == period].iloc[0]
    gold_threshold = float(report_row["threshold_profit_increase"])
    month_suggestion = suggestion[
        (suggestion["service_area"] == service_area)
        & (suggestion["year_month"] == period)
    ]

    threshold = f3.slider(
        "기사 예상 월 순수익 증가 하한 ($)",
        min_value=0.0,
        max_value=float(month_suggestion["expected_net_profit_increase"].max()),
        value=gold_threshold,
        step=50.0,
        format="$%d",
        help=rf"Gold 월간 리포트 기준값은 \${gold_threshold:,.0f} 입니다.",
    )

    scope = recommendation_scope(
        suggestion, aggregation, service_area, period, threshold
    )

    with head_slot:
        _head(period, len(month_suggestion))

    if threshold != gold_threshold:
        st.caption(
            rf"Gold 리포트 기준(\${gold_threshold:,.0f}, "
            f"{int(report_row['recommended_driver_count']):,}명) 대신 "
            rf"하한 \${threshold:,.0f} 로 다시 계산한 값입니다."
        )

    agg = _aggregates(scope)
    _hero(agg["total_revenue"], int(agg["count"]), agg["avg_profit"],
          len(month_suggestion))

    # 지난 달 대비 델타 — 같은 하한을 적용해 비교 기준을 맞춘다.
    previous = _previous_period(list(area_report["year_month"].unique()), period)
    prev_agg = (
        _aggregates(
            recommendation_scope(
                suggestion, aggregation, service_area, previous, threshold
            )
        )
        if previous
        else None
    )

    st.write("")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric(
        "추천 대상 기사",
        f"{int(agg['count']):,}명",
        delta=_delta(agg["count"], prev_agg["count"] if prev_agg else None, money=False),
        help=f"{period} 분석 대상 {len(month_suggestion):,}명 중"
        f" {agg['count'] / max(len(month_suggestion), 1):.1%}",
    )
    t2.metric(
        "기사 1인당 예상 월 순수익 증가",
        f"${agg['avg_profit']:,.0f}",
        delta=_delta(agg["avg_profit"], prev_agg["avg_profit"] if prev_agg else None),
    )
    t3.metric(
        "회사 평균 예상 월 렌탈 객단가 증가",
        f"${agg['avg_revenue']:,.0f}",
        delta=_delta(agg["avg_revenue"], prev_agg["avg_revenue"] if prev_agg else None),
    )
    t4.metric(
        "추천 차종 수",
        f"{len(scope[['manufacturer', 'model_name', 'model_year']].drop_duplicates())}종",
        help="추천 결과에 등장한 서로 다른 제조사·모델·연식 조합",
    )
    st.caption(
        rf"선정 기준 · 기사 예상 월 순수익 증가 \${threshold:,.0f} 이상 "
        r"· 회사 월 렌탈 객단가 증가 \$0 초과"
    )

    if scope.empty:
        st.info("이 조건에는 해당하는 기사가 없습니다. 하한을 낮춰 보세요.")
        return

    # ── 분포 · 차종 · 사유 ──
    _section("추천 규모", "얼마나 오르는 기사가 몇 명인지, 무엇을 타다 무엇으로 가는지")
    c1, c2 = st.columns([1.15, 1])
    with c1.container(key="card-dist"):
        st.markdown(
            "**기사 예상 월 순수익 증가 분포**  \n"
            rf"<span class='sub'>평균 \${agg['avg_profit']:,.0f} · "
            rf"중위 \${scope['expected_net_profit_increase'].median():,.0f}</span>",
            unsafe_allow_html=True,
        )
        st.altair_chart(
            charts.profit_distribution(scope, mode), width="stretch", theme=None
        )
    # 현재 차종과 추천 차종은 같은 스펙의 작은 배수로 나란히 둔다 — 무엇을 타다 무엇으로.
    with c2.container(key="card-fleet"):
        st.markdown(
            "**차종 이동**  \n<span class='sub'>기사 수 기준</span>",
            unsafe_allow_html=True,
        )
        st.markdown("<span class='sub'>현재 차종</span>", unsafe_allow_html=True)
        st.altair_chart(
            charts.count_bars(
                scope["current_manufacturer"] + " " + scope["current_model_name"], mode
            ),
            width="stretch",
            theme=None,
        )
        st.markdown("<span class='sub'>추천 차종</span>", unsafe_allow_html=True)
        st.altair_chart(
            charts.count_bars(
                scope["manufacturer"] + " " + scope["model_name"], mode
            ),
            width="stretch",
            theme=None,
        )

    # ── 선정 게이트 (진단용이라 기본 접힘) ──
    with st.expander(f"선정 게이트 — {period} 전체 기사 {len(month_suggestion):,}명 중 어디서 걸러지나"):
        st.altair_chart(
            charts.eligibility_gate(month_suggestion, threshold, mode),
            width="stretch",
            theme=None,
        )
        st.caption(
            r"가로 기준선은 회사 객단가 증가 \$0, 세로 기준선은 기사 순수익 증가 하한입니다. "
            "오른쪽 위 사분면만 선정됩니다."
        )

    # ── 리스트 ──
    _section("추천 리스트", "행을 클릭하면 아래에 기사 상세가 열립니다")
    display = scope[list(SUGGESTION_COLUMNS)].rename(columns=SUGGESTION_COLUMNS).round(2)
    event = st.dataframe(
        display,
        width="stretch",
        height=380,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=f"suggestion_table_{period}",
        column_config={
            "기사 ID": st.column_config.TextColumn(width="small"),
            "추천 연식": st.column_config.NumberColumn(format="%d", width="small"),
            "추천 사유": st.column_config.TextColumn(width="large"),
            "기사 예상 월 순수익 증가": st.column_config.NumberColumn(format="$%.0f"),
            "회사 월 렌탈 객단가 증가": st.column_config.NumberColumn(format="$%.0f"),
        },
    )

    # ── 기사 상세 ──
    selected_rows = event.selection.rows if event and event.selection else []
    if not selected_rows:
        _section("기사 상세", "리스트에서 기사를 선택하면 현재 차량과 추천 차량을 나란히 비교합니다")
        return

    picked = scope.iloc[selected_rows[0]]
    _section(f"기사 상세 · {picked['driver_id']}", str(picked["recommendation_reason"]))

    with st.container(key="card-detail"):
        st.markdown(
            f"""
            <div class="swap">
              <span>현재 <b>{picked['current_manufacturer']} {picked['current_model_name']}
              ({int(picked['current_model_year'])})</b></span>
              <span class="arrow">→</span>
              <span>추천 <b>{picked['manufacturer']} {picked['model_name']}
              ({int(picked['model_year'])})</b></span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.write("")

        # 단위가 다른 세 지표라 축을 합치지 않고 작은 배수로 쪼갠다.
        d1, d2, d3 = st.columns(3)
        with d1:
            st.altair_chart(
                charts.current_vs_recommended(
                    "월 리스료",
                    float(picked["current_monthly_lease_fee"]),
                    float(picked["recommended_monthly_lease_fee"]),
                    mode,
                    lower_is_better=True,
                ),
                width="stretch",
                theme=None,
            )
        with d2:
            st.altair_chart(
                charts.current_vs_recommended(
                    "월 연료비",
                    float(picked["current_monthly_fuel_cost"]),
                    float(picked["expected_monthly_fuel_cost"]),
                    mode,
                    lower_is_better=True,
                ),
                width="stretch",
                theme=None,
            )
        with d3:
            st.altair_chart(
                charts.current_vs_recommended(
                    "월 순이익",
                    float(picked["current_monthly_net_profit"]),
                    float(picked["expected_monthly_net_profit"]),
                    mode,
                    lower_is_better=False,
                ),
                width="stretch",
                theme=None,
            )

        st.markdown(
            f"""
            <div class="chips">
              <span class="chip">기사 순수익 증가 <b>${picked['expected_net_profit_increase']:,.0f}</b></span>
              <span class="chip">회사 객단가 증가 <b>${picked['expected_revenue_increase']:,.0f}</b></span>
              <span class="chip">월 주행 <b>{picked['monthly_mileage']:,.0f} mile</b></span>
              <span class="chip">플랫폼 정산 <b>${picked['monthly_driver_pay']:,.0f}</b></span>
              <span class="chip">팁 <b>${picked['monthly_tips']:,.0f}</b></span>
            </div>
            """,
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    render()
