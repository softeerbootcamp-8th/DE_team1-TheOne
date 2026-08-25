"""기사별 월간 차량 추천 대시보드.

Gold 4종(`driver_car_suggestion`, `driver_aggregation`, `recommendation_algorithm`,
`silver_lineage`)을 읽는다. `DASHBOARD_DATA_SOURCE` 환경변수로 로컬 CSV(기본)/RDS를
전환한다 — `datasource.py` 참고.

화면 구성은 위에서 아래로 한 줄기다.
    알고리즘·threshold 선택 → 지역·월 필터 → 히어로(회사 총 매출 증가) → 지표 타일
    → 분포·차종 → 선정 게이트 → 리스트 → 기사 상세
필터는 화면 전체를 한 번에 스코프한다 (차트별 필터를 두지 않는다).
"""

import pandas as pd
import streamlit as st

import charts
import theme
from datasource import DataSource, build_data_source

# 기사 예상 월 순수익 증가 하한 기본값 (USD). Gold monthly_report 제거(#915) 이후
# 더 이상 Gold가 계산해주지 않아 대시보드 상수로 둔다. threshold를 안 쓰는 알고리즘
# (v1)에는 이 값을 그대로 쓴다.
DEFAULT_THRESHOLD = 500.0

# schema.gold.DriverCarSuggestion.threshold의 sentinel — 알고리즘이 threshold를
# 쓰지 않으면 이 값으로 통일해 적재된다(#997). 실제 threshold는 항상 0 이상이라
# 구분된다.
NO_THRESHOLD = -1

SUGGESTION_COLUMNS = {
    "driver_id": "기사 ID",
    "manufacturer": "추천 제조사",
    "model_name": "추천 모델",
    "model_year": "추천 연식",
    "recommendation_reason": "추천 사유",
    "expected_net_profit_increase": "기사 예상 월 순수익 증가",
    "expected_revenue_increase": "회사 월 렌탈 객단가 증가",
}


@st.cache_resource
def _data_source() -> DataSource:
    """RDS 연결은 재실행마다 새로 맺지 않고 프로세스 생존 기간 동안 재사용한다.

    데이터셋이 2종에서 4종(#987)으로 늘면서 매 렌더링마다 새 연결을 맺으면
    (SSH 터널 경유 시 연결당 ~150-300ms) 왕복 비용만 1초 가까이 쌓였다.
    """
    return build_data_source()


@st.cache_data(ttl=5)
def load(dataset: str) -> pd.DataFrame:
    return _data_source().load(dataset)


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


def _silver_source_expander(
    lineage: pd.DataFrame, service_area: str, period: str
) -> None:
    """맨 아래 접힌 상태로 이번 실행이 읽은 Silver 4종 경로를 보여준다.

    lineage 는 지역·월 당 가장 최근 실행 한 행만 담고 있다(algorithm 축 없음) —
    선택된 알고리즘 버전이 최신 실행과 다르면 그 실행의 출처와는 다를 수 있다.
    """
    with st.expander("Silver 데이터 출처", expanded=False):
        matched = (
            lineage[
                (lineage["service_area"] == service_area)
                & (lineage["year_month"] == period)
            ]
            if not lineage.empty
            else lineage
        )
        if matched.empty:
            st.caption("이 지역·월의 Silver 출처 정보가 없습니다.")
            return
        row = matched.iloc[0]
        st.caption(f"운행 기록: {row['silver_monthly_taxi_trip_s3_link']}")
        st.caption(f"기사 차량 스냅샷: {row['silver_driver_vehicle_monthly_snapshot_s3_link']}")
        st.caption(f"보유 차량: {row['silver_lease_vehicle_inventory_s3_link']}")
        st.caption(f"연료비: {row['silver_gas_ev_price_s3_link']}")


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

    suggestion = load("driver_car_suggestion")
    aggregation = load("driver_aggregation")
    algorithms = load("recommendation_algorithm")
    lineage = load("silver_lineage")

    if suggestion.empty or aggregation.empty:
        st.error(
            "data/gold 가 비어 있습니다. "
            "main/spark/jobs/silver_to_gold/job.py 를 먼저 실행하세요."
        )
        st.stop()

    # 제목은 필터보다 위에 보여야 하니 자리를 먼저 잡고, 값이 정해진 뒤 채운다.
    head_slot = st.container()

    # ── 알고리즘 버전 선택: 이 값으로 suggestion 전체를 좁힌 뒤 나머지 필터를 적용한다 ──
    algo_col, desc_col = st.columns([1, 3], vertical_alignment="bottom")
    algorithm_id = algo_col.selectbox(
        "알고리즘 버전", sorted(suggestion["recommendation_algorithm_version_id"].unique())
    )
    descriptions = (
        dict(zip(algorithms["recommendation_algorithm_version_id"], algorithms["description"]))
        if not algorithms.empty
        else {}
    )
    desc_col.caption(descriptions.get(algorithm_id, "이 버전에 대한 설명이 없습니다."))
    suggestion = suggestion[
        suggestion["recommendation_algorithm_version_id"] == algorithm_id
    ]

    # ── threshold 선택: 이 알고리즘이 실제로 쌓은 값 중에서만 고른다(#998) ──
    # 값이 전부 sentinel이면 이 알고리즘은 threshold 축이 없다는 뜻 — 셀렉트박스
    # 대신 캡션만 보여주고, 기존 기본 하한을 그대로 쓴다. 알고리즘 ID로 분기하지
    # 않아서 나중에 threshold 없는 알고리즘이 늘어도 그대로 통한다.
    if (suggestion["threshold"] == NO_THRESHOLD).all():
        st.caption("이 알고리즘은 임계값을 쓰지 않습니다.")
        threshold = DEFAULT_THRESHOLD
    else:
        available_thresholds = sorted(suggestion["threshold"].unique())
        threshold = st.selectbox(
            "기사 예상 월 순수익 증가 하한 threshold ($)",
            available_thresholds,
            format_func=lambda value: f"${value:,.0f}",
        )
        suggestion = suggestion[suggestion["threshold"] == threshold]

    # ── 필터 한 줄: 아래 모든 카드·차트·표가 이 값으로 스코프된다 ──
    f1, f2 = st.columns(2, vertical_alignment="bottom")
    service_area = f1.selectbox("지역", sorted(suggestion["service_area"].unique()))
    area_suggestion = suggestion[suggestion["service_area"] == service_area]
    periods = sorted(area_suggestion["year_month"].unique(), reverse=True)
    period = f2.selectbox("월", periods)

    month_suggestion = area_suggestion[area_suggestion["year_month"] == period]

    scope = recommendation_scope(
        suggestion, aggregation, service_area, period, threshold
    )

    with head_slot:
        _head(period, len(month_suggestion))

    agg = _aggregates(scope)
    _hero(agg["total_revenue"], int(agg["count"]), agg["avg_profit"],
          len(month_suggestion))

    st.write("")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric(
        "추천 대상 기사",
        f"{int(agg['count']):,}명",
        help=f"{period} 분석 대상 {len(month_suggestion):,}명 중"
        f" {agg['count'] / max(len(month_suggestion), 1):.1%}",
    )
    t2.metric(
        "기사 1인당 예상 월 순수익 증가",
        f"${agg['avg_profit']:,.0f}",
    )
    t3.metric(
        "회사 평균 예상 월 렌탈 객단가 증가",
        f"${agg['avg_revenue']:,.0f}",
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
        _silver_source_expander(lineage, service_area, period)
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
        _silver_source_expander(lineage, service_area, period)
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

    _silver_source_expander(lineage, service_area, period)


if __name__ == "__main__":
    render()
