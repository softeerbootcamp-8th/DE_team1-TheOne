"""대시보드 차트. Altair 스펙만 만들고 Streamlit 호출은 하지 않는다.

규칙 (dataviz):
  - 막대는 24px 이하, 데이터 끝만 4px 라운드, 기준선 쪽은 각.
  - 격자·축은 실선 헤어라인. 점선 금지.
  - 계열이 2개 이상일 때만 범례. 라벨은 선택적으로만 — 모든 점에 숫자를 찍지 않는다.
  - 라벨·값·범례는 텍스트 토큰 색. 계열색은 막대와 점만 입는다.
  - x 축은 하나. 단위가 다른 지표는 같은 축에 겹치지 않고 작은 배수로 쪼갠다.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

import theme

BAR_SIZE = 22
CORNER = 4


def profit_distribution(scope: pd.DataFrame, mode: str) -> alt.Chart:
    """기사별 예상 월 순수익 증가 분포. 단일 계열이라 범례 없음, 평균만 직접 라벨."""
    t = theme.tokens(mode)
    accent = theme.series(mode)[0]
    mean = float(scope["expected_net_profit_increase"].mean())
    frame = scope[["expected_net_profit_increase"]]

    bars = (
        alt.Chart(frame)
        .mark_bar(color=accent, cornerRadiusTopLeft=CORNER, cornerRadiusTopRight=CORNER)
        .encode(
            x=alt.X(
                "expected_net_profit_increase:Q",
                bin=alt.Bin(maxbins=24),
                title="기사 예상 월 순수익 증가 ($)",
                axis=alt.Axis(grid=False, format=",.0f", tickCount=8),
            ),
            y=alt.Y("count():Q", title="기사 수", axis=alt.Axis(tickCount=4)),
            tooltip=[
                alt.Tooltip("expected_net_profit_increase:Q", bin=alt.Bin(maxbins=24),
                            title="순수익 증가 구간", format="$,.0f"),
                alt.Tooltip("count():Q", title="기사 수"),
            ],
        )
    )
    mean_rule = (
        alt.Chart(pd.DataFrame({"v": [mean]}))
        .mark_rule(color=t["ink_2"], strokeWidth=1)
        .encode(x="v:Q")
    )
    return (
        (bars + mean_rule)
        .properties(height=240)
        .configure(**theme.alt_theme(mode))
    )


def count_bars(labels: pd.Series, mode: str, limit: int = 7) -> alt.Chart:
    """라벨별 기사 수 가로 막대. 단일 계열 + 막대 끝 직접 라벨이라 x 축을 뺀다.

    상위 `limit` 개를 넘는 꼬리는 색을 새로 만들지 않고 "기타" 로 접는다.
    """
    t = theme.tokens(mode)
    accent = theme.series(mode)[0]

    counts = (
        labels.value_counts()
        .rename_axis("label")
        .reset_index(name="기사수")
    )
    head, tail = counts.head(limit), counts.iloc[limit:]
    if not tail.empty:
        head = pd.concat(
            [head, pd.DataFrame([{"label": f"기타 {len(tail)}종",
                                  "기사수": int(tail["기사수"].sum())}])],
            ignore_index=True,
        )

    order = head["label"].tolist()
    bars = (
        alt.Chart(head)
        .mark_bar(color=accent, size=BAR_SIZE,
                  cornerRadiusTopRight=CORNER, cornerRadiusBottomRight=CORNER)
        .encode(
            y=alt.Y("label:N", sort=order, title=None,
                    axis=alt.Axis(grid=False, domain=False, labelLimit=170)),
            x=alt.X("기사수:Q", title=None, axis=None,
                    scale=alt.Scale(nice=False, padding=0)),
            tooltip=[alt.Tooltip("label:N", title="차종"),
                     alt.Tooltip("기사수:Q", title="기사 수")],
        )
    )
    labels_layer = bars.mark_text(
        align="left", dx=8, fontSize=11, fontWeight=500,
    ).encode(text=alt.Text("기사수:Q", format=",.0f"), color=alt.value(t["ink_2"]))
    return (
        (bars + labels_layer)
        .properties(height=alt.Step(40))
        .configure(**theme.alt_theme(mode))
    )


def eligibility_gate(
    suggestion_month: pd.DataFrame, threshold: float, mode: str
) -> alt.Chart:
    """선정 게이트 산점도 — 왜 전체 기사 중 일부만 남는지 보여준다.

    강조 패턴: 통과한 기사만 계열색, 나머지는 중립 회색. 색만으로 구분되지 않도록
    범례를 항상 둔다.
    """
    t = theme.tokens(mode)
    accent = theme.series(mode)[0]

    frame = suggestion_month[[
        "driver_id", "manufacturer", "model_name",
        "expected_net_profit_increase", "expected_revenue_increase",
    ]].round({"expected_net_profit_increase": 0, "expected_revenue_increase": 0}).assign(
        구분=lambda d: [
            "선정" if p >= threshold and r > 0 else "제외"
            for p, r in zip(d["expected_net_profit_increase"],
                            d["expected_revenue_increase"])
        ]
    )
    color = alt.Color(
        "구분:N",
        scale=alt.Scale(domain=["선정", "제외"], range=[accent, t["neutral_mark"]]),
        legend=alt.Legend(title=None),
    )
    points = (
        alt.Chart(frame)
        .mark_circle(stroke=t["surface"], strokeWidth=1)
        .encode(
            x=alt.X("expected_net_profit_increase:Q",
                    title="기사 예상 월 순수익 증가 ($)",
                    axis=alt.Axis(format=",.0f", tickCount=8)),
            y=alt.Y("expected_revenue_increase:Q",
                    title="회사 월 렌탈 객단가 증가 ($)",
                    axis=alt.Axis(format=",.0f", tickCount=6)),
            color=color,
            opacity=alt.Opacity(
                "구분:N",
                scale=alt.Scale(domain=["선정", "제외"], range=[0.85, 0.28]),
                legend=None,
            ),
            size=alt.Size(
                "구분:N",
                scale=alt.Scale(domain=["선정", "제외"], range=[60, 34]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("driver_id:N", title="기사"),
                alt.Tooltip("manufacturer:N", title="제조사"),
                alt.Tooltip("model_name:N", title="모델"),
                alt.Tooltip("expected_net_profit_increase:Q",
                            title="기사 순수익 증가", format="$,.0f"),
                alt.Tooltip("expected_revenue_increase:Q",
                            title="회사 객단가 증가", format="$,.0f"),
            ],
        )
    )
    gate_x = (
        alt.Chart(pd.DataFrame({"v": [threshold]}))
        .mark_rule(color=t["axis"], strokeWidth=1).encode(x="v:Q")
    )
    gate_y = (
        alt.Chart(pd.DataFrame({"v": [0]}))
        .mark_rule(color=t["axis"], strokeWidth=1).encode(y="v:Q")
    )
    gate_label = (
        alt.Chart(pd.DataFrame({"v": [threshold], "label": [f"기준 ${threshold:,.0f}"]}))
        .mark_text(align="left", dx=7, dy=-4, baseline="top",
                   color=t["muted"], fontSize=11)
        .encode(x="v:Q", text="label:N")
    )
    return (
        (points + gate_x + gate_y + gate_label)
        .properties(height=320)
        .configure(**theme.alt_theme(mode))
    )


def current_vs_recommended(
    title: str, current: float, recommended: float, mode: str, lower_is_better: bool
) -> alt.Chart:
    """현재 ↔ 추천 2개 막대. 지표마다 단위가 달라 차트를 따로 둔다(작은 배수).

    현재는 중립 회색, 추천만 계열색 — 강조 패턴. 두 막대 모두 y 축에 이름이
    적혀 있어 색이 유일한 식별 수단이 아니고, 값은 막대 끝에 직접 붙는다.
    """
    t = theme.tokens(mode)
    accent = theme.series(mode)[0]
    good, bad = t["good"], t["bad"]

    diff = recommended - current
    improved = (diff < 0) if lower_is_better else (diff > 0)
    sign = "▼" if diff < 0 else "▲"
    frame = pd.DataFrame(
        [
            {"구분": "현재", "값": current, "라벨": f"${current:,.0f}"},
            {"구분": "추천", "값": recommended, "라벨": f"${recommended:,.0f}"},
        ]
    )
    bars = (
        alt.Chart(frame)
        .mark_bar(size=BAR_SIZE,
                  cornerRadiusTopRight=CORNER, cornerRadiusBottomRight=CORNER)
        .encode(
            y=alt.Y("구분:N", sort=["현재", "추천"], title=None,
                    axis=alt.Axis(grid=False, domain=False, labelFontSize=11)),
            x=alt.X("값:Q", title=None, axis=None,
                    scale=alt.Scale(nice=False, padding=0)),
            color=alt.Color(
                "구분:N",
                scale=alt.Scale(domain=["현재", "추천"],
                                range=[t["neutral_mark"], accent]),
                legend=None,
            ),
            tooltip=[alt.Tooltip("구분:N"), alt.Tooltip("값:Q", format="$,.2f")],
        )
    )
    labels = bars.mark_text(
        align="left", dx=8, fontSize=11, fontWeight=500,
    ).encode(text="라벨:N", color=alt.value(t["ink_2"]))
    return (
        (bars + labels)
        .properties(
            height=112,
            title=alt.TitleParams(
                title,
                subtitle=f"{sign} ${abs(diff):,.0f}",
                subtitleColor=good if improved else bad,
                subtitleFontSize=12,
                subtitleFontWeight=600,
                subtitlePadding=4,
                anchor="start",
                fontSize=12,
                fontWeight=500,
                color=t["muted"],
                offset=6,
                font=theme.FONT_STACK,
                subtitleFont=theme.FONT_STACK,
            ),
        )
        .configure(**theme.alt_theme(mode))
    )
