"""대시보드 디자인 토큰과 전역 스타일.

색은 `.streamlit/config.toml` 과 짝을 이룬다 — config 는 Streamlit 위젯을,
여기 토큰은 차트와 커스텀 카드를 칠한다. 값이 어긋나면 둘이 따로 보인다.

차트 계열색(SERIES)은 `dataviz` 검증기를 통과한 조합만 쓴다.
    라이트(표면 #FFFFFF, all-pairs): 최악 CVD ΔE 9.1 / 정상시야 17.5
    다크  (표면 #1B1E29, all-pairs): 최악 CVD ΔE 8.7 / 정상시야 19.9
라이트의 gold·rose 는 표면 대비 3:1 미만이라 항상 직접 라벨이나 표를 함께 둔다.

브랜드 색 #002C5F(0/44/95) 은 `brand` 로만 쓴다. 램프는 같은 색상 255.8° 에서 뽑았다.

`neutral_mark` 는 "현재 vs 추천" 비교에서 기준선 쪽 막대에만 쓰는 중립색이다(강조 패턴).
표면 대비 3:1 을 넘기려면 계열색 쪽으로 밝혀야 해서 다크 모드의 정상시야 ΔE 는 11.2 —
15 하한을 밑돈다. 두 막대 모두 y 축에 "현재"/"추천" 이 적히고 값이 막대 끝에 직접
붙으므로 색이 유일한 식별 수단이 아니다(2차 인코딩).
"""

from __future__ import annotations

import streamlit as st

LIGHT = {
    "plane": "#F1F6FE",
    "surface": "#FFFFFF",
    "surface_soft": "#F7FAFF",
    "ink": "#0F1B2D",
    "ink_2": "#44536B",
    "muted": "#7C8CA3",
    "border": "#E1E9F4",
    "grid": "#E7EDF6",
    "axis": "#CED8E6",
    # 브랜드 앵커. 크롬(히어로·pill·링크)에만 그대로 쓴다 — L=0.300 이라 차트 마크
    # 밝기 밴드(0.43~0.77) 밖이고, 막대로 쓰면 거의 검정으로 읽힌다.
    "brand": "#002C5F",
    # 차트 마크용 — 브랜드와 같은 색상(255.8°)의 밝은 스텝. 밴드 안(L=0.48).
    "accent": "#275DA2",
    "accent_soft": "#E3F0FF",
    "accent_ink": "#002C5F",
    "hero_from": "#0B3C75",
    "hero_to": "#002C5F",
    "neutral_mark": "#8189AB",
    "good": "#006A3D",
    "bad": "#C0342F",
    "shadow": "0 1px 2px rgba(0,44,95,.06), 0 12px 28px -18px rgba(0,44,95,.30)",
    "shadow_hero": "0 1px 2px rgba(0,44,95,.10), 0 18px 40px -24px rgba(0,44,95,.55)",
}

DARK = {
    "plane": "#0A0F15",
    "surface": "#181F28",
    "surface_soft": "#212832",
    "ink": "#EEF3FA",
    "ink_2": "#A3B0C4",
    "muted": "#76839A",
    "border": "#2A323D",
    "grid": "#242B33",
    "axis": "#39424E",
    "brand": "#528BD5",
    "accent": "#528BD5",
    "accent_soft": "#172B45",
    "accent_ink": "#86B4F0",
    "hero_from": "#194C8C",
    "hero_to": "#0B3C75",
    "neutral_mark": "#6B7290",
    "good": "#0CA30C",
    "bad": "#E66767",
    "shadow": "0 1px 2px rgba(0,0,0,.45), 0 12px 28px -18px rgba(0,0,0,.75)",
    "shadow_hero": "0 1px 2px rgba(0,0,0,.45), 0 18px 40px -24px rgba(0,0,0,.9)",
}

# 계열색 슬롯 — 순서가 CVD 안전성 장치다. 임의로 섞거나 5번째를 만들지 않는다.
SERIES = {
    "light": ["#275DA2", "#008A72", "#E0A008", "#EF7FA8"],
    "dark": ["#528BD5", "#0E8A5A", "#C08A00", "#DB6193"],
}

FONT_STACK = (
    'system-ui, -apple-system, "Segoe UI", "Apple SD Gothic Neo", '
    '"Malgun Gothic", sans-serif'
)


def mode() -> str:
    """브라우저 테마. 첫 렌더에서는 미확정일 수 있어 라이트로 떨어진다."""
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except Exception:  # 테스트·스크립트 실행처럼 컨텍스트가 없을 때
        return "light"


def tokens(theme_mode: str | None = None) -> dict[str, str]:
    return DARK if (theme_mode or mode()) == "dark" else LIGHT


def series(theme_mode: str | None = None) -> list[str]:
    return SERIES[theme_mode or mode()]


def inject_css(theme_mode: str | None = None) -> None:
    """전역 스타일. Streamlit 내부 구조에 의존하는 선택자는 여기에만 모아둔다."""
    t = tokens(theme_mode)
    st.markdown(
        f"""
        <style>
        :root {{
            --plane: {t["plane"]};
            --surface: {t["surface"]};
            --surface-soft: {t["surface_soft"]};
            --ink: {t["ink"]};
            --ink-2: {t["ink_2"]};
            --muted: {t["muted"]};
            --border: {t["border"]};
            --brand: {t["brand"]};
            --accent: {t["accent"]};
            --accent-soft: {t["accent_soft"]};
            --accent-ink: {t["accent_ink"]};
            --good: {t["good"]};
            --bad: {t["bad"]};
            --hero-from: {t["hero_from"]};
            --hero-to: {t["hero_to"]};
            --card-shadow: {t["shadow"]};
            --hero-shadow: {t["shadow_hero"]};
            --r-card: 20px;
            --r-hero: 26px;
        }}

        /* ── 판(plane): 카드가 떠 보이도록 본문 배경만 한 단계 낮춘다 ── */
        [data-testid="stAppViewContainer"] {{ background: var(--plane); }}
        [data-testid="stHeader"] {{ background: transparent; }}
        [data-testid="stAppDeployButton"] {{ display: none; }}
        [data-testid="stMainBlockContainer"] {{
            padding: 2.2rem 2.6rem 5rem;
            max-width: 1360px;
        }}
        html, body, [class*="st-"] {{ font-family: {FONT_STACK}; }}
        /* 위 규칙이 너무 광범위해 Streamlit 아이콘(Material Symbols 리거처)까지
           우리 폰트로 덮어써서 화살표 등이 글리프 대신 원본 텍스트로 보였다. */
        [data-testid="stIconMaterial"] {{ font-family: "Material Symbols Rounded" !important; }}

        /* ── 지표 타일 ── */
        [data-testid="stMetric"] {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: var(--r-card);
            padding: 1.05rem 1.25rem 1.15rem;
            box-shadow: var(--card-shadow);
            height: 100%;
        }}
        [data-testid="stMetricLabel"] p {{
            color: var(--muted);
            font-size: .8rem;
            font-weight: 500;
            letter-spacing: .01em;
        }}
        [data-testid="stMetricValue"] {{
            color: var(--ink);
            letter-spacing: -.02em;
            font-variant-numeric: proportional-nums;
        }}
        [data-testid="stMetricDelta"] {{ font-size: .82rem; font-weight: 500; }}

        /* 나란히 둔 카드의 높이를 맞춘다 — 열 안에서 카드가 늘어나도록. */
        [data-testid="stHorizontalBlock"] {{ align-items: stretch; }}
        [data-testid="stColumn"] > div {{ height: 100%; }}

        /* ── 카드(st.container(key=...) → .st-key-<key>) ── */
        [class*="st-key-card-"] {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: var(--r-card);
            padding: 1.35rem 1.5rem 1.45rem;
            box-shadow: var(--card-shadow);
            height: 100%;
        }}

        /* ── 헤더 / 히어로 ── */
        .dash-head {{
            display: flex; align-items: center; justify-content: space-between;
            gap: 1rem; flex-wrap: wrap; margin: 0 0 1.5rem;
        }}
        div.dash-head h1 {{
            margin: 0; font-size: 1.7rem; font-weight: 650;
            letter-spacing: -.025em; color: var(--ink);
        }}
        div.dash-head p {{ margin: .3rem 0 0; color: var(--muted); font-size: .9rem; }}
        span.pill {{
            display: inline-flex; align-items: center; gap: .45rem;
            background: var(--accent-soft); color: var(--accent-ink);
            border-radius: 999px; padding: .42rem .95rem;
            font-size: .82rem; font-weight: 600; letter-spacing: .01em;
        }}
        span.pill--ghost {{
            background: var(--surface); color: var(--ink-2);
            border: 1px solid var(--border);
        }}

        div.hero {{
            display: flex; align-items: flex-end; justify-content: space-between;
            gap: 1.5rem; flex-wrap: wrap;
            background: linear-gradient(135deg, var(--hero-from) 0%, var(--hero-to) 100%);
            border-radius: var(--r-hero);
            padding: 1.7rem 1.9rem;
            box-shadow: var(--hero-shadow);
            color: #fff;
        }}
        div.hero .hero__side {{ text-align: right; }}
        div.hero p.hero__side-value {{
            margin: .3rem 0 0; font-size: 1.6rem; font-weight: 620;
            letter-spacing: -.02em; color: #fff;
        }}
        div.hero p.hero__label {{
            margin: 0; font-size: .84rem; font-weight: 500;
            color: rgba(255,255,255,.78); letter-spacing: .01em;
        }}
        div.hero p.hero__value {{
            margin: .35rem 0 0; font-size: 3rem; font-weight: 650; line-height: 1.05;
            letter-spacing: -.035em; font-variant-numeric: proportional-nums;
        }}
        div.hero p.hero__note {{ margin: .6rem 0 0; font-size: .86rem; color: rgba(255,255,255,.8); }}
        div.hero p.hero__note b {{ color: #fff; font-weight: 600; }}

        /* ── 섹션 제목 ── */
        div.sect {{ margin: 2.3rem 0 .95rem; }}
        div.sect h2 {{
            margin: 0; font-size: 1.08rem; font-weight: 620;
            letter-spacing: -.015em; color: var(--ink);
        }}
        div.sect p {{ margin: .28rem 0 0; color: var(--muted); font-size: .85rem; }}

        [class*="st-key-card-"] span.sub {{
            color: var(--muted); font-size: .82rem; font-weight: 400;
        }}
        [class*="st-key-card-"] div.sub-row {{ margin-top: .5rem; }}

        div.chips {{ display: flex; flex-wrap: wrap; gap: .45rem; margin: .2rem 0 0; }}
        div.chips span.chip {{
            background: var(--surface-soft); border: 1px solid var(--border);
            border-radius: 999px; padding: .35rem .8rem;
            font-size: .8rem; color: var(--ink-2);
        }}
        div.chips span.chip b {{ color: var(--ink); font-weight: 600; }}

        div.swap {{
            display: flex; align-items: center; gap: .7rem; flex-wrap: wrap;
            font-size: .95rem; color: var(--ink-2);
        }}
        div.swap b {{ color: var(--ink); font-weight: 600; }}
        div.swap span.arrow {{ color: var(--brand); font-weight: 700; }}

        /* ── 위젯·표 ── */
        [data-testid="stDataFrame"] {{ border-radius: 16px; overflow: hidden; }}
        [data-testid="stExpander"] details {{
            border-radius: var(--r-card);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--card-shadow);
        }}
        [data-testid="stExpander"] summary {{ border-radius: var(--r-card); }}
        [data-baseweb="select"] > div {{ border-radius: 14px; }}
        [data-baseweb="tag"] {{ border-radius: 999px !important; }}
        [data-testid="stAlert"] {{ border-radius: var(--r-card); }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def alt_theme(theme_mode: str | None = None) -> dict:
    """Altair configure_* 에 그대로 펼쳐 넣는 차트 크롬. 격자·축은 실선 헤어라인."""
    t = tokens(theme_mode)
    return {
        "view": {"stroke": None, "continuousWidth": 300},
        "background": "transparent",
        "font": FONT_STACK,
        "axis": {
            "labelColor": t["muted"],
            "titleColor": t["ink_2"],
            "labelFont": FONT_STACK,
            "titleFont": FONT_STACK,
            "labelFontSize": 11,
            "titleFontSize": 11,
            "titleFontWeight": 500,
            "titlePadding": 10,
            "labelPadding": 6,
            "gridColor": t["grid"],
            "gridWidth": 1,
            "domainColor": t["axis"],
            "domainWidth": 1,
            "tickColor": t["axis"],
            "tickSize": 0,
        },
        "legend": {
            "labelColor": t["ink_2"],
            "titleColor": t["muted"],
            "labelFont": FONT_STACK,
            "titleFont": FONT_STACK,
            "labelFontSize": 11,
            "titleFontSize": 11,
            "symbolType": "circle",
            "symbolSize": 90,
            "orient": "top",
            "direction": "horizontal",
            "offset": 4,
            "padding": 0,
            "titlePadding": 8,
        },
        "text": {"font": FONT_STACK, "color": t["ink_2"]},
        "rule": {"color": t["axis"]},
    }
