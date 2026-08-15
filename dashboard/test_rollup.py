"""월 롤업 자체 점검. `uv run python test_rollup.py`

1. 주차 → 월 매핑이 ISO 기준(그 주의 목요일)인가
2. 월간 1위 차량이 주간 1위와 달라질 수 있는가 (합산 후 재선정)
3. 자격 3조건 — 월 이득 600 미만 / 회사 객단가 0 이하는 빠지는가
"""

import pandas as pd

from app import MIN_MONTHLY_GAIN_USD, SUM_COLS, month_key, rollup, verify_against_mart

WEEKS = ["2025-12-29", "2026-01-05"]


def swap_row(driver, model, week, driver_gain, arpu_gain=7.0, tier=0.0, fuel=0.0):
    return {
        "driver_id": driver, "make_key": "KIA", "model_key": model,
        "current_make_key": "KIA", "current_model_key": "FORTE",
        "driver_net_gain_usd": driver_gain, "company_arpu_gain_usd": arpu_gain,
        "gain_from_fuel_usd": fuel, "gain_from_tier_usd": tier, "cost_from_lease_usd": 0.0,
        "tier_upgraded": tier > 0, "is_feasible": True,
        "week_start": week, "month": month_key(pd.Series([week])).iat[0],
    }


def fleet_frame(drivers):
    rows = [
        {"driver_id": d, "week_start": w, "month": month_key(pd.Series([w])).iat[0],
         "total_miles": 100.0, "tenure_days": 300, "net_earnings_usd": -50.0,
         "top_pickup_borough": "Queens"}
        for d in drivers for w in WEEKS
    ]
    return pd.DataFrame(rows)


def test_주차는_목요일이_속한_달로_묶인다():
    assert month_key(pd.Series(["2025-12-29"])).iat[0] == "2026-01"
    assert month_key(pd.Series(["2025-12-22"])).iat[0] == "2025-12"


def test_월간_1위는_주간_1위가_아니라_합산_이득으로_다시_뽑힌다():
    """SOUL 은 1주차에 크게 앞서지만, 2주 합산은 SPORTAGE 가 더 크다."""
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", WEEKS[0], 1000.0),
        swap_row("D1", "SPORTAGE", WEEKS[0], 400.0),
        swap_row("D1", "SOUL", WEEKS[1], 50.0),
        swap_row("D1", "SPORTAGE", WEEKS[1], 900.0),
    ])
    kpi, call_list, best = rollup(swap, fleet_frame(["D1"]), MIN_MONTHLY_GAIN_USD)
    assert list(best["model_key"]) == ["SPORTAGE"]
    assert call_list["driver_net_gain_usd"].iat[0] == 1300.0
    assert call_list["miles"].iat[0] == 200.0, "주행거리는 두 주가 합산돼야 한다"
    assert kpi["active_driver_count"] == 1


def test_월_이득이_기준_미만이면_자격자에서_빠진다():
    """주간으로는 매주 이득이지만 월 합계가 기준에 못 미치는 기사."""
    below = MIN_MONTHLY_GAIN_USD / 2 - 1
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", WEEKS[0], below),
        swap_row("D1", "SOUL", WEEKS[1], below),
        swap_row("D2", "SOUL", WEEKS[0], MIN_MONTHLY_GAIN_USD),
    ])
    kpi, call_list, _ = rollup(swap, fleet_frame(["D1", "D2"]), MIN_MONTHLY_GAIN_USD)
    assert list(call_list["driver_id"]) == ["D2"], "경계값은 포함(>=)"
    assert kpi["active_driver_count"] == 2, "자격에서 빠져도 운행 기사 수에는 남는다"


def test_주간은_같은_함수를_기준_0으로_타고_자격자를_전원_싣는다():
    """주간 스코프는 기간이 1주라 합산이 항등 — 이득이 $1 인 기사도 리스트에 남는다."""
    week = WEEKS[0]
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", week, 1.0),
        swap_row("D2", "SOUL", week, 900.0),
    ])
    fleet = fleet_frame(["D1", "D2"])
    kpi, call_list, _ = rollup(swap, fleet[fleet["week_start"] == week], 0.0)
    assert list(call_list["driver_id"]) == ["D2", "D1"], "이득 내림차순 전원"
    assert kpi["total_arpu_gain_usd"] == 14.0, "객단가도 전원 합계"
    assert call_list["miles"].iat[0] == 100.0, "주간은 그 주 주행거리만"


def test_회사_객단가가_늘지_않는_추천은_콜_리스트에서_빠진다():
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", WEEKS[0], 1000.0, arpu_gain=0.0),
        swap_row("D2", "SOUL", WEEKS[0], 700.0, arpu_gain=7.0),
    ])
    kpi, call_list, _ = rollup(swap, fleet_frame(["D1", "D2"]), MIN_MONTHLY_GAIN_USD)
    assert list(call_list["driver_id"]) == ["D2"]
    assert kpi["total_arpu_gain_usd"] == 7.0
    assert kpi["target_customer_count"] == 1


def test_기여도가_양수인_항목만_큰_순으로_사유에_붙는다():
    swap = pd.DataFrame([swap_row("D1", "SOUL", WEEKS[0], 1000.0, tier=900.0, fuel=100.0)])
    _, call_list, _ = rollup(swap, fleet_frame(["D1"]), MIN_MONTHLY_GAIN_USD)
    assert call_list["reason_text"].iat[0] == "등급 상승 +$900.0, 연료비 절감 +$100.0"


def mart_frame(rows):
    """(rank, driver_id, company_arpu_gain_usd) 목록 → gold_mart_top_customers 모양."""
    return pd.DataFrame([
        {"week_start": WEEKS[0], "rank": rank, "driver_id": driver, "company_arpu_gain_usd": arpu}
        for rank, driver, arpu in rows
    ])


def test_마트와_같은_답을_내면_대조가_조용하다():
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", WEEKS[0], 900.0),
        swap_row("D2", "SOUL", WEEKS[0], 100.0),
    ])
    fleet = fleet_frame(["D1", "D2"])
    fleet = fleet[fleet["week_start"] == WEEKS[0]]
    mart = mart_frame([(1, "D1", 7.0), (2, "D2", 7.0)])
    assert verify_against_mart(swap, fleet, mart) == []


def test_선정이_마트와_어긋나면_대조가_잡아낸다():
    """마트는 D2 를 1위로 담았는데 rollup 은 D1 을 1위로 뽑는 상황."""
    swap = pd.DataFrame([
        swap_row("D1", "SOUL", WEEKS[0], 900.0),
        swap_row("D2", "SOUL", WEEKS[0], 100.0),
    ])
    fleet = fleet_frame(["D1", "D2"])
    fleet = fleet[fleet["week_start"] == WEEKS[0]]
    mart = mart_frame([(1, "D2", 7.0), (2, "D1", 7.0)])
    assert verify_against_mart(swap, fleet, mart) == [f"{WEEKS[0]} 기사 선정"]


if __name__ == "__main__":
    assert set(SUM_COLS) <= set(swap_row("D", "M", WEEKS[0], 1.0)), "합산 컬럼이 픽스처에 없다"
    for name, case in sorted(globals().items()):
        if name.startswith("test_"):
            case()
            print("ok", name)
