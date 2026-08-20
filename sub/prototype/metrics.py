"""매칭 품질 측정.

이 프로토타입의 존재 이유입니다. "돌아간다"만으로는 설계의 성립 여부를 알 수
없습니다. 아래 세 숫자가 서로 다른 질문에 답합니다.

  coverage    제공된 트립 중 몇 %에 신원을 붙였는가
  ceiling     기사 정원으로 물리적으로 가능한 최대치는 몇 %인가
  saturation  그 최대치의 몇 %를 실제로 채웠는가  ← 배정 알고리즘의 품질

coverage 가 낮아도 ceiling 이 낮으면 그건 기사 수 문제이고 알고리즘 문제가
아닙니다. 두 개를 섞어 보면 잘못된 곳을 고치게 됩니다.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


def capacity_ceiling(profiles: pd.DataFrame, service_dates: list) -> dict:
    """기사 정원의 하루 운행시간 예산 합계.

    기사마다 `target_drive_minutes` × (그 달의 활동 요일 수) 입니다. 요일 선호가
    있어서 달의 요일 분포에 따라 값이 달라집니다.

    예전에는 목표 **트립 수**로 계산했고, 그 값이 공차·유휴를 무시한 과대추정이라
    "절대값으로 읽지 마세요"라는 경고를 달고 있었습니다. 운행분은 배정 결과에서
    그대로 측정되는 물리량이라 그 경고가 필요 없습니다 — `saturation` 을 절대값으로
    읽어도 됩니다.
    """
    service_dates = pd.to_datetime(pd.Series(list(service_dates)))
    weekday_days = service_dates.dt.dayofweek.value_counts().to_dict()
    per_driver = [
        int(target) * sum(weekday_days.get(int(w), 0) for w in weekdays)
        for target, weekdays in zip(
            profiles["target_drive_minutes"], profiles["active_weekdays"]
        )
    ]
    return {
        "driver_count": int(len(profiles)),
        "service_days": int(len(service_dates)),
        "capacity_drive_minutes": int(sum(per_driver)),
        "capacity_drive_hours": round(sum(per_driver) / 60.0, 1),
        "drive_hours_per_driver_mean": round(float(np.mean(per_driver)) / 60.0, 2) if per_driver else 0.0,
    }


def matching_report(
    *,
    target_month: str,
    run_id: str,
    trip_count: int,
    service_dates: list,
    tier_counts: dict,
    profiles: pd.DataFrame,
    attribution,
    clip_rate: float,
    bucket_size: int,
) -> dict:
    """매칭 품질 한 장.

    `trips` 프레임을 받지 않고 집계값만 받습니다. 전체 달(2,090만 행) 경로에서는
    트립이 서비스일 파일로 흩어져 있어 한 프레임으로 존재하지 않습니다.
    """
    total = int(trip_count)
    attributed = attribution.attributed
    matched = int(len(attributed))
    ceiling = capacity_ceiling(profiles, service_dates)
    budget_minutes = max(1, ceiling["capacity_drive_minutes"])
    # 실현 운행분 = 승객 태운 시간 + 공차. 예산과 같은 단위라 바로 나눕니다.
    drive_minutes = (
        float((attributed["trip_time"] / 60.0 + attributed["deadhead_minutes"]).sum())
        if matched
        else 0.0
    )
    # 트립 하나가 실제로 먹은 운행분. `ceiling_pct` 환산에만 씁니다 — 제공 트립의
    # 소요 시간은 전체 달 경로에서 집계로 들고 오지 않아 관측값으로 환산합니다.
    minutes_per_trip = drive_minutes / matched if matched else 0.0

    report: dict = {
        "target_month": target_month,
        "run_id": run_id,
        "bucket_size": bucket_size,
        "trips_offered": total,
        "trips_attributed": matched,
        "coverage_pct": round(100.0 * matched / max(1, total), 2),
        "capacity": ceiling,
        "ceiling_pct": (
            round(100.0 * (budget_minutes / minutes_per_trip) / max(1, total), 2)
            if minutes_per_trip
            else 0.0
        ),
        "saturation_pct": round(100.0 * drive_minutes / budget_minutes, 2),
        "drive_hours_total": round(drive_minutes / 60.0, 1),
        "candidate_rows": attribution.candidate_rows,
        "candidate_rows_surviving_vector_filters": attribution.survivor_rows,
        "rejection_counts": attribution.reason_counts,
        "realization_clip_rate": round(clip_rate, 4),
    }
    if matched == 0:
        report["note"] = "배정 0건 — 제약이 전부 떨어냈습니다. rejection_counts 를 보세요."
        return report

    # --- 등급·플랫폼별 매칭률: 제약 2 가 어디서 무는지 -----------------------
    got = attributed.groupby(["platform_name", "estimated_service_tier"]).size().to_dict()
    report["coverage_by_tier_pct"] = {
        f"{platform}/{tier}": round(100.0 * got.get((platform, tier), 0) / offered, 2)
        for (platform, tier), offered in sorted(tier_counts.items())
        if offered
    }

    # --- 기사별 활용도: 프로필이 장식이 아닌지 -----------------------------
    per_driver = attributed.groupby("driver_id").size()
    idle = int(len(profiles) - per_driver.count())
    report["driver_utilization"] = {
        "drivers_with_zero_trips": idle,
        "idle_driver_pct": round(100.0 * idle / max(1, len(profiles)), 2),
        "trips_per_driver_mean": round(float(per_driver.mean()), 2),
        "trips_per_driver_p50": int(per_driver.median()),
        "trips_per_driver_max": int(per_driver.max()),
    }

    # --- 하루 운행시간: 목표가 트립 수가 아니라 이 값입니다 -----------------
    # 하한(기사별 4~8h)에 닿았는가와 예산(8~12h)을 얼마나 소모했는가를 같이 봅니다.
    # 하한은 제약이 아니라 목표입니다 — 트립을 만들어내지 않으므로 후보가 없으면
    # 채울 방법이 없고, 미달률이 곧 배정이 못 메운 몫입니다.
    daily_drive = (
        attributed.assign(
            _drive=attributed["trip_time"] / 60.0 + attributed["deadhead_minutes"]
        )
        .groupby(["driver_id", "service_date"])["_drive"]
        .sum()
    )
    floor = dict(zip(profiles["driver_id"], profiles["min_drive_minutes"]))
    budget = dict(zip(profiles["driver_id"], profiles["target_drive_minutes"]))
    reached = [
        minutes >= floor.get(driver_id, 0) for (driver_id, _), minutes in daily_drive.items()
    ]
    exhausted = [
        minutes >= 0.95 * budget.get(driver_id, 1)
        for (driver_id, _), minutes in daily_drive.items()
    ]
    # 운행시간을 두 조각으로 분해합니다. 어느 쪽이 천장인지 이 두 값이 말해 줍니다.
    #
    #   운행시간 = 픽업 창(첫~막 픽업) × 가동률
    #
    # 창이 천장이면 `PREFERRED_BLOCK_RUN`(선호 시간대 폭)을 봐야 하고, 가동률이
    # 낮으면 창 안에 빈틈이 있는 것이라 공차 한도·후보 밀도를 봐야 합니다. 실제로
    # 3블록일 때 창의 최대가 정확히 9.00h 였고, 후보를 3.2배 줘도 운행시간이 8%만
    # 올랐습니다 — 그 진단이 이 두 줄에서 나왔습니다.
    daily = attributed.groupby(["driver_id", "service_date"])
    window = (daily["pickup_datetime"].max() - daily["pickup_datetime"].min()).dt.total_seconds() / 60.0
    span = (daily["dropoff_datetime"].max() - daily["pickup_datetime"].min()).dt.total_seconds() / 60.0
    utilization = daily_drive / span.clip(lower=1)

    hours = daily_drive / 60.0
    report["drive_hours_per_day"] = {
        "driver_days": int(len(hours)),
        "mean": round(float(hours.mean()), 2),
        "p05": round(float(hours.quantile(0.05)), 2),
        "p50": round(float(hours.median()), 2),
        "p95": round(float(hours.quantile(0.95)), 2),
        "max": round(float(hours.max()), 2),
        "reaching_floor_pct": round(100.0 * float(np.mean(reached)), 2) if reached else 0.0,
        "budget_exhausted_pct": round(100.0 * float(np.mean(exhausted)), 2) if exhausted else 0.0,
        # 픽업 창(첫~막 픽업). 선호 시간대 폭이 천장이면 여기가 그 값에 붙습니다.
        "pickup_window_h_mean": round(float(window.mean()) / 60.0, 2),
        "pickup_window_h_p95": round(float(window.quantile(0.95)) / 60.0, 2),
        "pickup_window_h_max": round(float(window.max()) / 60.0, 2),
        # 하루 길이(첫 픽업~막 하차) 중 실제로 운행한 비율. 나머지는 대기입니다.
        "span_h_mean": round(float(span.mean()) / 60.0, 2),
        "utilization_pct": round(100.0 * float(utilization.mean()), 2),
    }

    # --- 제약 6 이 실제로 작동했는지: 프로필 준수율 ------------------------
    weekday_hit = [
        row_weekday in weekdays
        for row_weekday, weekdays in zip(
            attributed["pickup_datetime"].dt.dayofweek,
            attributed["driver_id"].map(dict(zip(profiles["driver_id"], profiles["active_weekdays"]))),
        )
    ]
    block_hit = [
        (row_block in blocks)
        for row_block, blocks in zip(
            attributed["pickup_datetime"].dt.hour // 3,
            attributed["driver_id"].map(dict(zip(profiles["driver_id"], profiles["preferred_time_blocks"]))),
        )
    ]
    report["profile_adherence_pct"] = {
        "active_weekday": round(100.0 * float(np.mean(weekday_hit)), 2),
        "preferred_time_block": round(100.0 * float(np.mean(block_hit)), 2),
    }

    # --- 물리 정합: 공차와 선호 점수 --------------------------------------
    deadhead = attributed["deadhead_minutes"]
    report["deadhead_minutes"] = {
        "zero_pct": round(100.0 * float((deadhead == 0).mean()), 2),
        "mean": round(float(deadhead.mean()), 2),
        "p95": round(float(deadhead.quantile(0.95)), 2),
        "max": round(float(deadhead.max()), 2),
    }
    report["preference_score"] = {
        "mean": round(float(attributed["preference_score"].mean()), 4),
        "p05": round(float(attributed["preference_score"].quantile(0.05)), 4),
        "p95": round(float(attributed["preference_score"].quantile(0.95)), 4),
    }

    # --- 경제 지표: 메인 프로덕트가 추천할 여지가 남아 있는지 (D6) ----------
    pay = attributed.groupby("driver_id")["driver_pay"].sum()
    miles = attributed.groupby("driver_id")["trip_miles"].sum()
    report["economics"] = {
        "driver_pay_total_usd": round(float(attributed["driver_pay"].sum()), 2),
        "driver_pay_per_driver_mean_usd": round(float(pay.mean()), 2),
        "miles_per_driver_mean": round(float(miles.mean()), 2),
    }
    return report


def write_report(report: dict, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def format_report(report: dict) -> str:
    """터미널에서 바로 읽히는 요약."""
    lines = [
        f"[{report['target_month']}] run_id={report['run_id']}",
        f"  트립 제공        {report['trips_offered']:>10,}",
        f"  트립 귀속        {report['trips_attributed']:>10,}  (coverage {report['coverage_pct']}%)",
        f"  운행 예산(h)     {report['capacity']['capacity_drive_hours']:>10,.0f}  (ceiling {report['ceiling_pct']}%)",
        f"  예산 소진율      {report['saturation_pct']:>10}%   <- 배정 알고리즘 품질",
        f"  후보 행          {report['candidate_rows']:>10,} -> 벡터 제약 통과 {report['candidate_rows_surviving_vector_filters']:,}",
        "  탈락 사유:",
    ]
    for reason, count in sorted(report["rejection_counts"].items(), key=lambda kv: -kv[1]):
        lines.append(f"    {reason:<28} {count:>12,}")
    if "driver_utilization" in report:
        u = report["driver_utilization"]
        lines += [
            f"  유휴 기사        {u['drivers_with_zero_trips']:>10,}  ({u['idle_driver_pct']}%)",
            f"  기사당 트립      평균 {u['trips_per_driver_mean']}, 중앙 {u['trips_per_driver_p50']}, 최대 {u['trips_per_driver_max']}",
        ]
        h = report["drive_hours_per_day"]
        lines += [
            f"  하루 운행(h)     평균 {h['mean']}, 중앙 {h['p50']}, p05 {h['p05']}, p95 {h['p95']}, 최대 {h['max']}",
            f"  하한 도달        {h['reaching_floor_pct']}% (기사-일), 예산 소진 {h['budget_exhausted_pct']}%",
            f"  픽업 창(h)       평균 {h['pickup_window_h_mean']}, p95 {h['pickup_window_h_p95']}, 최대 {h['pickup_window_h_max']}",
            f"  가동률           {h['utilization_pct']}% (하루 길이 평균 {h['span_h_mean']}h 중 운행)",
        ]
        a = report["profile_adherence_pct"]
        lines.append(f"  프로필 준수      요일 {a['active_weekday']}%, 시간대 {a['preferred_time_block']}%")
        d = report["deadhead_minutes"]
        lines.append(f"  공차(분)         0분 {d['zero_pct']}%, 평균 {d['mean']}, p95 {d['p95']}")
        lines.append(f"  등급별 coverage  {report['coverage_by_tier_pct']}")
    lines.append(f"  실현값 클리핑    {report['realization_clip_rate'] * 100:.2f}%")
    return "\n".join(lines)
