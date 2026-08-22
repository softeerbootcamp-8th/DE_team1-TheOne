"""Silver → Gold DAG의 2개 실행 Asset 계약과 산출물 검증 시나리오.

1. Gold 스케줄은 API 3종 완료 READY와 Fuel Silver의 OR Asset
2. API Silver 3종은 개별 Asset을 발행하지 않고 Fuel만 검증 후 발행
3. Asset 으로 실행된 Gold 는 해당 파티션 키를 대상 월로 사용
4. 대상 연월은 기준일 이하의 최신 HVFHV 파티션이며 수동 파라미터가 우선
5. Asset 실행은 같은 월 Silver가 덜 준비되면 skip, 수동 실행은 실패
6. 같은 월 Silver 4종이 모두 있어야 입력 경로 확정
7. Gold 검증 성공 태스크에만 Slack 완료 알림 연결
8. Gold 3종이 비었거나 필수 컬럼이 없거나 다른 연월이면 실패
9. API Silver는 최신 collected_at 파일만 선택
10. Asset skip 시 Slack skip 알림을 직접 호출
11. SLA 기준일 초과 시 Slack staleness 경고, 기준 이내면 조용히 통과
12. SLA 기준일은 Param이 우선, 없으면 Variable, 그마저 없으면 기본값 31
"""

import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable

from main.airflow.common import assets
from main.airflow.scripts.monthly_taxi_trip_silver_to_gold import tasks as dag_module
from shared.airflow.common.slack_failure_callback import slack_success_callback


GOLD_DAG = importlib.import_module(
    "dags.monthly_taxi_trip_silver_to_gold_dag"
).monthly_taxi_trip_silver_to_gold_dag


def _params(root: Path, **overrides) -> dict:
    params = {
        "monthly_taxi_trip_path": str(root / "monthly_taxi_trip"),
        "driver_vehicle_monthly_snapshot_path": str(root / "driver_vehicle_monthly_snapshot"),
        "lease_vehicle_inventory_path": str(root / "lease_vehicle_inventory"),
        "fuel_price_path": str(root / "gas_ev_price"),
        "year": None,
        "month": None,
    }
    params.update(overrides)
    return params


def _logical_date(year: int, month: int) -> datetime:
    return datetime(year, month, 13, tzinfo=timezone.utc)


def _write_inputs(root: Path, year_month: str) -> None:
    monthly_taxi_trip = root / "monthly_taxi_trip" / f"year_month={year_month}"
    monthly_taxi_trip.mkdir(parents=True)
    (monthly_taxi_trip / "part-00000.parquet").touch()

    files = {
        "driver_vehicle_monthly_snapshot": "driver_vehicle_monthly_snapshot.parquet",
        "lease_vehicle_inventory": "lease_vehicle_inventory.parquet",
        "gas_ev_price": "gas_ev_price.parquet",
    }
    for dataset, file_name in files.items():
        partition = root / dataset / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / file_name).touch()


def _write_version(root: Path, dataset: str, year_month: str, file_name: str) -> Path:
    path = root / dataset / f"year_month={year_month}" / file_name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def test_Gold_DAG은_API3종완료_READY와_Fuel중_어느_Asset이든_실행된다():
    timetable = GOLD_DAG.timetable

    assert isinstance(timetable, PartitionedAssetTimetable)
    assert type(timetable.asset_condition).__name__ == "SerializedAssetAny"
    assert {item.name for item in timetable.asset_condition.objects} == {
        assets.API_SILVER_REFRESH_READY.name,
        assets.FUEL_PRICE_SILVER.name,
    }
    assert isinstance(timetable.default_partition_mapper, IdentityMapper)
    assert timetable.default_partition_mapper.to_downstream("2026-05") == "2026-05"


@pytest.mark.parametrize(
    ("module_name", "dag_name"),
    [
        (
            "dags.monthly_taxi_trip_raw_to_silver_dag",
            "monthly_taxi_trip_dag",
        ),
        (
            "dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag",
            "driver_vehicle_monthly_snapshot_raw_to_silver_dag",
        ),
        (
            "dags.lease_vehicle_inventory_raw_to_silver_dag",
            "lease_vehicle_inventory_raw_to_silver_dag",
        ),
    ],
)
def test_API_Silver_3종은_개별_Asset을_발행하지_않는다(module_name, dag_name):
    upstream = getattr(importlib.import_module(module_name), dag_name)

    assert not upstream.get_task("validate_silver").outlets


def test_Fuel_Silver는_검증_태스크에서_Asset을_발행한다():
    upstream = importlib.import_module(
        "dags.eia_fuel_price_silver_dag"
    ).eia_fuel_price_silver_dag

    assert [outlet.name for outlet in upstream.get_task("validate_silver").outlets] == [
        assets.FUEL_PRICE_SILVER.name
    ]
    assert not upstream.get_task("combine_silver").outlets


def test_검증된_월이_Asset_파티션키로_기록된다():
    class Recorder:
        def __init__(self):
            self.keys = set()

        def add_partitions(self, keys):
            self.keys.add(keys)

    recorder = Recorder()
    events = {assets.FUEL_PRICE_SILVER: recorder}

    assets.publish_month_partition(events, assets.FUEL_PRICE_SILVER, "2026-05")

    assert recorder.keys == {"2026-05"}


def test_Gold_대상월은_Asset_파티션키를_그대로_사용한다(tmp_path):
    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 8),
        _params(tmp_path),
        str(tmp_path / "monthly_taxi_trip"),
        partition_key="2026-05",
    )

    assert resolved == "2026-05"


def test_대상연월은_기준일_이하_최신_HVFHV_파티션이다(tmp_path):
    for year_month in ("2026-03", "2026-05", "2026-09"):
        partition = tmp_path / "monthly_taxi_trip" / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / "part-00000.parquet").touch()

    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6), _params(tmp_path), str(tmp_path / "monthly_taxi_trip")
    )

    assert resolved == "2026-05"


def test_year_month_파라미터가_파티션보다_우선한다(tmp_path):
    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6),
        _params(tmp_path, year="2025", month="7"),
        str(tmp_path / "monthly_taxi_trip"),
    )

    assert resolved == "2025-07"


def test_Silver_4종이_있으면_같은_월_경로를_확정한다(tmp_path):
    _write_inputs(tmp_path, "2026-05")

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path))

    assert resolved["year"] == "2026" and resolved["month"] == "5"
    assert resolved["monthly_taxi_trip_path"].endswith(
        "monthly_taxi_trip/year_month=2026-05/part-*.parquet"
    )
    assert resolved["driver_vehicle_monthly_snapshot_path"].endswith(
        "year_month=2026-05/driver_vehicle_monthly_snapshot.parquet"
    )
    assert resolved["lease_vehicle_inventory_path"].endswith(
        "year_month=2026-05/lease_vehicle_inventory.parquet"
    )
    assert resolved["fuel_price_path"].endswith(
        "year_month=2026-05/gas_ev_price.parquet"
    )


def test_API_Silver는_가장최신_collected_at_파일만_선택한다(tmp_path):
    _write_inputs(tmp_path, "2026-05")
    older = "20260820T123456123456Z.parquet"
    latest = "20260821T123456123456Z.parquet"
    for dataset in (
        "monthly_taxi_trip",
        "driver_vehicle_monthly_snapshot",
        "lease_vehicle_inventory",
    ):
        _write_version(tmp_path, dataset, "2026-05", older)
        _write_version(tmp_path, dataset, "2026-05", latest)

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path))

    assert Path(resolved["monthly_taxi_trip_path"]).name == latest
    assert Path(resolved["driver_vehicle_monthly_snapshot_path"]).name == latest
    assert Path(resolved["lease_vehicle_inventory_path"]).name == latest


def test_Silver_입력이_빠지면_상류_DAG를_알려준다(tmp_path):
    _write_inputs(tmp_path, "2026-05")
    (tmp_path / "gas_ev_price/year_month=2026-05/gas_ev_price.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="eia_fuel_price_silver_pipeline"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path))


def test_Asset실행은_같은월_Silver입력이_덜준비되면_skip한다(tmp_path):
    dag_run = type("DagRun", (), {"partition_key": "2026-05"})()

    with pytest.raises(AirflowSkipException, match="Silver 4종 준비 대기"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path),
            logical_date=_logical_date(2026, 5),
            dag_run=dag_run,
        )


def test_skip시_Slack_skip_알림을_직접_호출한다(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        dag_module, "slack_skip_alert_callback", lambda context: calls.append(context)
    )
    dag_run = type("DagRun", (), {"partition_key": "2026-05"})()

    with pytest.raises(AirflowSkipException):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path),
            logical_date=_logical_date(2026, 5),
            dag_run=dag_run,
        )

    assert len(calls) == 1
    assert isinstance(calls[0]["exception"], FileNotFoundError)


def test_정상실행에서는_Slack_skip_알림을_호출하지_않는다(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        dag_module, "slack_skip_alert_callback", lambda context: calls.append(context)
    )
    _write_inputs(tmp_path, "2026-05")

    dag_module.validate_inputs_task.function(
        params=_params(tmp_path),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "2026-05"})(),
    )

    assert calls == []


def test_수동실행은_Silver입력이_빠지면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="월별 택시 운행 기록 Silver"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path, year="2026", month="5"),
            logical_date=_logical_date(2026, 5),
            dag_run=type("DagRun", (), {"partition_key": None})(),
        )


def test_Gold검증_성공태스크만_Slack완료알림을_보낸다():
    validate_gold = GOLD_DAG.get_task("validate_gold")

    assert slack_success_callback in validate_gold.on_success_callback
    for task_id in ("validate_inputs", "build_gold"):
        assert slack_success_callback not in (
            GOLD_DAG.get_task(task_id).on_success_callback or []
        )


def _write_gold(root: Path, year_month: str) -> None:
    frames = {
        "driver_aggregation": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "monthly_net_profit": 100.0, "monthly_lease_fee": 400.0}]
        ),
        "driver_car_suggestion": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "vehicle_model_id": "MODEL1", "manufacturer": "KIA", "model_name": "FORTE", "expected_net_profit_increase": 120.0, "recommendation_reason": "연료비 절감"}]
        ),
        "monthly_report": pd.DataFrame(
            [{"year_month": year_month, "threshold_profit_increase": 100.0, "recommended_driver_count": 1, "avg_net_profit_increase_per_driver": 120.0}]
        ),
    }
    for dataset, frame in frames.items():
        path = root / dataset / f"year_month={year_month}" / f"{dataset}.csv"
        path.parent.mkdir(parents=True)
        frame.to_csv(path, index=False)


def test_정상_Gold_3종은_검증을_통과한다(tmp_path):
    _write_gold(tmp_path, "2026-05")

    dag_module.validate_gold_outputs(str(tmp_path), "2026-05")


@pytest.mark.parametrize(
    "violation, expected",
    [
        ("missing", "Gold 산출물이 없습니다"),
        ("empty", "비어 있습니다"),
        ("column", "필수 컬럼 누락"),
        ("year_month", "다른 연월이 섞였습니다"),
    ],
)
def test_Gold_산출물이_계약을_어기면_실패한다(tmp_path, violation, expected):
    _write_gold(tmp_path, "2026-05")
    target = tmp_path / "monthly_report/year_month=2026-05/monthly_report.csv"

    if violation == "missing":
        target.unlink()
    elif violation == "empty":
        pd.read_csv(target).iloc[0:0].to_csv(target, index=False)
    elif violation == "column":
        pd.read_csv(target).drop(columns="recommended_driver_count").to_csv(target, index=False)
    else:
        frame = pd.read_csv(target)
        frame["year_month"] = "2026-04"
        frame.to_csv(target, index=False)

    with pytest.raises((FileNotFoundError, ValueError), match=expected):
        dag_module.validate_gold_outputs(str(tmp_path), "2026-05")


# --- staleness SLA ------------------------------------------------------------


def test_직전성공기록이_없으면_경과일은_None이다():
    assert dag_module.days_since_last_success(None, _logical_date(2026, 8)) is None


def test_경과일은_직전성공_이후_일수다():
    prev = _logical_date(2026, 7)
    now = prev + timedelta(days=40)

    assert dag_module.days_since_last_success(prev, now) == 40


def test_SLA기준일은_Param이_있으면_Variable을_보지_않는다(monkeypatch):
    monkeypatch.setattr(
        dag_module.Variable,
        "get",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("Variable을 조회하면 안 됩니다")),
    )

    assert dag_module.resolve_stale_sla_days({"gold_stale_sla_days": 10}) == 10


def test_SLA기준일은_Param이_없으면_Variable값을_쓴다(monkeypatch):
    monkeypatch.setattr(
        dag_module.Variable, "get", lambda key, default=None: 45
    )

    assert dag_module.resolve_stale_sla_days({"gold_stale_sla_days": None}) == 45


def test_SLA기준일은_Variable도_없으면_기본값_31이다(monkeypatch):
    def fake_get(key, default=None):
        return default  # 실제 Variable.get의 "미설정 시 default 반환" 동작

    monkeypatch.setattr(dag_module.Variable, "get", fake_get)

    assert dag_module.resolve_stale_sla_days({}) == dag_module.DEFAULT_STALE_SLA_DAYS
    assert dag_module.DEFAULT_STALE_SLA_DAYS == 31


def test_SLA기준일은_Variable조회_실패시에도_기본값으로_내려간다(monkeypatch):
    def raising_get(*args, **kwargs):
        raise RuntimeError("실행 컨텍스트 밖")

    monkeypatch.setattr(dag_module.Variable, "get", raising_get)

    assert dag_module.resolve_stale_sla_days({}) == dag_module.DEFAULT_STALE_SLA_DAYS


def test_SLA초과시_staleness_Slack알림을_보낸다(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        dag_module, "slack_stale_alert_callback", lambda context: calls.append(context)
    )
    _write_inputs(tmp_path, "2026-05")
    prev_success = datetime.now(timezone.utc) - timedelta(days=40)

    dag_module.validate_inputs_task.function(
        params=_params(tmp_path, gold_stale_sla_days=10),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "2026-05"})(),
        prev_end_date_success=prev_success,
    )

    assert len(calls) == 1
    assert calls[0]["stale_days"] == 10
    assert calls[0]["days_since_success"] > 10


def test_SLA이내면_staleness_Slack알림을_보내지_않는다(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        dag_module, "slack_stale_alert_callback", lambda context: calls.append(context)
    )
    _write_inputs(tmp_path, "2026-05")

    dag_module.validate_inputs_task.function(
        params=_params(tmp_path, gold_stale_sla_days=31),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "2026-05"})(),
        prev_end_date_success=datetime.now(timezone.utc) - timedelta(days=1),
    )

    assert calls == []
