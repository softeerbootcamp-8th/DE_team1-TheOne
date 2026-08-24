"""Silver → Gold DAG의 파티션별 실행 Asset 계약과 산출물 검증 시나리오.

1. Gold는 같은 키의 API READY와 Fuel이 처음 모이면 실행, 이후 하나만 갱신돼도 재실행
2. API Silver 3종은 개별 Asset을 발행하지 않고 Fuel만 검증 후 발행
3. Asset 으로 실행된 Gold 는 해당 파티션 키를 대상 월로 사용
4. 대상 연월은 기준일 이하의 최신 HVFHV 파티션이며 수동 파라미터가 우선
5. Asset 실행은 같은 월 Silver가 덜 준비되면 skip, 수동 실행은 실패
6. 같은 월 Silver 4종이 모두 있어야 입력 경로 확정
7. Gold 검증 성공 태스크에만 Slack 완료 알림 연결
8. Gold 3종은 지역 경로만 검증하며 비었거나 필수 컬럼이 없거나 다른 연월이면 실패
9. API Silver는 최신 collected_at 파일만 선택
10. Asset skip 시 Slack skip 알림을 직접 호출
11. SLA 기준일 초과 시 Slack staleness 경고, 기준 이내면 조용히 통과
12. SLA 기준일은 Param이 우선, 없으면 Variable, 그마저 없으면 기본값 31
13. 경과일 계산에 실패해도(now가 None이거나 뺄셈이 안 되는 값) 예외 없이 None
14. 최초완료/재트리거 판정은 로컬은 기존 Gold 산출물, 운영은 서빙 DB 존재로 확인
15. 운영 EMR 대기는 배포 재시작에 안전한 deferrable 모드
"""

import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from airflow.assets.evaluation import AssetEvaluator
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable

from main.airflow.common import assets
from main.airflow.scripts.monthly_taxi_trip_silver_to_gold import tasks as dag_module
from shared.airflow.common.slack_failure_callback import slack_success_callback


GOLD_DAG_MODULE = importlib.import_module("dags.monthly_taxi_trip_silver_to_gold_dag")
GOLD_DAG = GOLD_DAG_MODULE.monthly_taxi_trip_silver_to_gold_dag


def test_운영_Gold_EMR은_배포재시작에_안전하게_대기한다(monkeypatch):
    monkeypatch.setenv("EMR_APPLICATION_ID", "app-test")
    monkeypatch.setenv(
        "EMR_EXECUTION_ROLE_ARN",
        "arn:aws:iam::123456789012:role/theone-spark-emr-exec",
    )
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")
    monkeypatch.setenv(
        "GOLD_DATABASE_URL",
        "postgresql://airflow:secret@example.internal:5432/gold",
    )

    operator = GOLD_DAG_MODULE._emr_build_gold()

    assert operator.wait_for_completion is True
    assert operator.deferrable is True
    assert operator.cancel_on_kill is True
    assert (
        operator.waiter_delay * operator.waiter_max_attempts
        < operator.execution_timeout.total_seconds()
    )


def _params(root: Path, **overrides) -> dict:
    params = {
        "monthly_taxi_trip_path": str(root / "monthly_taxi_trip"),
        "driver_vehicle_monthly_snapshot_path": str(root / "driver_vehicle_monthly_snapshot"),
        "lease_vehicle_inventory_path": str(root / "lease_vehicle_inventory"),
        "fuel_price_path": str(root / "gas_ev_price"),
        "output_dir": str(root / "gold"),
        "year": None,
        "month": None,
    }
    params.update(overrides)
    return params


def _logical_date(year: int, month: int) -> datetime:
    return datetime(year, month, 13, tzinfo=timezone.utc)


def _write_inputs(
    root: Path, year_month: str, service_area: str = "NYC"
) -> None:
    monthly_taxi_trip = (
        root / "monthly_taxi_trip" / f"service_area={service_area}"
        / f"year_month={year_month}"
    )
    monthly_taxi_trip.mkdir(parents=True)
    (monthly_taxi_trip / "part-00000.parquet").touch()

    files = {
        "driver_vehicle_monthly_snapshot": "driver_vehicle_monthly_snapshot.parquet",
        "lease_vehicle_inventory": "lease_vehicle_inventory.parquet",
        "gas_ev_price": "gas_ev_price.parquet",
    }
    for dataset, file_name in files.items():
        partition = (
            root / dataset / f"service_area={service_area}"
            / f"year_month={year_month}"
        )
        partition.mkdir(parents=True)
        (partition / file_name).touch()


def _write_version(root: Path, dataset: str, year_month: str, file_name: str) -> Path:
    path = (
        root / dataset / "service_area=NYC" / f"year_month={year_month}"
        / file_name
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return path


def _write_completed_version(
    root: Path, dataset: str, year_month: str, token: str
) -> Path:
    version = (
        root / dataset / "service_area=NYC" / f"year_month={year_month}"
        / f"source_collected_at={token}"
    )
    version.mkdir(parents=True, exist_ok=True)
    file_name = "part-00000.parquet" if dataset == "monthly_taxi_trip" else "data.parquet"
    (version / file_name).touch()
    (version / "_SUCCESS").touch()
    return version


def _gold_condition_satisfied(*asset_names: str) -> bool:
    timetable = GOLD_DAG.timetable
    keys = {
        asset.name: unique_key
        for unique_key, asset in timetable.asset_condition.iter_assets()
    }
    return AssetEvaluator(None).run(
        timetable.asset_condition,
        {keys[name]: True for name in asset_names},
    )


@pytest.mark.parametrize(
    ("asset_names", "expected"),
    [
        ((assets.API_SILVER_REFRESH_READY.name,), False),
        ((assets.FUEL_PRICE_SILVER.name,), False),
        (
            (
                assets.API_SILVER_REFRESH_READY.name,
                assets.FUEL_PRICE_SILVER.name,
            ),
            True,
        ),
        (
            (
                assets.GOLD_INPUTS_READY.name,
                assets.API_SILVER_REFRESH_READY.name,
            ),
            True,
        ),
        (
            (
                assets.GOLD_INPUTS_READY.name,
                assets.FUEL_PRICE_SILVER.name,
            ),
            True,
        ),
        ((assets.GOLD_INPUTS_READY.name,), False),
    ],
)
def test_Gold_DAG은_최초엔_두입력_이후엔_하나의_갱신으로_실행된다(
    asset_names,
    expected,
):
    assert _gold_condition_satisfied(*asset_names) is expected


def test_Gold_DAG은_지역과_월이_같은_파티션만_결합한다():
    timetable = GOLD_DAG.timetable

    assert isinstance(timetable, PartitionedAssetTimetable)
    assert isinstance(timetable.default_partition_mapper, IdentityMapper)
    # 복합 키(#674)를 그대로 통과시켜야 합니다. IdentityMapper 는 항등이라 코드를
    # 손댈 필요가 없다는 게 이 설계의 핵심 근거인데, 항등이라서 이 단정만으로는
    # 아무것도 검증되지 않습니다. 두 지역이 **서로 다른 파티션으로 남는지**까지
    # 확인해야 "지역별 독립 트리거" 주장이 실제로 성립합니다.
    mapper = timetable.default_partition_mapper
    assert mapper.to_downstream("NYC:2026-05") == "NYC:2026-05"
    assert mapper.to_downstream("TX:2026-05") == "TX:2026-05"
    assert mapper.to_downstream("NYC:2026-06") == "NYC:2026-06"
    assert mapper.to_downstream("NYC:2026-05") != mapper.to_downstream("TX:2026-05")
    assert mapper.to_downstream("NYC:2026-05") != mapper.to_downstream("NYC:2026-06")


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
            self.extra = {}

        def add_partitions(self, keys):
            self.keys.add(keys)

    recorder = Recorder()
    events = {assets.FUEL_PRICE_SILVER: recorder}

    assets.publish_month_partition(events, assets.FUEL_PRICE_SILVER, "2026-05")

    # 지역을 안 넘기면 기본 지역이 붙습니다. 키는 항상 복합 형식입니다(#674) —
    # 생산자가 bare 월을 발행하면 Gold 소비자가 파싱에서 실패하므로, 여기서
    # 형식이 어긋나는 걸 먼저 잡습니다.
    assert recorder.keys == {"NYC:2026-05"}
    assert recorder.extra == {}


def test_지역을_지정하면_그_지역이_파티션키에_들어간다():
    class Recorder:
        def __init__(self):
            self.keys = set()

        def add_partitions(self, keys):
            self.keys.add(keys)

    recorder = Recorder()
    events = {assets.FUEL_PRICE_SILVER: recorder}

    assets.publish_month_partition(
        events, assets.FUEL_PRICE_SILVER, "2026-05", "TX"
    )

    assert recorder.keys == {"TX:2026-05"}


def test_생산자가_bare_월을_발행하면_소비자가_요란하게_실패한다(tmp_path):
    """이 변경에서 가장 위험한 실패는 생산자와 소비자 중 한쪽만 바뀌어 Gold 가
    아무 에러 없이 안 도는 것입니다. 지역 성분이 없는 옛 키를 기본 지역으로
    조용히 받아주면 그 사실이 묻히므로, 형식이 어긋나면 실패하게 둡니다."""
    with pytest.raises(ValueError, match="지역 성분이 없습니다"):
        dag_module.resolve_target_year_month(
            _logical_date(2026, 8),
            _params(tmp_path),
            str(tmp_path / "monthly_taxi_trip"),
            "NYC",
            partition_key="2026-05",
        )


def test_Gold_대상월은_Asset_파티션키를_그대로_사용한다(tmp_path):
    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 8),
        _params(tmp_path),
        str(tmp_path / "monthly_taxi_trip"),
        "NYC",
        partition_key="NYC:2026-05",
    )

    assert resolved == "2026-05"


def test_대상연월은_기준일_이하_최신_HVFHV_파티션이다(tmp_path):
    for year_month in ("2026-03", "2026-05", "2026-09"):
        partition = (
            tmp_path / "monthly_taxi_trip" / "service_area=NYC"
            / f"year_month={year_month}"
        )
        partition.mkdir(parents=True)
        (partition / "part-00000.parquet").touch()

    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6), _params(tmp_path),
        str(tmp_path / "monthly_taxi_trip"), "NYC"
    )

    assert resolved == "2026-05"


def test_year_month_파라미터가_파티션보다_우선한다(tmp_path):
    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6),
        _params(tmp_path, year="2025", month="7"),
        str(tmp_path / "monthly_taxi_trip"),
        "NYC",
    )

    assert resolved == "2025-07"


def test_Silver_4종이_있으면_같은_월_경로를_확정한다(tmp_path):
    _write_inputs(tmp_path, "2026-05")

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")

    assert resolved["year"] == "2026" and resolved["month"] == "5"
    assert resolved["monthly_taxi_trip_path"].endswith(
        "monthly_taxi_trip/service_area=NYC/year_month=2026-05/part-*.parquet"
    )
    assert resolved["driver_vehicle_monthly_snapshot_path"].endswith(
        "service_area=NYC/year_month=2026-05/driver_vehicle_monthly_snapshot.parquet"
    )
    assert resolved["lease_vehicle_inventory_path"].endswith(
        "service_area=NYC/year_month=2026-05/lease_vehicle_inventory.parquet"
    )
    assert resolved["fuel_price_path"].endswith(
        "service_area=NYC/year_month=2026-05/gas_ev_price.parquet"
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

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")

    assert Path(resolved["monthly_taxi_trip_path"]).name == latest
    assert Path(resolved["driver_vehicle_monthly_snapshot_path"]).name == latest
    assert Path(resolved["lease_vehicle_inventory_path"]).name == latest


def test_API_Silver는_SUCCESS가_있는_source_collected_at만_선택한다(tmp_path):
    _write_inputs(tmp_path, "2026-05")
    completed_token = "20260821T123456123456Z"
    incomplete_token = "20260822T123456123456Z"
    for dataset in (
        "monthly_taxi_trip",
        "driver_vehicle_monthly_snapshot",
        "lease_vehicle_inventory",
    ):
        _write_completed_version(tmp_path, dataset, "2026-05", completed_token)
        incomplete = (
            tmp_path / dataset / "service_area=NYC" / "year_month=2026-05"
            / f"source_collected_at={incomplete_token}"
        )
        incomplete.mkdir()
        (incomplete / "part-00000.parquet").touch()

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")

    for key in (
        "monthly_taxi_trip_path",
        "driver_vehicle_monthly_snapshot_path",
        "lease_vehicle_inventory_path",
    ):
        assert Path(resolved[key]).name == f"source_collected_at={completed_token}"


def test_Silver_입력이_빠지면_상류_DAG를_알려준다(tmp_path):
    _write_inputs(tmp_path, "2026-05")
    (
        tmp_path / "gas_ev_price/service_area=NYC/year_month=2026-05/"
        "gas_ev_price.parquet"
    ).unlink()

    with pytest.raises(FileNotFoundError, match="eia_fuel_price_silver_pipeline"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")


def test_Asset실행은_같은월_Silver입력이_덜준비되면_skip한다(tmp_path):
    dag_run = type("DagRun", (), {"partition_key": "NYC:2026-05"})()

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
    dag_run = type("DagRun", (), {"partition_key": "NYC:2026-05"})()

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
        dag_run=type("DagRun", (), {"partition_key": "NYC:2026-05"})(),
    )

    assert calls == []


def test_Gold_입력검증은_같은_지역월의_READY_파티션을_남긴다(tmp_path):
    class Recorder:
        def __init__(self):
            self.keys = set()

        def add_partitions(self, key):
            self.keys.add(key)

    _write_inputs(tmp_path, "2026-05")
    recorder = Recorder()

    dag_module.validate_inputs_task.function(
        params=_params(tmp_path),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "NYC:2026-05"})(),
        outlet_events={assets.GOLD_INPUTS_READY: recorder},
    )

    assert [outlet.name for outlet in GOLD_DAG.get_task("validate_inputs").outlets] == [
        assets.GOLD_INPUTS_READY.name
    ]
    assert recorder.keys == {"NYC:2026-05"}


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


def _write_gold(root: Path, year_month: str, service_area: str) -> None:
    frames = {
        "driver_aggregation": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "monthly_net_profit": 100.0, "monthly_lease_fee": 400.0}]
        ),
        "driver_vehicle_profit_simulation": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "candidate_vehicle_model_id": "MODEL1", "candidate_stock": 10, "manufacturer": "KIA", "model_name": "FORTE", "expected_net_profit_increase": 120.0, "recommendation_reason": "연료비 절감"}]
        ),
        "monthly_report": pd.DataFrame(
            [{"year_month": year_month, "threshold_profit_increase": 100.0, "is_rerun": False, "recommended_driver_count": 1, "avg_net_profit_increase_per_driver": 120.0}]
        ),
    }
    for dataset, frame in frames.items():
        path = (
            root / dataset / f"service_area={service_area}"
            / f"year_month={year_month}" / f"{dataset}.csv"
        )
        path.parent.mkdir(parents=True)
        frame.to_csv(path, index=False)


def test_정상_Gold_3종은_검증을_통과한다(tmp_path):
    _write_gold(tmp_path, "2026-05", "NYC")

    dag_module.validate_gold_outputs(str(tmp_path), "2026-05", "NYC")


def test_Gold검증은_다른지역_산출물을_대신_보지_않는다(tmp_path):
    _write_gold(tmp_path, "2026-05", "NYC")

    with pytest.raises(FileNotFoundError, match="service_area=TX"):
        dag_module.validate_gold_outputs(str(tmp_path), "2026-05", "TX")


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
    _write_gold(tmp_path, "2026-05", "NYC")
    target = tmp_path / "monthly_report/service_area=NYC/year_month=2026-05/monthly_report.csv"

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
        dag_module.validate_gold_outputs(str(tmp_path), "2026-05", "NYC")


def test_validate_gold_task는_해석된_지역을_검증에_넘긴다(monkeypatch):
    calls = []
    monkeypatch.setattr(
        dag_module,
        "validate_gold_outputs",
        lambda output_dir, year_month, service_area: calls.append(
            (output_dir, year_month, service_area)
        ),
    )
    monkeypatch.setenv("SPARK_JOB_ENV", "local")
    task_instance = type(
        "TaskInstance",
        (),
        {"xcom_pull": lambda self, task_ids: {"year_month": "2026-05", "service_area": "TX"}},
    )()

    dag_module.validate_gold_task.function(
        params={"output_dir": "/gold"}, task_instance=task_instance
    )

    assert calls == [("/gold", "2026-05", "TX")]


# --- staleness SLA ------------------------------------------------------------


def test_직전성공기록이_없으면_경과일은_None이다():
    assert dag_module.days_since_last_success(None, _logical_date(2026, 8)) is None


def test_경과일은_직전성공_이후_일수다():
    prev = _logical_date(2026, 7)
    now = prev + timedelta(days=40)

    assert dag_module.days_since_last_success(prev, now) == 40


def test_now가_None이면_경과일은_None이다():
    assert dag_module.days_since_last_success(_logical_date(2026, 7), None) is None


def test_경과일_계산이_실패해도_예외없이_None을_반환한다(caplog):
    class Unsubtractable:
        pass

    with caplog.at_level("WARNING"):
        result = dag_module.days_since_last_success(Unsubtractable(), _logical_date(2026, 8))

    assert result is None
    assert "계산 실패" in caplog.text


def test_이전_성공_DagRun이_없으면_Proxy로_감싸져도_경고없이_None이다(caplog):
    """Airflow 3 TaskSDK는 이전 성공이 없어도 plain None이 아니라 None을 감싼
    lazy_object_proxy.Proxy를 준다(`airflow/sdk/execution_time/task_runner.py`).
    `is None` 검사가 이걸 못 걸러 매 첫 실행마다 TypeError 경고가 났었다(#760)."""
    import lazy_object_proxy

    proxy_wrapping_none = lazy_object_proxy.Proxy(lambda: None)

    with caplog.at_level("WARNING"):
        result = dag_module.days_since_last_success(proxy_wrapping_none, _logical_date(2026, 8))

    assert result is None
    assert "계산 실패" not in caplog.text


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
        dag_run=type("DagRun", (), {"partition_key": "NYC:2026-05"})(),
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
        dag_run=type("DagRun", (), {"partition_key": "NYC:2026-05"})(),
        prev_end_date_success=datetime.now(timezone.utc) - timedelta(days=1),
    )

    assert calls == []


# --- 최초완료/재트리거 판정 ----------------------------------------------------


def test_로컬은_기존_monthly_report가_없으면_최초완료다(tmp_path):
    assert dag_module.resolve_is_rerun(
        "local", "2026-05", _params(tmp_path)
    ) is False


def test_로컬은_기존_monthly_report가_있으면_재트리거다(tmp_path):
    params = _params(tmp_path)
    path = Path(params["output_dir"]) / "monthly_report" / "year_month=2026-05" / "monthly_report.csv"
    path.parent.mkdir(parents=True)
    path.touch()

    assert dag_module.resolve_is_rerun("local", "2026-05", params) is True


def test_운영은_GOLD_DATABASE_URL이_없으면_최초완료로_간주한다(monkeypatch):
    monkeypatch.delenv("GOLD_DATABASE_URL", raising=False)

    assert dag_module.resolve_is_rerun("prod", "2026-05", {}) is False


def test_운영은_Postgres_조회_실패시에도_최초완료로_내려간다(monkeypatch):
    monkeypatch.setenv("GOLD_DATABASE_URL", "postgresql://unreachable")

    def raising_connect(dsn):
        raise RuntimeError("연결 실패")

    monkeypatch.setattr(dag_module.psycopg2, "connect", raising_connect)

    assert dag_module.resolve_is_rerun("prod", "2026-05", {}) is False


class _FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, parameters):
        self.executed = (sql, parameters)

    def fetchone(self):
        return self.row


class _FakeConnection:
    def __init__(self, row):
        self.cursor_obj = _FakeCursor(row)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_운영은_기존_행이_있으면_재트리거다(monkeypatch):
    monkeypatch.setenv("GOLD_DATABASE_URL", "postgresql://gold")
    fake_conn = _FakeConnection(row=(1,))
    monkeypatch.setattr(dag_module.psycopg2, "connect", lambda dsn: fake_conn)

    assert dag_module.resolve_is_rerun("prod", "2026-05", {}) is True
    assert fake_conn.cursor_obj.executed[1] == ("2026-05",)
    assert fake_conn.closed is True


def test_운영은_기존_행이_없으면_최초완료다(monkeypatch):
    monkeypatch.setenv("GOLD_DATABASE_URL", "postgresql://gold")
    fake_conn = _FakeConnection(row=None)
    monkeypatch.setattr(dag_module.psycopg2, "connect", lambda dsn: fake_conn)

    assert dag_module.resolve_is_rerun("prod", "2026-05", {}) is False


def test_정상실행_결과에_최초완료_재트리거_판정이_실린다(tmp_path):
    _write_inputs(tmp_path, "2026-05")

    resolved = dag_module.validate_inputs_task.function(
        params=_params(tmp_path),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "NYC:2026-05"})(),
    )

    assert resolved["is_rerun"] is False


def test_대상지역은_파티션키가_파라미터를_덮어쓴다():
    """`resolve_target_year_month` 와 **우선순위가 반대**입니다. 연월은 수동
    파라미터가 파티션 키를 덮어쓰지만, 지역은 그러면 안 됩니다 — service_area
    파라미터는 기본값(NYC)이 있어서 Asset 트리거 실행에서도 항상 값이 차 있고,
    파라미터를 우선하면 "TX:2026-08" 파티션의 Gold 를 **NYC 로 적재**합니다.
    """
    resolved = dag_module.resolve_target_service_area(
        {"service_area": "NYC"}, partition_key="TX:2026-05"
    )

    assert resolved == "TX"


def test_파티션키가_없으면_파라미터_지역을_쓴다():
    assert dag_module.resolve_target_service_area({"service_area": "TX"}) == "TX"


def test_파티션키도_파라미터도_없으면_기본_지역을_쓴다():
    assert dag_module.resolve_target_service_area({}) == "NYC"


def test_validate_inputs는_대상지역을_함께_반환한다(tmp_path):
    """Spark 잡이 --service_area 로 받아 Gold 자연 키에 넣습니다(#805, #809).
    빠지면 두 지역의 같은 기사 ID 가 한 행으로 취급됩니다."""
    _write_inputs(tmp_path, "2026-05", "TX")

    result = dag_module.validate_inputs_task.function(
        params=_params(tmp_path),
        logical_date=_logical_date(2026, 5),
        dag_run=type("DagRun", (), {"partition_key": "TX:2026-05"})(),
    )

    assert result["service_area"] == "TX"


def _write_scoped_inputs(root: Path, year_month: str, service_area: str) -> None:
    """지역 계층 아래에 Silver 4종을 씁니다."""
    monthly_taxi_trip = (
        root / "monthly_taxi_trip" / f"service_area={service_area}"
        / f"year_month={year_month}"
    )
    monthly_taxi_trip.mkdir(parents=True)
    (monthly_taxi_trip / "part-00000.parquet").touch()

    for dataset, file_name in {
        "driver_vehicle_monthly_snapshot": "driver_vehicle_monthly_snapshot.parquet",
        "lease_vehicle_inventory": "lease_vehicle_inventory.parquet",
        "gas_ev_price": "gas_ev_price.parquet",
    }.items():
        partition = (
            root / dataset / f"service_area={service_area}"
            / f"year_month={year_month}"
        )
        partition.mkdir(parents=True)
        (partition / file_name).touch()


def test_Gold_입력은_지역_계층_아래도_찾는다(tmp_path):
    """#840~#845 가 데이터셋별로 writer 를 옮기는 중에도 Gold 가 찾아야 합니다(#851)."""
    _write_scoped_inputs(tmp_path, "2026-05", "NYC")

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")

    for key in (
        "monthly_taxi_trip_path",
        "driver_vehicle_monthly_snapshot_path",
        "lease_vehicle_inventory_path",
        "fuel_price_path",
    ):
        assert "service_area=NYC" in resolved[key], key


def test_Gold_입력은_비지역_경로로_폴백하지않는다(tmp_path):
    legacy = tmp_path / "monthly_taxi_trip/year_month=2026-05"
    legacy.mkdir(parents=True)
    (legacy / "part-00000.parquet").touch()

    with pytest.raises(FileNotFoundError, match="service_area=NYC"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")


def test_Gold_입력은_지역_경로를_먼저_본다(tmp_path):
    """지역 경로의 입력을 같은 지역의 Gold 입력으로 확정합니다."""
    _write_scoped_inputs(tmp_path, "2026-05", "NYC")

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")

    assert "service_area=NYC" in resolved["monthly_taxi_trip_path"]


def test_available_year_months는_지역_계층_아래도_센다(tmp_path):
    """한 레벨 glob 만 보면 조용히 빈 목록이 되고, 수동 실행 폴백이 '파티션이
    없습니다' 로 죽습니다."""
    _write_scoped_inputs(tmp_path, "2026-05", "NYC")

    assert dag_module.available_year_months(
        tmp_path / "monthly_taxi_trip", "NYC"
    ) == ["2026-05"]
