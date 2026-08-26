"""Silver → Gold DAG의 파티션별 실행 Asset 계약과 산출물 검증 시나리오.

1. Gold는 같은 키의 API READY와 Fuel이 처음 모이면 실행, 이후 하나만 갱신돼도 재실행
2. API Silver 3종은 개별 Asset을 발행하지 않고 Fuel만 검증 후 발행
3. Asset 으로 실행된 Gold 는 소비한 모든 이벤트의 지역·연월 일치 확인
4. 대상 연월은 기준일 이하의 최신 HVFHV 파티션이며 수동 파라미터가 우선
5. Asset 실행은 같은 월 Silver가 덜 준비되면 skip, 수동 실행은 실패
6. 같은 월 Silver 4종이 모두 있어야 입력 경로 확정
7. Gold 검증 성공 태스크에만 Slack 완료 알림 연결
8. Gold 2종은 지역 경로만 검증하며 비었거나 필수 컬럼이 없거나 다른 연월이면 실패
9. API Silver는 최신 collected_at 파일만 선택
10. Asset skip 시 Slack skip 알림을 직접 호출
11. Gold 검증 성공 뒤 지역·월 성공 상태와 READY Asset 기록
12. 최초완료/재트리거 판정은 로컬은 기존 Gold 산출물, 운영은 서빙 DB 존재로 확인
13. 운영 EMR 대기는 배포 재시작에 안전한 deferrable 모드
14. Fuel Silver는 최신 완료 `input_version`의 `fuel.parquet`만 Gold 입력으로 선택
15. 운영 수동 실행도 S3 Silver 4종 완료본이 실제로 있어야 통과
16. 경로 파라미터가 없어도 로컬 기본 경로로 입력을 해석
"""

import importlib
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

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
FUEL_INPUT_VERSION = (
    "input_version=gas-20260820T123456123456Z__ev-20260819T123456123456Z"
)


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


def test_경로_파라미터가_없어도_로컬_기본경로를_쓴다(monkeypatch):
    captured = {}

    def target_year_month(logical_date, params, path, service_area, partition_key):
        captured["target"] = params.copy()
        return "2026-05"

    def input_paths(year_month, params, service_area):
        captured["input"] = params.copy()
        return {"year_month": year_month, "year": "2026", "month": "5"}

    monkeypatch.setattr(dag_module, "resolve_target_year_month", target_year_month)
    monkeypatch.setattr(dag_module, "resolve_input_paths", input_paths)

    result = GOLD_DAG.get_task("validate_inputs").python_callable(
        params={"year": None, "month": None, "service_area": "NYC"},
        logical_date=_logical_date(2026, 5),
        dag_run=SimpleNamespace(partition_key=None),
    )

    assert dag_module.DEFAULT_PATHS.items() <= captured["target"].items()
    assert dag_module.DEFAULT_PATHS.items() <= captured["input"].items()
    assert result["service_area"] == "NYC"


def _triggering_events(api_key: str, fuel_key: str | None = None) -> dict:
    return {
        assets.API_SILVER_REFRESH_READY: [SimpleNamespace(partition_key=api_key)],
        assets.FUEL_PRICE_SILVER: [
            SimpleNamespace(partition_key=fuel_key or api_key)
        ],
    }


class _PartitionRecorder:
    def __init__(self):
        self.keys = set()

    def add_partitions(self, key):
        self.keys.add(key)


def _write_inputs(
    root: Path, year_month: str, service_area: str = "NYC"
) -> None:
    token = "20260820T123456123456Z"
    for dataset in (
        "monthly_taxi_trip",
        "driver_vehicle_monthly_snapshot",
        "lease_vehicle_inventory",
    ):
        _write_completed_version(root, dataset, year_month, token, service_area)

    version = (
        root / "gas_ev_price" / f"service_area={service_area}"
        / f"year_month={year_month}"
        / FUEL_INPUT_VERSION
    )
    version.mkdir(parents=True)
    (version / "fuel.parquet").touch()
    (version / "_SUCCESS").touch()


def _write_completed_version(
    root: Path,
    dataset: str,
    year_month: str,
    token: str,
    service_area: str = "NYC",
) -> Path:
    version = (
        root / dataset / f"service_area={service_area}" / f"year_month={year_month}"
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
        _write_completed_version(
            tmp_path,
            "monthly_taxi_trip",
            year_month,
            "20260820T123456123456Z",
        )

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
        "year_month=2026-05/source_collected_at=20260820T123456123456Z"
    )
    assert resolved["driver_vehicle_monthly_snapshot_path"].endswith(
        "year_month=2026-05/source_collected_at=20260820T123456123456Z"
    )
    assert resolved["lease_vehicle_inventory_path"].endswith(
        "year_month=2026-05/source_collected_at=20260820T123456123456Z"
    )
    assert resolved["fuel_price_path"].endswith(
        "service_area=NYC/year_month=2026-05/"
        f"{FUEL_INPUT_VERSION}/fuel.parquet"
    )


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
    data_file = (
        tmp_path / "gas_ev_price/service_area=NYC/year_month=2026-05/"
        f"{FUEL_INPUT_VERSION}/fuel.parquet"
    )
    data_file.unlink()
    (data_file.parent / "gas_ev_price.parquet").touch()

    with pytest.raises(FileNotFoundError, match="eia_fuel_price_silver_pipeline"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path), "NYC")


def test_Asset실행은_같은월_Silver입력이_덜준비되면_skip한다(tmp_path):
    dag_run = type("DagRun", (), {"partition_key": "NYC:2026-05"})()

    with pytest.raises(AirflowSkipException, match="Silver 4종 준비 대기"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path),
            logical_date=_logical_date(2026, 5),
            dag_run=dag_run,
            triggering_asset_events=_triggering_events("NYC:2026-05"),
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
            triggering_asset_events=_triggering_events("NYC:2026-05"),
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
        triggering_asset_events=_triggering_events("NYC:2026-05"),
    )

    assert calls == []


def test_Gold_입력Asset은_모두_실행대상_지역월과_같아야한다():
    dag_module.validate_triggering_asset_partitions(
        _triggering_events("NYC:2026-05"), "NYC", "2026-05"
    )


@pytest.mark.parametrize(
    ("events", "expected"),
    [
        ({}, "이벤트가 없습니다"),
        (_triggering_events("NYC:2026-05", "TX:2026-05"), "파티션 불일치"),
        (_triggering_events("NYC:2026-05", "NYC:2026-06"), "파티션 불일치"),
    ],
)
def test_Gold_입력Asset의_지역월이_빠지거나_다르면_실패한다(events, expected):
    with pytest.raises(ValueError, match=expected):
        dag_module.validate_triggering_asset_partitions(
            events, "NYC", "2026-05"
        )


def test_Gold_최종검증이_성공해야_READY_파티션을_남긴다(monkeypatch):
    recorder = _PartitionRecorder()
    monkeypatch.setattr(dag_module, "validate_gold_outputs", lambda *args: None)
    monkeypatch.setattr(dag_module, "record_success", lambda *args: None)
    task_instance = type(
        "TaskInstance",
        (),
        {"xcom_pull": lambda self, task_ids: {"year_month": "2026-05", "service_area": "NYC"}},
    )()

    dag_module.validate_gold_task.function(
        params={"output_dir": "/gold"},
        task_instance=task_instance,
        outlet_events={assets.GOLD_INPUTS_READY: recorder},
    )

    assert not GOLD_DAG.get_task("validate_inputs").outlets
    assert [outlet.name for outlet in GOLD_DAG.get_task("validate_gold").outlets] == [
        assets.GOLD_INPUTS_READY.name
    ]
    assert recorder.keys == {"NYC:2026-05"}


def test_Gold_최종검증이_실패하면_READY를_남기지않는다(monkeypatch):
    recorder = _PartitionRecorder()

    def fail_validation(*args):
        raise ValueError("Gold 검증 실패")

    monkeypatch.setattr(dag_module, "validate_gold_outputs", fail_validation)
    task_instance = type(
        "TaskInstance",
        (),
        {"xcom_pull": lambda self, task_ids: {"year_month": "2026-05", "service_area": "NYC"}},
    )()

    with pytest.raises(ValueError, match="Gold 검증 실패"):
        dag_module.validate_gold_task.function(
            params={"output_dir": "/gold"},
            task_instance=task_instance,
            outlet_events={assets.GOLD_INPUTS_READY: recorder},
        )

    assert recorder.keys == set()


def test_수동실행은_Silver입력이_빠지면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="월별 택시 운행 기록 Silver"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path, year="2026", month="5"),
            logical_date=_logical_date(2026, 5),
            dag_run=type("DagRun", (), {"partition_key": None})(),
        )


def test_운영_수동실행은_S3에_대상월이_없으면_실패한다(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_JOB_ENV", "prod")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")
    monkeypatch.setattr(dag_module, "list_keys", lambda bucket, prefix: [], raising=False)

    with pytest.raises(FileNotFoundError, match="2098-02"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path, year="2098", month="2"),
            logical_date=_logical_date(2098, 2),
            dag_run=type("DagRun", (), {"partition_key": None})(),
        )


def test_운영_수동실행은_S3_Silver_4종_완료본이_있으면_통과한다(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SPARK_JOB_ENV", "prod")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")

    def completed_keys(bucket, prefix):
        if "gas_ev_price" in prefix:
            version = (
                f"{prefix}input_version=gas-20260824T123456123456Z"
                "__ev-20260823T123456123456Z/"
            )
            return [f"{version}fuel.parquet", f"{version}_SUCCESS"]
        version = f"{prefix}source_collected_at=20260824T123456123456Z/"
        return [f"{version}data.parquet", f"{version}_SUCCESS"]

    monkeypatch.setattr(dag_module, "list_keys", completed_keys, raising=False)

    result = dag_module.validate_inputs_task.function(
        params=_params(tmp_path, year="2098", month="2"),
        logical_date=_logical_date(2098, 2),
        dag_run=type("DagRun", (), {"partition_key": None})(),
    )

    assert result["year_month"] == "2098-02"


def test_운영_수동실행은_S3_데이터만_있고_SUCCESS가_없으면_실패한다(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SPARK_JOB_ENV", "prod")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")
    monkeypatch.setattr(
        dag_module,
        "list_keys",
        lambda bucket, prefix: [
            f"{prefix}input_version=gas-20260824T123456123456Z"
            "__ev-20260823T123456123456Z/fuel.parquet"
        ],
        raising=False,
    )

    with pytest.raises(FileNotFoundError, match="완료본"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path, year="2098", month="2"),
            logical_date=_logical_date(2098, 2),
            dag_run=type("DagRun", (), {"partition_key": None})(),
        )


def test_운영_Fuel은_옛_파일명에_SUCCESS가_있어도_완료본으로_보지않는다(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SPARK_JOB_ENV", "prod")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")

    def keys_with_legacy_fuel(bucket, prefix):
        version = (
            f"{prefix}input_version=gas-20260824T123456123456Z"
            "__ev-20260823T123456123456Z/"
            if "gas_ev_price" in prefix
            else f"{prefix}source_collected_at=20260824T123456123456Z/"
        )
        name = "data.parquet"
        return [f"{version}{name}", f"{version}_SUCCESS"]

    monkeypatch.setattr(
        dag_module, "list_keys", keys_with_legacy_fuel, raising=False
    )

    with pytest.raises(FileNotFoundError, match="gas_ev_price"):
        dag_module.validate_inputs_task.function(
            params=_params(tmp_path, year="2098", month="2"),
            logical_date=_logical_date(2098, 2),
            dag_run=type("DagRun", (), {"partition_key": None})(),
        )


def test_build_gold는_thresholds_파라미터를_job에_넘긴다():
    """#997 — RevenueFirstAlgorithm이 스윕할 threshold를 job.py에 배선한다."""
    build_gold = GOLD_DAG.get_task("build_gold")

    assert "--thresholds" in build_gold.bash_command
    assert "{{ params.thresholds }}" in build_gold.bash_command


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
        "driver_car_suggestion": pd.DataFrame(
            [{
                "driver_id": "D1", "year_month": year_month, "vehicle_model_id": "MODEL1",
                "manufacturer": "KIA", "model_name": "FORTE",
                "expected_net_profit_increase": 120.0, "recommendation_reason": "연료비 절감",
                "recommendation_algorithm_version_id": 1, "threshold": -1,
            }]
        ),
    }
    for dataset, frame in frames.items():
        path = (
            root / dataset / f"service_area={service_area}"
            / f"year_month={year_month}" / f"{dataset}.csv"
        )
        path.parent.mkdir(parents=True)
        frame.to_csv(path, index=False)


def test_정상_Gold_2종은_검증을_통과한다(tmp_path):
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
    target = tmp_path / "driver_car_suggestion/service_area=NYC/year_month=2026-05/driver_car_suggestion.csv"

    if violation == "missing":
        target.unlink()
    elif violation == "empty":
        pd.read_csv(target).iloc[0:0].to_csv(target, index=False)
    elif violation == "column":
        pd.read_csv(target).drop(columns="vehicle_model_id").to_csv(target, index=False)
    else:
        frame = pd.read_csv(target)
        frame["year_month"] = "2026-04"
        frame.to_csv(target, index=False)

    with pytest.raises((FileNotFoundError, ValueError), match=expected):
        dag_module.validate_gold_outputs(str(tmp_path), "2026-05", "NYC")


def test_validate_gold_task는_해석된_지역을_검증에_넘긴다(monkeypatch):
    calls = []
    successes = []
    monkeypatch.setattr(
        dag_module,
        "validate_gold_outputs",
        lambda output_dir, year_month, service_area: calls.append(
            (output_dir, year_month, service_area)
        ),
    )
    monkeypatch.setattr(
        dag_module,
        "record_success",
        lambda service_area, year_month, completed_at: successes.append(
            (service_area, year_month, completed_at)
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
    assert successes[0][:2] == ("TX", "2026-05")
    assert successes[0][2].tzinfo == timezone.utc


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
        triggering_asset_events=_triggering_events("TX:2026-05"),
    )

    assert result["service_area"] == "TX"


def _write_scoped_inputs(root: Path, year_month: str, service_area: str) -> None:
    """지역 계층 아래에 Silver 4종을 씁니다."""
    _write_inputs(root, year_month, service_area)


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
