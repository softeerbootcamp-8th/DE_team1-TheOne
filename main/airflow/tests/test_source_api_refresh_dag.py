"""원천 API 변경 subset → Raw/Silver → 단일 READY Asset 계약.

1. latest를 찾은 뒤 ETag·Last-Modified를 조건부 HEAD에 함께 전달
2. 304는 미변경, 200은 변경으로 판정하고 대상 월·version을 반환
3. API가 미변경이어도 대상 월 Bronze가 없으면 하위 DAG를 다시 실행
4. 로컬과 S3에서 대상 월 Bronze가 있으면 미변경 분기를 Skip
5. 원천별 변경 분기는 서로 독립적으로 하위 DAG를 실행
6. 성공한 원천만 상태를 기록하고 실패한 분기는 다음 실행에 남김
7. 변경 또는 복구된 하위 DAG를 모두 기다린 뒤 READY Asset을 정확히 한 번 발행
8. 모두 미변경이거나 하나라도 실패하면 READY Asset을 발행하지 않음
9. 확정된 연월·API 주소·지역을 하위 DAG trigger conf로 전달
10. refresh DAG가 내부 Source API 기본 주소를 사용
"""

import requests
import pytest
from airflow.task.trigger_rule import TriggerRule

from dags.source_api_refresh_dag import SOURCES, source_api_refresh_dag
from main.airflow.common import assets
from main.airflow.scripts.source_api_refresh import tasks as task_module


API_BASE_URL = "https://company.example"
YEAR_MONTH = "2026-08"
# READY 파티션 키는 "{service_area}:{year_month}" 복합 문자열입니다(#674).
READY_PARTITION_KEY = f"NYC:{YEAR_MONTH}"
ETAG = '"47b92fc60237333c9667c4bcbe1c9573-97"'
LAST_MODIFIED = "Fri, 21 Aug 2026 00:00:00 GMT"


def _response(
    status: int,
    url: str,
    headers: dict | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.url = url
    response.headers.update(headers or {})
    response._content = b""
    return response


def _latest_response(dataset: str) -> requests.Response:
    return _response(
        307,
        f"{API_BASE_URL}/v1/data/latest/datasets/{dataset}",
        {"Location": f"/v1/data/{YEAR_MONTH}/datasets/{dataset}"},
    )


def _inspection_result(dataset: str = "monthly_taxi_trip") -> dict:
    return {
        "dataset": dataset,
        "year_month": YEAR_MONTH,
        "year": "2026",
        "month": "08",
        "etag": ETAG,
        "last_modified": LAST_MODIFIED,
        "changed": False,
        "version": "same",
        "api_base_url": API_BASE_URL,
        "service_area": "NYC",
    }


def _mock_unchanged_source(monkeypatch, dataset: str) -> None:
    monkeypatch.setattr(task_module.Variable, "get", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        task_module,
        "inspect_source",
        lambda *args, **kwargs: _inspection_result(dataset),
    )


def _check(dataset: str, service_area: str = "NYC"):
    return task_module.check_and_should_refresh_task.function(
        dataset,
        params={
            "api_base_url": API_BASE_URL,
            "request_timeout": 30,
            "service_area": service_area,
        },
    )


def test_이전_validator를_조건부_HEAD에_보내고_304를_미변경으로_판정한다(
    monkeypatch,
):
    dataset = "monthly_taxi_trip"
    responses = iter(
        [
            _response(
                307,
                f"{API_BASE_URL}/v1/data/latest/datasets/{dataset}?service_area=TX",
                {
                    "Location": (
                        f"/v1/data/{YEAR_MONTH}/datasets/{dataset}?service_area=TX"
                    )
                },
            ),
            _response(
                304,
                f"{API_BASE_URL}/v1/data/{YEAR_MONTH}/datasets/{dataset}?service_area=TX",
                {"ETag": ETAG, "Last-Modified": LAST_MODIFIED},
            ),
        ]
    )
    calls = []

    def head(url, **kwargs):
        calls.append((url, kwargs))
        return next(responses)

    monkeypatch.setattr(task_module.requests, "head", head)

    result = task_module.inspect_source(
        API_BASE_URL,
        dataset,
        service_area="TX",
        previous={
            "api_base_url": API_BASE_URL,
            "year_month": YEAR_MONTH,
            "etag": ETAG,
            "last_modified": LAST_MODIFIED,
        },
    )

    assert calls[1][1]["headers"] == {
        "If-None-Match": ETAG,
        "If-Modified-Since": LAST_MODIFIED,
    }
    assert calls[0][1]["params"] == {"service_area": "TX"}
    assert calls[1][0].endswith("?service_area=TX")
    assert result["changed"] is False
    assert result["year_month"] == YEAR_MONTH
    assert result["version"]


def test_조건부_HEAD의_200응답은_변경으로_판정한다(monkeypatch):
    dataset = "lease_vehicle_inventory"
    responses = iter(
        [
            _latest_response(dataset),
            _response(
                200,
                f"{API_BASE_URL}/v1/data/{YEAR_MONTH}/datasets/{dataset}",
                {"ETag": ETAG, "Last-Modified": LAST_MODIFIED},
            ),
        ]
    )
    monkeypatch.setattr(
        task_module.requests,
        "head",
        lambda *args, **kwargs: next(responses),
    )

    result = task_module.inspect_source(API_BASE_URL, dataset)

    assert result == {
        "dataset": dataset,
        "year_month": YEAR_MONTH,
        "year": "2026",
        "month": "08",
        "etag": ETAG,
        "last_modified": LAST_MODIFIED,
        "changed": True,
        "version": result["version"],
        "api_base_url": API_BASE_URL,
        "service_area": "NYC",
    }


def test_API가_미변경이어도_로컬_Bronze가_없으면_재실행한다(
    tmp_path,
    monkeypatch,
):
    _mock_unchanged_source(monkeypatch, "monthly_taxi_trip")
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))

    result = _check("monthly_taxi_trip")

    assert result["refresh_required"] is True


@pytest.mark.parametrize(
    ("dataset", "dataset_dir"),
    task_module.BRONZE_DATASET_DIRS.items(),
)
def test_API가_미변경이고_로컬_Bronze가_있으면_Skip한다(
    tmp_path,
    monkeypatch,
    dataset,
    dataset_dir,
):
    _mock_unchanged_source(monkeypatch, dataset)
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))
    partition = (
        tmp_path
        / dataset_dir
        / "service_area=NYC"
        / f"year_month={YEAR_MONTH}"
    )
    partition.mkdir(parents=True)
    (partition / "20260822T010203123456Z.parquet").touch()

    assert _check(dataset) is False


def test_API가_미변경이고_새_로컬_Bronze가_있으면_Skip한다(tmp_path, monkeypatch):
    dataset = "monthly_taxi_trip"
    _mock_unchanged_source(monkeypatch, dataset)
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))
    data = (
        tmp_path
        / dataset
        / "service_area=NYC"
        / f"year_month={YEAR_MONTH}"
        / "collected_at=20260822T010203123456Z"
        / "data.parquet"
    )
    data.parent.mkdir(parents=True)
    data.touch()

    assert _check(dataset) is False


def test_빈_collected_at_디렉터리는_Bronze로_보지않는다(tmp_path, monkeypatch):
    dataset = "monthly_taxi_trip"
    _mock_unchanged_source(monkeypatch, dataset)
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))
    (
        tmp_path
        / dataset
        / "service_area=NYC"
        / f"year_month={YEAR_MONTH}"
        / "collected_at=20260822T010203123456Z"
    ).mkdir(parents=True)

    assert _check(dataset)["refresh_required"] is True


@pytest.mark.parametrize(
    ("keys", "refresh_required"),
    [
        ([], True),
        (
            [
                "bronze/lease_vehicle_inventory/"
                "service_area=NYC/year_month=2026-08/"
                "20260822T010203123456Z.parquet"
            ],
            False,
        ),
        (
            [
                "bronze/lease_vehicle_inventory/service_area=NYC/"
                "year_month=2026-08/"
                "collected_at=20260822T010203123456Z/data.parquet"
            ],
            False,
        ),
        (
            [
                "bronze/lease_vehicle_inventory/service_area=NYC/"
                "year_month=2026-08/"
                "collected_at=20260822T010203123456Z/"
            ],
            True,
        ),
    ],
)
def test_S3_Bronze_존재여부로_미변경_원천의_재실행을_판정한다(
    monkeypatch,
    keys,
    refresh_required,
):
    dataset = "lease_vehicle_inventory"
    _mock_unchanged_source(monkeypatch, dataset)
    monkeypatch.setenv("BRONZE_STORAGE", "s3")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "lake")
    prefixes = []

    def list_keys(bucket, prefix):
        prefixes.append((bucket, prefix))
        return keys

    monkeypatch.setattr(task_module, "list_keys", list_keys)

    result = _check(dataset)

    if refresh_required:
        assert result["refresh_required"] is True
    else:
        assert result is False
    assert prefixes == [
        (
            "lake",
            "bronze/lease_vehicle_inventory/service_area=NYC/"
            "year_month=2026-08/",
        )
    ]


def test_다른지역_Bronze는_대상지역의_중복수집을_막지않는다(
    tmp_path, monkeypatch
):
    dataset = "monthly_taxi_trip"
    _mock_unchanged_source(monkeypatch, dataset)
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))
    tx_data = (
        tmp_path
        / dataset
        / "service_area=TX"
        / f"year_month={YEAR_MONTH}"
        / "collected_at=20260822T010203123456Z"
        / "data.parquet"
    )
    tx_data.parent.mkdir(parents=True)
    tx_data.touch()

    result = _check(dataset, service_area="NYC")

    assert result["refresh_required"] is True


def test_같은지역과_원본의_두번째_감시는_skip한다(tmp_path, monkeypatch):
    dataset = "monthly_taxi_trip"
    inspections = iter(
        [
            {**_inspection_result(dataset), "changed": True},
            _inspection_result(dataset),
        ]
    )
    monkeypatch.setattr(task_module.Variable, "get", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        task_module, "inspect_source", lambda *args, **kwargs: next(inspections)
    )
    monkeypatch.setenv("BRONZE_STORAGE", "local")
    monkeypatch.setenv("BRONZE_DIR", str(tmp_path))

    assert _check(dataset)["refresh_required"] is True

    data = (
        tmp_path
        / dataset
        / "service_area=NYC"
        / f"year_month={YEAR_MONTH}"
        / "collected_at=20260822T010203123456Z"
        / "data.parquet"
    )
    data.parent.mkdir(parents=True)
    data.touch()

    assert _check(dataset) is False


def test_수동_연월은_정규화한_URL과_trigger값으로_반환한다(monkeypatch):
    dataset = "monthly_taxi_trip"
    calls = []

    def head(url, **kwargs):
        calls.append((url, kwargs))
        return _response(
            200,
            url,
            {"ETag": ETAG, "Last-Modified": LAST_MODIFIED},
        )

    monkeypatch.setattr(task_module.requests, "head", head)

    result = task_module.inspect_source(
        API_BASE_URL,
        dataset,
        service_area="TX",
        year="2026",
        month="8",
    )

    assert calls[0][0] == f"{API_BASE_URL}/v1/data/2026-08/datasets/{dataset}"
    assert calls[0][1]["params"] == {"service_area": "TX"}
    assert result["year_month"] == "2026-08"
    assert (result["year"], result["month"]) == ("2026", "08")


def test_감시DAG는_변경DAG들을_기다리고_READY를_한번만_발행한다():
    assert source_api_refresh_dag.schedule == "@daily"
    assert source_api_refresh_dag.max_active_runs == 1
    assert len(source_api_refresh_dag.tasks) == len(SOURCES) * 3 + 1

    ready = source_api_refresh_dag.get_task("publish_api_refresh_ready")
    marker_ids = {f"mark_processed_{dataset}" for dataset, _ in SOURCES}
    assert ready.upstream_task_ids == marker_ids
    assert ready.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    assert [outlet.name for outlet in ready.outlets] == [
        assets.API_SILVER_REFRESH_READY.name
    ]

    for dataset, child_dag_id in SOURCES:
        gate = source_api_refresh_dag.get_task(
            f"check_and_should_refresh_{dataset}"
        )
        trigger = source_api_refresh_dag.get_task(f"trigger_{dataset}")
        marker = source_api_refresh_dag.get_task(f"mark_processed_{dataset}")

        assert not gate.upstream_task_ids
        assert gate.ignore_downstream_trigger_rules is False
        assert gate.downstream_task_ids == {trigger.task_id, marker.task_id}
        assert marker.upstream_task_ids == {gate.task_id, trigger.task_id}
        assert marker.downstream_task_ids == {ready.task_id}
        assert trigger.trigger_dag_id == child_dag_id
        assert trigger.wait_for_completion is True
        assert trigger.deferrable is True
        assert trigger.reset_dag_run is True


def test_trigger_run_id에_지역이_들어간다():
    """`version` 은 (year_month, etag, last_modified) 해시라, 두 지역이 같은 원천에서
    같은 응답을 받으면 run_id 가 겹칩니다. `reset_dag_run=True` 와 겹치면 **한 지역이
    다른 지역의 DagRun 을 리셋**합니다(#674). 값은 params 가 아니라 xcom 에서 꺼내
    실제로 상태 조회에 쓴 지역과 어긋나지 않게 합니다."""
    for dataset, _ in SOURCES:
        gate_task_id = f"check_and_should_refresh_{dataset}"
        trigger = source_api_refresh_dag.get_task(f"trigger_{dataset}")

        assert (
            f"ti.xcom_pull(task_ids='{gate_task_id}')['service_area']"
            in trigger.trigger_run_id
        )
        # 지역이 월·version 보다 앞에 와야 같은 달의 두 지역이 다른 run_id 가 됩니다.
        assert trigger.trigger_run_id.index("['service_area']") < (
            trigger.trigger_run_id.index("['year_month']")
        )


def test_하위DAG_trigger는_확정된_연월과_API주소와_지역을_conf로_전달한다():
    for dataset, _ in SOURCES:
        gate_task_id = f"check_and_should_refresh_{dataset}"
        trigger = source_api_refresh_dag.get_task(f"trigger_{dataset}")

        assert trigger.conf == {
            "year": (
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['year'] }}}}"
            ),
            "month": (
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['month'] }}}}"
            ),
            "api_base_url": (
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['api_base_url'] }}}}"
            ),
            "service_area": (
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['service_area'] }}}}"
            ),
        }


def test_refresh_DAG는_내부_Source_API_기본주소를_사용한다():
    assert source_api_refresh_dag.params["api_base_url"] == "http://10.0.10.81:8091"


def test_같은월에_여러원천이_변경돼도_READY_파티션은_한번만_발행한다():
    class Recorder:
        def __init__(self):
            self.keys = set()

        def add_partitions(self, key):
            self.keys.add(key)

    class TaskInstance:
        def xcom_pull(self, task_ids):
            assert len(task_ids) == len(SOURCES)
            return [
                {"year_month": YEAR_MONTH, "changed": True},
                {"year_month": YEAR_MONTH, "changed": True},
                False,
            ]

    recorder = Recorder()
    task_module.publish_api_refresh_ready_task.function(
        [f"check_and_should_refresh_{dataset}" for dataset, _ in SOURCES],
        task_instance=TaskInstance(),
        outlet_events={assets.API_SILVER_REFRESH_READY: recorder},
    )

    assert recorder.keys == {READY_PARTITION_KEY}


def test_미변경_Bronze_복구도_READY_파티션을_발행한다():
    class Recorder:
        def __init__(self):
            self.keys = set()

        def add_partitions(self, key):
            self.keys.add(key)

    class TaskInstance:
        def xcom_pull(self, task_ids):
            return [
                {
                    "year_month": YEAR_MONTH,
                    "changed": False,
                    "refresh_required": True,
                }
            ]

    recorder = Recorder()
    task_module.publish_api_refresh_ready_task.function(
        ["check_and_should_refresh_monthly_taxi_trip"],
        task_instance=TaskInstance(),
        outlet_events={assets.API_SILVER_REFRESH_READY: recorder},
    )

    assert recorder.keys == {READY_PARTITION_KEY}


def test_처리완료_validator는_원천별로_기록한다(monkeypatch):
    written = {}
    monkeypatch.setattr(
        task_module.Variable,
        "set",
        lambda key, value, **kwargs: written.update(
            {"key": key, "value": value, **kwargs}
        ),
    )

    task_module.mark_processed_task.function(
        {
            "dataset": "monthly_taxi_trip",
            "service_area": "NYC",
            "api_base_url": API_BASE_URL,
            "year_month": YEAR_MONTH,
            "etag": ETAG,
            "last_modified": LAST_MODIFIED,
        }
    )

    assert written == {
        "key": task_module.state_key("monthly_taxi_trip", "NYC"),
        "value": {
            "api_base_url": API_BASE_URL,
            "year_month": YEAR_MONTH,
            "etag": ETAG,
            "last_modified": LAST_MODIFIED,
        },
        "serialize_json": True,
    }


def test_지역이_다르면_다른_상태키에_기록한다():
    """지역이 상태 키에 없으면 NYC 가 ETag 를 기록한 뒤 TX 가 304 를 받아 자기
    수집을 한 번도 못 하고 건너뜁니다 — 실패가 아니라 성공적인 skip 이라 알림도
    가지 않고, 그다음엔 TX 가 키를 덮어써 NYC 가 굶습니다(#674)."""
    nyc = task_module.state_key("monthly_taxi_trip", "NYC")
    tx = task_module.state_key("monthly_taxi_trip", "TX")

    assert nyc != tx
    assert nyc.startswith(task_module.STATE_KEY_PREFIX)
    assert tx.startswith(task_module.STATE_KEY_PREFIX)
