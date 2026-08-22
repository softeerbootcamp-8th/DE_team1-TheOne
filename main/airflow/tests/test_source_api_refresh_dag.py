"""원천 API 변경 subset → Raw/Silver → 단일 READY Asset 계약.

1. latest를 찾은 뒤 ETag·Last-Modified를 조건부 HEAD에 함께 전달
2. 304는 미변경, 200은 변경으로 판정하고 대상 월·version을 반환
3. 원천별 변경 분기는 서로 독립적으로 하위 DAG를 실행
4. 성공한 원천만 상태를 기록하고 실패한 분기는 다음 실행에 남김
5. 변경된 하위 DAG를 모두 기다린 뒤 READY Asset을 정확히 한 번 발행
6. 모두 미변경이거나 하나라도 실패하면 READY Asset을 발행하지 않음
7. 확정된 연월과 API 주소를 하위 DAG trigger conf로 전달
8. refresh DAG가 내부 Source API 기본 주소를 사용
"""

import requests
from airflow.task.trigger_rule import TriggerRule

from dags.source_api_refresh_dag import SOURCES, source_api_refresh_dag
from main.airflow.common import assets
from main.airflow.scripts.source_api_refresh import tasks as task_module


API_BASE_URL = "https://company.example"
YEAR_MONTH = "2026-08"
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


def test_이전_validator를_조건부_HEAD에_보내고_304를_미변경으로_판정한다(
    monkeypatch,
):
    dataset = "monthly_taxi_trip"
    responses = iter(
        [
            _latest_response(dataset),
            _response(
                304,
                f"{API_BASE_URL}/v1/data/{YEAR_MONTH}/datasets/{dataset}",
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
    }


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
        year="2026",
        month="8",
    )

    assert calls[0][0] == f"{API_BASE_URL}/v1/data/2026-08/datasets/{dataset}"
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


def test_하위DAG_trigger는_확정된_연월과_API주소를_conf로_전달한다():
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

    assert recorder.keys == {YEAR_MONTH}


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
            "api_base_url": API_BASE_URL,
            "year_month": YEAR_MONTH,
            "etag": ETAG,
            "last_modified": LAST_MODIFIED,
        }
    )

    assert written == {
        "key": f"{task_module.STATE_KEY_PREFIX}monthly_taxi_trip",
        "value": {
            "api_base_url": API_BASE_URL,
            "year_month": YEAR_MONTH,
            "etag": ETAG,
            "last_modified": LAST_MODIFIED,
        },
        "serialize_json": True,
    }
