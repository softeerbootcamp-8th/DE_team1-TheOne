"""월별 가짜 기사-운행 원천 API 시나리오. 이슈 #452.

1. 월별 TLC 입력 수집 → 상태 검증 → 두 원천 생성 → 공개 검증
2. 네트워크 수집만 짧은 지수 백오프로 재시도
3. 생성 Spark 명령은 source 입력과 상태·릴리스 경로만 사용
4. manifest 행 수·checksum·필수 컬럼 검증
5. API manifest 조회와 두 Parquet 다운로드
"""

import hashlib
import io
import json
import sys
import threading
import urllib.request
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import synthetic_driver_trip_source_dag as dag_module
from common.project_paths import PROJECT_ROOT
from scripts.synthetic_driver_trip_source import tasks as task_module

sys.path.append(str(PROJECT_ROOT))
from synthetic_source_api.server import create_server


DAG = dag_module.synthetic_driver_trip_source_dag


def test_DAG는_월별로_두_원천을_생성하고_API_릴리스를_검증한다():
    assert DAG.dag_id == "synthetic_driver_trip_source_pipeline"
    assert set(DAG.task_ids) == {
        "collect_source_input",
        "validate_inputs",
        "build_source_release",
        "validate_release",
    }
    assert DAG.get_task("collect_source_input").downstream_task_ids == {
        "validate_inputs"
    }
    assert DAG.get_task("validate_inputs").downstream_task_ids == {
        "build_source_release"
    }
    assert DAG.get_task("build_source_release").downstream_task_ids == {
        "validate_release"
    }
    assert DAG.schedule == "0 0 10 * *"
    assert DAG.catchup is False and DAG.max_active_runs == 1
    assert DAG.params["test_row_limit"] == 0


def test_Spark_명령은_DE_Bronze_Silver가_아닌_source_입력만_받는다():
    command = DAG.get_task("build_source_release").bash_command
    for option in (
        "--hvfhv_input_path",
        "--zone_lookup_path",
        "--previous_snapshot_dir",
        "--previous_preferences_path",
        "--state_output_dir",
        "--release_output_dir",
        "--year_month",
        "--seed",
        "--test_row_limit",
    ):
        assert option in command
    assert "bronze_trips" not in command
    assert "trips_path" not in command


def test_임시행제한은_프로덕션과_분리된_경로를_사용한다(tmp_path):
    assert task_module._test_scoped_root(tmp_path, 0) == tmp_path
    assert task_module._test_scoped_root(tmp_path, 20_000) == (
        tmp_path / "_temporary" / "test_row_limit=20000"
    )

    with pytest.raises(ValueError, match="0 이상"):
        task_module._test_scoped_root(tmp_path, -1)


def test_이미_발행한_최신월은_건너뛰고_이전_미발행월을_선택한다(tmp_path):
    release = tmp_path / "release" / "year_month=2026-07"
    release.mkdir(parents=True)
    (release / "manifest.json").touch()
    asked = []

    def available(year, month):
        asked.append(f"{year}-{month}")
        return f"{year}-{month}" == "2026-06"

    resolved = task_module.resolve_source_year_month(
        datetime(2026, 8, 12, tzinfo=timezone.utc),
        {
            "release_output_dir": str(tmp_path / "release"),
            "source_input_dir": str(tmp_path / "source"),
        },
        is_available=available,
    )

    assert resolved == "2026-06"
    assert asked == ["2026-06"]


def test_TLC_월별_Parquet은_전체응답을_메모리에_올리지_않고_저장한다(
    tmp_path, monkeypatch
):
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([{"trip_miles": 1.0}]), sink)

    class Response(io.BytesIO):
        def __init__(self, data):
            super().__init__(data)
            self.read_sizes = []

        def read(self, size=-1):
            self.read_sizes.append(size)
            return super().read(size)

    response = Response(sink.getvalue().to_pybytes())
    monkeypatch.setattr(task_module.urllib.request, "urlopen", lambda *_a, **_k: response)

    written = task_module.fetch_tlc_hvfhv("2026", "07", tmp_path)

    assert pq.ParquetFile(written).metadata.num_rows == 1
    assert response.read_sizes and -1 not in response.read_sizes


def _touch_snapshot(partition):
    partition.mkdir(parents=True)
    for name in ("customer", "lease_contract", "taxi"):
        (partition / f"{name}.parquet").touch()


def test_입력검증은_대상월이_없으면_직전월상태를_선택한다(tmp_path):
    state = tmp_path / "state"
    previous = state / "data_month=2026-08"
    _touch_snapshot(previous)
    preferences = previous / "driver_preferences.parquet"
    preferences.touch()
    hvfhv = tmp_path / "source" / "hvfhv.parquet"
    zone = tmp_path / "source" / "taxi_zone_lookup.csv"
    hvfhv.parent.mkdir()
    hvfhv.touch()
    zone.touch()
    params = {
        **task_module.DEFAULT_PATHS,
        "company_path": str(tmp_path / "company"),
        "state_output_dir": str(state),
        "release_output_dir": str(tmp_path / "release"),
    }

    result = task_module.validate_source_inputs(
        {
            "year_month": "2026-09",
            "hvfhv_input_path": str(hvfhv),
            "zone_lookup_path": str(zone),
        },
        params,
    )

    assert result["snapshot_date"] == "2026-09-01"
    assert result["previous_snapshot_dir"] == str(previous)
    assert result["previous_preferences_path"] == str(preferences)


def _write_release(root, *, manifest_rows=1):
    release = root / "year_month=2026-09"
    release.mkdir(parents=True)
    trip_file = release / "hvfhv_taxi_trips.parquet"
    lease_file = release / "driver_vehicle_leases.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"pickup_datetime": datetime(2026, 9, 2, 9), "taxi_id": "taxi-1"}]
        ),
        trip_file,
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "lease_id": "lease-1",
                    "customer_id": "customer-1",
                    "driver_id": "driver-1",
                    "taxi_id": "taxi-1",
                    "lease_started_on": date(2026, 9, 1),
                    "lease_ended_on": None,
                }
            ]
        ),
        lease_file,
    )

    def metadata(path):
        return {
            "file": path.name,
            "row_count": manifest_rows,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    manifest = {
        "release_id": "2026-09-seed-42",
        "year_month": "2026-09",
        "seed": 42,
        "datasets": {
            "hvfhv_taxi_trips": metadata(trip_file),
            "driver_vehicle_leases": metadata(lease_file),
        },
    }
    (release / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return release, manifest


def test_릴리스검증은_manifest_행수_checksum_필수컬럼을_확인한다(tmp_path):
    _write_release(tmp_path)

    task_module.validate_release(tmp_path, "2026-09", 42)


def test_릴리스행수가_manifest와_다르면_실패한다(tmp_path):
    _write_release(tmp_path, manifest_rows=2)

    with pytest.raises(ValueError, match="행 수가 manifest와 다릅니다"):
        task_module.validate_release(tmp_path, "2026-09", 42)


def test_API는_manifest와_두_Parquet을_다운로드한다(tmp_path):
    release, manifest = _write_release(tmp_path)
    server = create_server(tmp_path, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        with urllib.request.urlopen(f"{base_url}/v1/releases/latest") as response:
            body = json.load(response)
        assert body["release_id"] == manifest["release_id"]
        for dataset in manifest["datasets"]:
            with urllib.request.urlopen(
                f"{base_url}/v1/releases/2026-09/datasets/{dataset}"
            ) as response:
                assert response.read() == (
                    release / manifest["datasets"][dataset]["file"]
                ).read_bytes()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
