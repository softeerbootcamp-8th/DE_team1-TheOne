"""월별 HVFHV+taxi_id 데이터와 기사 데이터 제공 시나리오. 이슈 #452.

1. 월별 TLC 입력 수집 → 상태 검증 → 세 원천 생성 → 공개 검증
2. 네트워크 수집만 짧은 지수 백오프로 재시도
3. 생성 Spark 명령은 source 입력과 상태·릴리스 경로만 사용
4. 내부 manifest 행 수·checksum·필수 컬럼 검증
5. API는 manifest를 공개하지 않고 세 Parquet만 다운로드
6. EMR build_source_release는 병렬 자원과 executor 상한을 함께 지정
7. 로컬과 EMR은 seed·bucket_size·bucket 실행 파라미터를 같은 의미로 전달
"""

import hashlib
import io
import json
import sys
import threading
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SNAPSHOT_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA as INVENTORY_SCHEMA,
)
from shared.airflow.common.project_paths import PROJECT_ROOT
from sub.airflow.dags import synthetic_driver_trip_source_dag as dag_module
from sub.airflow.scripts.synthetic_driver_trip_source import (
    spark_operator as operator_module,
)
from sub.airflow.scripts.synthetic_driver_trip_source import tasks as task_module

sys.path.append(str(PROJECT_ROOT))
from sub.source_api.server import LocalDatasetStorage, create_server

DAG = dag_module.synthetic_driver_trip_source_dag


def test_DAG는_월별로_세_원천을_생성하고_API_릴리스를_검증한다():
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
        "--vehicle_master_path",
        "--state_output_dir",
        "--attribution_output_dir",
        "--release_output_dir",
        "--year_month",
        # 조건부 플래그입니다. Param 을 비우면 렌더링 후 사라지지만, 템플릿 원문에는
        # 남아 있어야 합니다 — 없으면 seed 오버라이드 경로가 끊긴 것입니다.
        "--seed",
        "--bucket_size",
        "--test_row_limit",
        "--storage",
    ):
        assert option in command
    assert "bronze_trips" not in command
    assert "trips_path" not in command


class _StubTaskInstance:
    """템플릿이 부르는 xcom_pull 만 흉내냅니다."""

    @staticmethod
    def xcom_pull(**_):
        return {
            "hvfhv_input_path": "h",
            "zone_lookup_path": "z",
            "vehicle_master_path": "v",
            "year_month": "2026-08",
        }


def _render_build_command(seed=None, bucket_size=None, storage="local", bucket="") -> str:
    task = DAG.get_task("build_source_release")
    return DAG.get_template_env().from_string(task.bash_command).render(
        params={
            "seed": seed,
            "bucket_size": bucket_size,
            "state_output_dir": "S",
            "attribution_output_dir": "A",
            "release_output_dir": "R",
            "test_row_limit": 0,
            "storage": storage,
            "bucket": bucket,
        },
        task_instance=_StubTaskInstance(),
    )


def _render_emr_arguments(
    monkeypatch, *, seed=None, bucket_size=None, bucket=None
) -> list[str]:
    monkeypatch.setenv("EMR_APPLICATION_ID", "app-test")
    monkeypatch.setenv(
        "EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::123456789012:role/emr-exec"
    )
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")
    arguments = operator_module.emr_build().job_driver["sparkSubmit"][
        "entryPointArguments"
    ]
    context = {
        "params": {
            "seed": seed,
            "bucket_size": bucket_size,
            "bucket": bucket,
            "state_output_dir": "S",
            "attribution_output_dir": "A",
            "release_output_dir": "R",
            "test_row_limit": 0,
        },
        "task_instance": _StubTaskInstance(),
    }
    template_env = DAG.get_template_env()
    return [template_env.from_string(value).render(context) for value in arguments]


def test_seed_Param을_비우면_플래그가_렌더링되지_않는다():
    """Param 에 기본값을 두면 항상 CLI 로 실려서 generation.json 이 영원히 가려집니다.

    비어 있을 때 플래그 자체가 사라져야 job 이 설정 파일을 읽습니다.
    """
    assert "--seed" not in _render_build_command(seed=None)


def test_seed_Param을_주면_CLI로_전달된다():
    assert "--seed 7" in _render_build_command(seed=7)


def test_bucket_size_Param을_비우면_플래그가_렌더링되지_않는다():
    """Param 에 기본값을 두면 항상 CLI 로 실려서 config 의 allocation.bucket_size 가 가려집니다."""
    assert "--bucket_size" not in _render_build_command(bucket_size=None)


def test_bucket_size_Param을_주면_CLI로_전달된다():
    assert "--bucket_size 20" in _render_build_command(bucket_size=20)


def test_storage_Param은_항상_렌더링된다():
    """seed/bucket_size와 달리 storage는 CLI 쪽에도 기본값(local)이 있어 항상 실립니다."""
    assert "--storage local" in _render_build_command(storage="local")
    assert "--storage s3" in _render_build_command(storage="s3")


def test_bucket_Param을_비우면_플래그가_렌더링되지_않는다():
    assert "--bucket" not in _render_build_command(bucket="")


def test_bucket_Param을_주면_CLI로_전달된다():
    assert "--bucket my-bucket" in _render_build_command(storage="s3", bucket="my-bucket")


def test_EMR도_seed_bucket_size_bucket_Param을_전달한다(monkeypatch):
    arguments = _render_emr_arguments(
        monkeypatch, seed=7, bucket_size=20, bucket="override-lake"
    )

    assert arguments[arguments.index("--seed") + 1] == "7"
    assert arguments[arguments.index("--bucket_size") + 1] == "20"
    assert arguments[arguments.index("--bucket") + 1] == "override-lake"


def test_EMR의_선택_Param을_비우면_config와_환경버킷을_사용한다(monkeypatch):
    arguments = _render_emr_arguments(monkeypatch)

    assert arguments[arguments.index("--seed") + 1] == "config"
    assert arguments[arguments.index("--bucket_size") + 1] == "config"
    assert arguments[arguments.index("--bucket") + 1] == "test-lake"


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


def test_입력검증은_기사_상태_스냅샷이_전혀_없어도_통과한다(tmp_path):
    """event sourcing 이후 `prepare_monthly_state()`가 스스로 부트스트랩하거나
    체크포인트를 이어받으므로(#605/#628), 사전에 기사·차량 상태 스냅샷이 있어야
    한다는 전제 자체가 없다 — 예전(legacy) 검사를 없앤 회귀(실제 Airflow 컨테이너
    첫 실행에서 `FileNotFoundError: 회사 스냅샷 파일이 없습니다`로 재현됨)."""
    hvfhv = tmp_path / "source" / "hvfhv.parquet"
    zone = tmp_path / "source" / "taxi_zone_lookup.csv"
    hvfhv.parent.mkdir()
    hvfhv.touch()
    zone.touch()
    params = {
        **task_module.DEFAULT_PATHS,
        "vehicle_master_dir": str(tmp_path / "silver" / "vehicle_master"),
    }
    vehicle_master = (
        Path(params["vehicle_master_dir"])
        / "collected_date=2026-08-17"
        / "city=new-york"
        / "vehicle_master.parquet"
    )
    vehicle_master.parent.mkdir(parents=True)
    vehicle_master.touch()

    result = task_module.validate_source_inputs(
        {
            "year_month": "2026-09",
            "hvfhv_input_path": str(hvfhv),
            "zone_lookup_path": str(zone),
        },
        params,
    )

    assert result["snapshot_date"] == "2026-09-01"
    assert result["vehicle_master_path"] == str(vehicle_master)


CONFIG_HASH = "0123456789ab"


def _write_release(root, *, manifest_rows=1):
    release = root / "year_month=2026-09"
    release.mkdir(parents=True)
    trip_file = release / "hvfhv_taxi_trips.parquet"
    snapshot_file = release / "driver_vehicle_monthly_snapshot.parquet"
    inventory_file = release / "lease_vehicle_inventory.parquet"
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
                    "snapshot_month": "2026-09",
                    "driver_id": "driver-1",
                    "taxi_id": "taxi-1",
                    "vehicle_model_id": "vehicle-model-1",
                    "manufacturer": "KIA",
                    "model_name": "SPORTAGE",
                    "fuel_type": "GAS",
                    "comfort_eligible": True,
                    "extra_comfort_eligible": False,
                    "weekly_lease_fee": 574.0,
                    "join_date": date(2024, 3, 1),
                    "exit_date": None,
                    "experience_years": 7,
                    "vehicle_since": date(2026, 9, 1),
                    "snapshot_created_at": datetime(2026, 9, 1),
                }
            ],
            schema=SNAPSHOT_SCHEMA,
        ),
        snapshot_file,
    )
    pq.write_table(pa.Table.from_pylist([{}], schema=INVENTORY_SCHEMA), inventory_file)

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
        # 설정 통합 이후 계보 필드. run_id 는 year_month 와 config_hash 로 조립됩니다.
        "run_id": f"2026-09_{CONFIG_HASH}",
        "config_hash": CONFIG_HASH,
        "created_at": "2026-09-10T00:00:00+00:00",
        "datasets": {
            "hvfhv_taxi_trips": metadata(trip_file),
            "driver_vehicle_monthly_snapshot": metadata(snapshot_file),
            "lease_vehicle_inventory": metadata(inventory_file),
        },
    }
    (release / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (release / "quality_report.json").write_text(
        json.dumps({"target_month": "2026-09", "clip_rate": 0.0}), encoding="utf-8"
    )
    return release, manifest


def test_릴리스검증은_manifest_행수_checksum_필수컬럼을_확인한다(tmp_path):
    _write_release(tmp_path)

    task_module.validate_release(tmp_path, "2026-09", 42)


def test_계보필드가_없는_옛_릴리스는_복구방법과_함께_실패한다(tmp_path):
    release, manifest = _write_release(tmp_path)
    del manifest["run_id"]
    del manifest["config_hash"]
    (release / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError) as error:
        task_module.validate_release(tmp_path, "2026-09", 42)
    # 메시지만으로 다음 사람이 복구할 수 있어야 합니다.
    assert "run_id" in str(error.value)
    assert "rm -rf" in str(error.value)


def test_run_id가_year_month_config_hash와_어긋나면_실패한다(tmp_path):
    release, manifest = _write_release(tmp_path)
    manifest["run_id"] = "2026-08_0123456789ab"
    (release / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="run_id가 year_month"):
        task_module.validate_release(tmp_path, "2026-09", 42)


def test_seed를_비우면_manifest_seed를_비교하지_않는다(tmp_path):
    """Param 을 비운 실행은 config 의 global_seed 를 쓴 것이라 맞춰 볼 요청값이 없습니다."""
    _write_release(tmp_path)
    task_module.validate_release(tmp_path, "2026-09", None)


def test_릴리스행수가_manifest와_다르면_실패한다(tmp_path):
    _write_release(tmp_path, manifest_rows=2)

    with pytest.raises(ValueError, match="행 수가 manifest와 다릅니다"):
        task_module.validate_release(tmp_path, "2026-09", 42)


def test_품질리포트가_없으면_실패한다(tmp_path):
    release, _ = _write_release(tmp_path)
    (release / "quality_report.json").unlink()

    with pytest.raises(ValueError, match="품질 리포트가 없습니다"):
        task_module.validate_release(tmp_path, "2026-09", 42)


def test_API는_manifest를_공개하지않고_세_Parquet만_다운로드한다(tmp_path):
    """manifest의 dataset 키는 생성 DAG가 쓰는 이름 그대로지만, 공개 API는 이름이
    다른 것(hvfhv_taxi_trips -> monthly_taxi_trip)이 있어 URL은 그걸로 만듭니다
    (LocalDatasetStorage의 번역표 참고)."""
    release, manifest = _write_release(tmp_path)
    server = create_server(LocalDatasetStorage(tmp_path), port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    public_names = {"hvfhv_taxi_trips": "monthly_taxi_trip"}
    try:
        for path in ("/v1/data/latest", "/v1/data/2026-09"):
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(f"{base_url}{path}")
            assert exc_info.value.code == 404

        for dataset in manifest["datasets"]:
            public_name = public_names.get(dataset, dataset)
            dataset_url = f"{base_url}/v1/data/2026-09/datasets/{public_name}"
            with urllib.request.urlopen(dataset_url) as response:
                assert response.headers["Content-Type"] == (
                    "application/vnd.apache.parquet"
                )
                assert response.read() == (
                    release / manifest["datasets"][dataset]["file"]
                ).read_bytes()

            with urllib.request.urlopen(
                f"{base_url}/v1/data/latest/datasets/{public_name}"
            ) as response:
                assert response.geturl() == dataset_url
                assert response.read() == (
                    release / manifest["datasets"][dataset]["file"]
                ).read_bytes()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


# --- 트리거 폼에서 비워둘 수 있어야 하는 파라미터 -----------------------------

def test_bucket_은_비워둘_수_있다():
    """`type` 에 "null" 이 없으면 UI 트리거 폼이 필수 입력으로 취급합니다.

    버킷은 드물게 쓰는 재정의값이고, 비우면 DATA_LAKE_S3_BUCKET 을 쓰는 것이 정상
    경로입니다(다른 DAG 들은 파라미터 없이 환경변수만 씁니다). 필수가 되면 EC2 에서
    매번 손으로 넣어야 하고, 그러다 `s3://버킷` 처럼 잘못 넣으면 조회가 깨집니다.
    """
    param = DAG.params.get_param("bucket")

    assert param.resolve(None) is None
    assert param.resolve("de-theone") == "de-theone"


def test_bucket_을_비우면_spark_명령에_플래그가_안_붙는다():
    # 빈 값으로 `--bucket ` 만 붙으면 argparse 가 다음 인자를 값으로 삼습니다.
    command = DAG.get_task("build_source_release").bash_command

    assert "{% if params.bucket %}--bucket" in command


# --- EMR Serverless 분기 (#767) -----------------------------------------------
#
# EC2 컨테이너에서 local[3] Spark 로 돌면 Airflow scheduler·Postgres 와 자원을
# 다투고 t4g 가 오래 붙잡힙니다. main 쪽 DAG 두 개와 같은 JOB_ENV 분기를 씁니다.

def test_기본값은_local_이라_기존_Bash_경로를_쓴다():
    """로컬 pyspark 는 hadoop-aws jar 이 없어 s3:// 를 못 읽습니다(#712).

    그래서 기본값이 prod 면 로컬 개발이 통째로 깨집니다.
    """
    assert operator_module.JOB_ENV == "local"

    operator = operator_module.local_build()

    assert type(operator).__name__ == "BashOperator"
    assert "driver_assignment/source_job.py" in operator.bash_command


def test_운영은_EMR_Serverless_로_제출하고_완료까지_기다린다(monkeypatch):
    monkeypatch.setenv("EMR_APPLICATION_ID", "app-test")
    monkeypatch.setenv("EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::123456789012:role/emr-exec")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")

    operator = operator_module.emr_build()
    spark_submit = operator.job_driver["sparkSubmit"]

    assert type(operator).__name__ == "EmrServerlessStartJobOperator"
    assert operator.application_id == "app-test"
    assert operator.wait_for_completion is True
    assert spark_submit["entryPoint"] == operator_module.EMR_ENTRY_POINT
    assert (
        operator.configuration_overrides["monitoringConfiguration"][
            "s3MonitoringConfiguration"
        ]["logUri"]
        == "s3://test-lake/emr-logs/"
    )


def test_EMR_build_source_release는_최적화된_2core_worker와_상한을_지정한다(monkeypatch):
    monkeypatch.setenv("EMR_APPLICATION_ID", "app-test")
    monkeypatch.setenv("EMR_EXECUTION_ROLE_ARN", "arn:aws:iam::123456789012:role/emr-exec")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")

    parameters = operator_module.emr_build().job_driver["sparkSubmit"][
        "sparkSubmitParameters"
    ]

    for expected in (
        "spark.driver.cores=2",
        "spark.driver.memory=6g",
        "spark.driver.memoryOverhead=2g",
        "spark.executor.cores=2",
        "spark.executor.memory=6g",
        "spark.executor.memoryOverhead=2g",
        "spark.dynamicAllocation.minExecutors=1",
        "spark.dynamicAllocation.initialExecutors=3",
        "spark.dynamicAllocation.maxExecutors=5",
    ):
        assert f"--conf {expected}" in parameters


def test_EMR_은_storage_를_s3_로_고정한다(monkeypatch):
    """params 의 storage 를 그대로 쓰면 local 로 둔 채 제출될 수 있습니다.

    그러면 EMR 워커가 컨테이너 로컬 디스크(빈 디렉터리)를 보게 됩니다.
    """
    arguments = _render_emr_arguments(monkeypatch)

    assert arguments[arguments.index("--storage") + 1] == "s3"
    assert arguments[arguments.index("--bucket") + 1] == "test-lake"


@pytest.mark.parametrize(
    "missing",
    ["EMR_APPLICATION_ID", "EMR_EXECUTION_ROLE_ARN", "DATA_LAKE_S3_BUCKET"],
)
def test_운영_필수변수가_없으면_누락된_이름으로_실패한다(monkeypatch, missing):
    for name in ("EMR_APPLICATION_ID", "EMR_EXECUTION_ROLE_ARN", "DATA_LAKE_S3_BUCKET"):
        monkeypatch.setenv(name, "value")
    monkeypatch.delenv(missing)

    with pytest.raises(ValueError, match=missing):
        operator_module.emr_build()


def test_알_수_없는_SPARK_JOB_ENV는_거부한다(monkeypatch):
    monkeypatch.setattr(operator_module, "JOB_ENV", "staging")

    with pytest.raises(ValueError, match="알 수 없는 SPARK_JOB_ENV"):
        operator_module.build_operator()


def test_storage_기본값은_SPARK_JOB_ENV를_따라간다():
    """prod 인데 local 이 기본이면 수집은 컨테이너 디스크, 제출은 s3 로 갈라집니다."""
    assert operator_module.DEFAULT_STORAGE == "local"
    assert DAG.params["storage"] == "local"
