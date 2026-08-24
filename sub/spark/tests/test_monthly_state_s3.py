"""월별 기사·차량 스냅샷을 S3 에 공개하는 경로 (#767).

`prepare_monthly_state` 가 만드는 `driver_preferences`/`current_driver_vehicle` 두
파일은 하류에서 `spark.read.parquet` 으로 읽습니다. 로컬 디스크에 두면 EMR
Serverless executor 가 못 봐서 `spark.read` 가 죽습니다 — 체크포인트(#763)와 같은
이유로 이 둘도 S3 로 갑니다.

`s3fs` 를 쓰지 않는 이유 — spark 런타임은 numpy/pandas/pyarrow 를 EMR 7.13 이
제공하는 값으로 고정하고, `s3fs` 는 `aiobotocore` 를 끌고 와 `boto3` 핀과 충돌합니다.

Airflow의 선택 Param은 로컬에서 생략되고 EMR에서는 `config`로 전달되지만, 둘 다
`generation.json`을 써야 합니다. 숫자를 주면 config_hash까지 바뀌어야 합니다(#826).
"""

from dataclasses import replace
from datetime import date

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from conftest import TEST_CONFIG_DATA
from sub.config import build_config
from sub.generators.synthetic_driver_trip_source import monthly
from sub.run_context import RunContext
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS
from shared.common.source_published_layout import dataset_key, manifest_key

BUCKET = "test-lake"
REGION = "ap-northeast-2"
SNAPSHOT_DATE = date(2026, 8, 1)


def _make_bucket():
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    return client


def _config(initial_count: int = 30, bootstrap_date: date = SNAPSHOT_DATE):
    data = {k: (dict(v) if isinstance(v, dict) else v) for k, v in TEST_CONFIG_DATA.items()}
    data["driver"] = {**data["driver"], "initial_count": initial_count}
    data["bootstrap"] = {**data["bootstrap"], "snapshot_date": bootstrap_date.isoformat()}
    return build_config(data)




def test_snapshot_root는_storage_s3면_로컬_경로를_무시한다():
    """EMR 워커는 컨테이너 로컬 디스크를 못 봅니다 — 로컬 경로가 새면 executor 가 죽습니다."""
    root = monthly._snapshot_root("/opt/airflow/data/state", storage="s3", bucket=BUCKET)

    assert root == f"s3://{BUCKET}/{monthly.S3_STATE_PREFIX}"


def test_snapshot_root는_storage_local이면_준_경로를_그대로_쓴다():
    assert monthly._snapshot_root("/data/state", storage="local", bucket=None) == "/data/state"


def test_snapshot_root는_버킷_없는_s3를_거부한다():
    with pytest.raises(ValueError, match="bucket"):
        monthly._snapshot_root("/data/state", storage="s3", bucket=None)


def test_알_수_없는_storage는_거부한다():
    with pytest.raises(ValueError, match="알 수 없는 storage"):
        monthly._snapshot_root("/data/state", storage="gcs", bucket=None)


def test_data_month_파티션은_s3_스킴을_보존한다():
    """`Path` 로 join 하면 `s3://b/x` 가 `s3:/b/x` 로 뭉개집니다."""
    partition = monthly._data_month_partition(f"s3://{BUCKET}/state", SNAPSHOT_DATE)

    assert partition == f"s3://{BUCKET}/state/data_month=2026-08"


@mock_aws
def test_exists는_S3_객체를_정확히_한_키로_판정한다():
    client = _make_bucket()
    client.put_object(Bucket=BUCKET, Key="state/data_month=2026-08/a.parquet", Body=b"x")

    assert monthly._exists(f"s3://{BUCKET}/state/data_month=2026-08/a.parquet")
    # prefix 일치를 존재로 보면 반쯤 쓰인 파티션을 완결된 것으로 오인합니다.
    assert not monthly._exists(f"s3://{BUCKET}/state/data_month=2026-08")


@mock_aws
def test_S3_스냅샷은_기존_로컬과_같은_컬럼_집합을_쓴다(tmp_path):
    """로컬과 S3 의 스키마가 갈리면 storage 만 바꿔 돌렸을 때 하류 Spark 가 깨집니다."""
    _make_bucket()
    frame = pd.DataFrame(
        {name: [0] for name in PREFERENCE_COLUMNS} | {"버려질_컬럼": [1]}
    )

    loaded = pd.read_parquet(_bytes_io(monthly._preferences_bytes(frame)))

    assert list(loaded.columns) == list(PREFERENCE_COLUMNS)


def _bytes_io(body: bytes):
    import io

    return io.BytesIO(body)


@mock_aws
def test_현재차량_스냅샷은_날짜를_date32로_강제한다():
    """전부 NaT 면 pandas 추론이 timestamp[ns] 로 남아 Spark 리더가 거부합니다."""
    import pyarrow as pa

    frame = pd.DataFrame(
        {
            "driver_id": ["d1"],
            "joined_on": pd.to_datetime(["2026-01-01"]),
            "lease_started_on": pd.to_datetime(["2026-01-01"]),
            "lease_ended_on": pd.to_datetime([None]),
        }
    )

    table = pa.parquet.read_table(_bytes_io(monthly._current_driver_vehicle_bytes(frame)))

    for name in monthly._CURRENT_DRIVER_VEHICLE_DATE_COLUMNS:
        assert table.schema.field(name).type == pa.date32()


# --- 왕복 (S3 로 공개하고 다시 읽기) -----------------------------------------

def _bootstrap_pools():
    import numpy as np

    return {
        "trip_miles": np.array([1.0, 3.0, 8.0]),
        "trip_time_min": np.array([10.0, 20.0, 40.0]),
    }


def _vehicle_master_silver() -> pd.DataFrame:
    rows = [
        {"make_key": "A", "model_key": "BOTH", "platform": "uber", "product": "Comfort"},
        {"make_key": "A", "model_key": "BOTH", "platform": "lyft", "product": "Extra Comfort"},
        {"make_key": "B", "model_key": "STANDARD", "platform": "uber", "product": "UberX"},
        {"make_key": "C", "model_key": "UBER_ONLY", "platform": "uber", "product": "Comfort"},
        {"make_key": "D", "model_key": "LYFT_ONLY", "platform": "lyft", "product": "Extra Comfort"},
    ]
    prices = {"BOTH": 700.0, "STANDARD": 500.0, "UBER_ONLY": 600.0, "LYFT_ONLY": 650.0}
    return pd.DataFrame([
        {
            **row,
            "vendor": "fasttrack",
            "min_year": 2020,
            "weekly_lease_fee": prices[row["model_key"]],
            "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
            "combined_kwh_per_100mi_min": 0.0, "combined_kwh_per_100mi_max": 0.0,
        }
        for row in rows
    ])


def _upload_vehicle_master(client) -> str:
    import io

    buffer = io.BytesIO()
    _vehicle_master_silver().to_parquet(buffer, index=False)
    key = "source/curated/vehicle_master/data_month=2026-08/data.parquet"
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{key}"


def _upload_published_snapshot(client, *, config, year_month: str, taxi_id: str) -> None:
    import io
    import json

    snapshot = pd.DataFrame(
        {
            "driver_id": ["DRIVER_0"],
            "taxi_id": [taxi_id],
            "join_date": [date(2026, 1, 1)],
            "exit_date": [None],
            "vehicle_since": [date(2026, 1, 1)],
        }
    )
    body = io.BytesIO()
    snapshot.to_parquet(body, index=False)
    key = dataset_key("driver_vehicle_monthly_snapshot", year_month)
    client.put_object(Bucket=BUCKET, Key=key, Body=body.getvalue())
    run = RunContext.create(year_month, config)
    client.put_object(
        Bucket=BUCKET,
        Key=manifest_key(year_month),
        Body=json.dumps(
            {
                "year_month": year_month,
                "run_id": run.run_id,
                "config_hash": run.config_hash,
                "datasets": {"driver_vehicle_monthly_snapshot": {"key": key}},
            }
        ).encode(),
    )


@mock_aws
def test_storage_s3면_스냅샷_두_파일이_S3로_가고_재실행이_같은_결과다(tmp_path, monkeypatch):
    """`vehicle_master` 도 `s3://` 로 받습니다 — pandas 가 boto3 로 읽습니다."""
    client = _make_bucket()
    vehicle_master_uri = _upload_vehicle_master(client)
    monkeypatch.setattr(monthly, "load_bootstrap_pools", lambda **_: _bootstrap_pools())

    first = monthly.prepare_monthly_state(
        hvfhv_input_dir=f"s3://{BUCKET}/source/raw/hvfhv",
        output_dir=tmp_path / "state",
        snapshot_date=SNAPSHOT_DATE,
        config=_config(),
        vehicle_master_path=vehicle_master_uri,
        storage="s3",
        bucket=BUCKET,
    )

    prefix = f"s3://{BUCKET}/{monthly.S3_STATE_PREFIX}/data_month=2026-08"
    assert first.snapshot_dir == prefix
    assert first.preferences_path == f"{prefix}/{monthly.PREFERENCES_FILE}"
    assert first.current_driver_vehicle_path == (
        f"{prefix}/{monthly.CURRENT_DRIVER_VEHICLE_FILE}"
    )
    # 로컬 디스크에 새지 않아야 합니다 — EMR executor 가 못 보는 곳입니다.
    assert not (tmp_path / "state" / "data_month=2026-08").exists()

    rerun = monthly.prepare_monthly_state(
        hvfhv_input_dir=f"s3://{BUCKET}/source/raw/hvfhv",
        output_dir=tmp_path / "state",
        snapshot_date=SNAPSHOT_DATE,
        config=_config(),
        vehicle_master_path=vehicle_master_uri,
        storage="s3",
        bucket=BUCKET,
    )
    assert rerun == first

    from shared.common.s3_reader import read_parquet_uri

    preferences = read_parquet_uri(first.preferences_path)
    current = read_parquet_uri(first.current_driver_vehicle_path)
    assert len(current) == 30
    assert set(current.loc[current["lease_ended_on"].isna(), "driver_id"]) <= set(
        preferences["driver_id"]
    )


@mock_aws
def test_S3_스냅샷은_두_파일이_모두_있어야_완결로_본다(tmp_path, monkeypatch):
    """S3 에는 rename 이 없습니다. 하나만 보고 완결로 치면 반쯤 쓰인 달을 재사용합니다."""
    client = _make_bucket()
    vehicle_master_uri = _upload_vehicle_master(client)
    monkeypatch.setattr(monthly, "load_bootstrap_pools", lambda **_: _bootstrap_pools())
    prefix = f"{monthly.S3_STATE_PREFIX}/data_month=2026-08"
    client.put_object(Bucket=BUCKET, Key=f"{prefix}/{monthly.PREFERENCES_FILE}", Body=b"x")

    state = monthly.prepare_monthly_state(
        hvfhv_input_dir=f"s3://{BUCKET}/source/raw/hvfhv",
        output_dir=tmp_path / "state",
        snapshot_date=SNAPSHOT_DATE,
        config=_config(),
        vehicle_master_path=vehicle_master_uri,
        storage="s3",
        bucket=BUCKET,
    )

    # 반쯤 쓰인 파티션을 재사용하지 않고 다시 만들어 덮어씁니다.
    from shared.common.s3_reader import read_parquet_uri

    assert len(read_parquet_uri(state.preferences_path)) == 30


@mock_aws
def test_계속월은_폐기된_체크포인트가_아니라_전월_published를_승계한다(
    tmp_path, monkeypatch
):
    from sub.generators.synthetic_driver_state import adapters, fleet

    client = _make_bucket()
    vehicle_master_uri = _upload_vehicle_master(client)
    vehicle_pool = adapters.vehicle_pool_from_silver(_vehicle_master_silver())
    taxi_id = fleet.expand_fleet_units(
        fleet.build_fleet_stock(vehicle_pool, driver_count=1)
    ).iloc[0]["taxi_id"]
    config = _config(initial_count=1, bootstrap_date=date(2026, 1, 1))
    _upload_published_snapshot(
        client, config=config, year_month="2026-01", taxi_id=str(taxi_id)
    )
    monkeypatch.setattr(monthly, "load_bootstrap_pools", lambda **_: _bootstrap_pools())

    def legacy_checkpoint_must_not_be_read(*_args, **_kwargs):
        raise AssertionError("폐기된 S3 체크포인트를 조회했습니다")

    monkeypatch.setattr(
        monthly.checkpoint,
        "resolve_previous_checkpoint",
        legacy_checkpoint_must_not_be_read,
    )

    state = monthly.prepare_monthly_state(
        hvfhv_input_dir=f"s3://{BUCKET}/source/raw/hvfhv",
        output_dir=tmp_path / "state",
        snapshot_date=date(2026, 2, 1),
        config=config,
        vehicle_master_path=vehicle_master_uri,
        storage="s3",
        bucket=BUCKET,
    )

    assert state.snapshot_dir.startswith(
        f"s3://{BUCKET}/source/published/NYC/_runtime/"
    )
    current = pd.read_parquet(_bytes_io(client.get_object(
        Bucket=BUCKET,
        Key=state.current_driver_vehicle_path.split(f"s3://{BUCKET}/", 1)[1],
    )["Body"].read()))
    assert current.loc[0, "driver_id"] == "DRIVER_0"
    assert pd.isna(current.loc[0, "lease_ended_on"])


@mock_aws
def test_전월_published_manifest의_config_hash가_다르면_승계를_거부한다():
    import json

    client = _make_bucket()
    config = _config(initial_count=1, bootstrap_date=date(2026, 1, 1))
    client.put_object(
        Bucket=BUCKET,
        Key=manifest_key("2026-01"),
        Body=json.dumps(
            {"run_id": "old", "config_hash": "different", "datasets": {}}
        ).encode(),
    )

    with pytest.raises(monthly.checkpoint.CheckpointLineageError, match="published 릴리스"):
        monthly._resolve_previous_published(
            RunContext.create("2026-02", config), bucket=BUCKET
        )


@mock_aws
def test_부트스트랩월은_전월_published를_요구하지_않는다():
    _make_bucket()
    config = _config(initial_count=1, bootstrap_date=date(2026, 1, 1))

    assert monthly._resolve_previous_published(
        RunContext.create("2026-01", config), bucket=BUCKET
    ) == (None, None, None, None, None)


# --- source_job 의 --env 분기 -------------------------------------------------

def test_env_prod은_storage_s3를_요구한다():
    """조합을 허용하면 executor 가 로컬 경로를 못 찾아 수십 분 뒤에 죽습니다.

    `--storage` 는 입출력을 "어디에" 두는지, `--env` 는 Spark 세션을 "어디서"
    띄우는지라 서로 다른 축입니다. prod + local 만 성립할 수 없는 조합입니다.
    """
    from sub.spark.jobs.driver_assignment import source_job

    with pytest.raises(ValueError, match="--env prod 는 --storage s3"):
        source_job.main(
            [
                "--hvfhv_input_path", "/tmp/a.parquet",
                "--zone_lookup_path", "/tmp/z.csv",
                "--vehicle_master_path", "/tmp/vm.parquet",
                "--state_output_dir", "/tmp/state",
                "--release_output_dir", "/tmp/release",
                "--attribution_output_dir", "/tmp/attribution",
                "--year_month", "2026-08",
                "--env", "prod",
                "--storage", "local",
            ]
        )


@pytest.mark.parametrize(
    ("extra_args", "expected_seed", "expected_bucket_size", "hash_changes"),
    [
        (
            [],
            TEST_CONFIG_DATA["global_seed"],
            TEST_CONFIG_DATA["allocation"]["bucket_size"],
            False,
        ),
        (
            ["--seed", "config", "--bucket_size", "config"],
            TEST_CONFIG_DATA["global_seed"],
            TEST_CONFIG_DATA["allocation"]["bucket_size"],
            False,
        ),
        (
            ["--seed", "config", "--bucket_size", "20"],
            TEST_CONFIG_DATA["global_seed"],
            20,
            True,
        ),
        (["--seed", "7", "--bucket_size", "20"], 7, 20, True),
    ],
)
def test_source_job_선택인자는_config_기본값과_실행계보에_반영된다(
    monkeypatch, extra_args, expected_seed, expected_bucket_size, hash_changes
):
    from sub.spark.jobs.driver_assignment import source_job

    base_config = _config()
    captured = {}

    def capture_config(**kwargs):
        captured["config"] = kwargs["config"]
        raise RuntimeError("설정 확인 완료")

    monkeypatch.setattr(source_job, "load_config", lambda: base_config)
    monkeypatch.setattr(source_job, "prepare_monthly_state", capture_config)

    with pytest.raises(RuntimeError, match="설정 확인 완료"):
        source_job.main(
            [
                "--hvfhv_input_path", "/tmp/a.parquet",
                "--zone_lookup_path", "/tmp/z.csv",
                "--vehicle_master_path", "/tmp/vm.parquet",
                "--state_output_dir", "/tmp/state",
                "--release_output_dir", "/tmp/release",
                "--attribution_output_dir", "/tmp/attribution",
                "--year_month", "2026-08",
                *extra_args,
            ]
        )

    expected = replace(
        base_config,
        global_seed=expected_seed,
        allocation=replace(
            base_config.allocation, bucket_size=expected_bucket_size
        ),
    )
    assert captured["config"] == expected
    actual_hash = RunContext.create("2026-08", captured["config"]).config_hash
    base_hash = RunContext.create("2026-08", base_config).config_hash
    assert (actual_hash != base_hash) is hash_changes


# --- vehicle_master 입력 경로 (#782) ------------------------------------------
#
# EMR 워커는 Airflow 컨테이너 디스크를 못 봅니다. 내려받은 로컬 경로를 넘기면
# `FileNotFoundError: /opt/airflow/project-root/data/source/curated/...` 로 죽습니다.

@mock_aws
def test_vehicle_master는_내려받지_않고_s3_URI를_넘긴다(tmp_path, monkeypatch):
    from sub.generators.synthetic_company_snapshot.generate import (
        resolve_vehicle_master_path,
    )

    client = _make_bucket()
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", BUCKET)
    monkeypatch.setattr("shared.common.env.load_local_env", lambda: None)
    prefix = "source/curated/vehicle_master/"
    for collected in ("2026-08-20", "2026-08-21"):
        client.put_object(
            Bucket=BUCKET,
            Key=f"{prefix}collected_date={collected}/city=new-york/vehicle_master.parquet",
            Body=b"parquet",
        )

    uri = resolve_vehicle_master_path(tmp_path, storage="s3")

    assert uri == (
        f"s3://{BUCKET}/{prefix}collected_date=2026-08-21/city=new-york/vehicle_master.parquet"
    )
    assert not list(tmp_path.rglob("*.parquet"))


def test_env_local은_s3_입력을_경로_이름과_함께_거부한다():
    """로컬 pyspark 는 hadoop-aws jar 이 없어 `s3://` 를 못 읽습니다(#712).

    어느 인자가 s3 인지 알려주지 않으면 hadoop 쪽 스택트레이스만 남습니다.
    """
    from sub.spark.jobs.driver_assignment import source_job

    with pytest.raises(ValueError, match="--vehicle_master_path"):
        source_job.main(
            [
                "--hvfhv_input_path", "/tmp/a.parquet",
                "--zone_lookup_path", "/tmp/z.csv",
                "--vehicle_master_path", f"s3://{BUCKET}/vm.parquet",
                "--state_output_dir", "/tmp/state",
                "--release_output_dir", "/tmp/release",
                "--attribution_output_dir", "/tmp/attribution",
                "--year_month", "2026-01",
                "--env", "local",
                "--storage", "local",
            ]
        )


def test_env_local에_s3_입력이_없으면_거부하지_않는다(monkeypatch):
    """`--storage s3` 로 출력만 S3 에 쓰면서 입력은 로컬로 넘기는 실행을 막지 않습니다."""
    from sub.spark.jobs.driver_assignment import source_job

    def stop_after_guard(**_kwargs):
        raise RuntimeError("가드를 통과했습니다")

    monkeypatch.setattr(source_job, "prepare_monthly_state", stop_after_guard)
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", BUCKET)

    with pytest.raises(RuntimeError, match="가드를 통과했습니다"):
        source_job.main(
            [
                "--hvfhv_input_path", "/tmp/a.parquet",
                "--zone_lookup_path", "/tmp/z.csv",
                "--vehicle_master_path", "/tmp/vm.parquet",
                "--state_output_dir", "/tmp/state",
                "--release_output_dir", "/tmp/release",
                "--attribution_output_dir", "/tmp/attribution",
                "--year_month", "2026-01",
                "--env", "local",
                "--storage", "s3",
            ]
        )
