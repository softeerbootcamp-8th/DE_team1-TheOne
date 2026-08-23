"""`storage=s3` 일 때 원천 수집·검증이 S3 를 보는지 (#767).

EMR Serverless 워커는 Airflow 컨테이너의 로컬 디스크를 볼 수 없습니다. 그래서
`collect_source_input` 은 내려받은 TLC 원본과 zone lookup 을 `source/raw/` 에 올리고
`s3://` URI 를 XCom 으로 넘겨야 합니다.

moto 를 쓰지 않는 이유 — 이 테스트는 airflow 런타임에서 도는데 거기엔 moto 가 없습니다
(`main/airflow/pyproject.toml`). S3 접점 두 개(`_s3_object_exists`/`_upload_raw`)를
시임으로 두고 그 호출을 검증합니다.
"""

from datetime import datetime, timezone

import pytest

from sub.airflow.scripts.synthetic_driver_trip_source import tasks as task_module

BUCKET = "test-lake"


@pytest.fixture
def s3_seam(monkeypatch):
    """S3 에 있는 키 집합과 업로드 기록을 들고 있는 가짜 S3."""
    state = {"keys": set(), "uploaded": []}
    monkeypatch.setattr(
        task_module, "_s3_object_exists", lambda bucket, key: key in state["keys"]
    )

    def upload(local, bucket, key):
        state["uploaded"].append((str(local), key))
        state["keys"].add(key)
        return f"s3://{bucket}/{key}"

    monkeypatch.setattr(task_module, "_upload_raw", upload)
    return state


def _params(tmp_path, **overrides):
    params = {
        "source_input_dir": str(tmp_path / "inputs"),
        "release_output_dir": str(tmp_path / "release"),
        "storage": "s3",
        "bucket": BUCKET,
    }
    params.update(overrides)
    return params


def test_버킷이_비면_수집_전에_실패한다(tmp_path, monkeypatch):
    """조용히 넘기면 한참 뒤 boto3 의 InvalidBucketName 으로 터집니다."""
    monkeypatch.delenv("DATA_LAKE_S3_BUCKET", raising=False)

    with pytest.raises(ValueError, match="DATA_LAKE_S3_BUCKET"):
        task_module._s3_bucket(_params(tmp_path, bucket=None))


def test_버킷_파라미터가_비면_환경변수를_쓴다(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "de-theone")

    assert task_module._s3_bucket(_params(tmp_path, bucket=None)) == "de-theone"


def test_storage_local_이면_S3를_아예_보지_않는다(tmp_path):
    assert task_module._s3_bucket(_params(tmp_path, storage="local")) is None


def test_내려받은_원본과_zone_lookup을_source_raw에_올린다(tmp_path, s3_seam, monkeypatch):
    source_input_dir = tmp_path / "inputs"
    hvfhv = task_module._source_input_file(source_input_dir, "2026-08")
    hvfhv.parent.mkdir(parents=True)
    hvfhv.write_bytes(b"parquet")
    monkeypatch.setattr(task_module.pq, "read_schema", lambda _: None)
    zone = source_input_dir / "taxi_zone_lookup.csv"
    zone.write_text("LocationID,Borough\n1,EWR\n", encoding="utf-8")
    params = _params(tmp_path)

    hvfhv_uri = task_module._collect_hvfhv("2026", "08", params, BUCKET)
    zone_uri = task_module._collect_zone_lookup(params, BUCKET)

    assert hvfhv_uri == f"s3://{BUCKET}/source/raw/hvfhv/year_month=2026-08/hvfhv.parquet"
    assert zone_uri == f"s3://{BUCKET}/source/raw/taxi_zone_lookup.csv"
    assert [key for _, key in s3_seam["uploaded"]] == [
        "source/raw/hvfhv/year_month=2026-08/hvfhv.parquet",
        "source/raw/taxi_zone_lookup.csv",
    ]


def test_S3에_이미_있으면_내려받지_않는다(tmp_path, s3_seam, monkeypatch):
    """월별 HVFHV Parquet 은 수백 MB 입니다 — 재시도마다 다시 받으면 안 됩니다."""
    s3_seam["keys"].add("source/raw/hvfhv/year_month=2026-08/hvfhv.parquet")

    def fail(*_args, **_kwargs):
        raise AssertionError("S3 에 있는데 TLC 를 다시 내려받았습니다")

    monkeypatch.setattr(task_module, "fetch_tlc_hvfhv", fail)

    uri = task_module._collect_hvfhv("2026", "08", _params(tmp_path), BUCKET)

    assert uri == f"s3://{BUCKET}/source/raw/hvfhv/year_month=2026-08/hvfhv.parquet"
    assert s3_seam["uploaded"] == []


def test_storage_local_이면_로컬_경로를_그대로_넘긴다(tmp_path, s3_seam, monkeypatch):
    source_input_dir = tmp_path / "inputs"
    hvfhv = task_module._source_input_file(source_input_dir, "2026-08")
    hvfhv.parent.mkdir(parents=True)
    hvfhv.write_bytes(b"parquet")
    monkeypatch.setattr(task_module.pq, "read_schema", lambda _: None)

    uri = task_module._collect_hvfhv("2026", "08", _params(tmp_path), None)

    assert uri == str(hvfhv)
    assert s3_seam["uploaded"] == []


def test_이미_S3에_발행한_달은_다시_고르지_않는다(tmp_path, s3_seam):
    """로컬 manifest 만 보면 storage=s3 에서는 같은 달을 무한히 재생성합니다."""
    for year_month in ("2026-07", "2026-06"):
        s3_seam["keys"].add(
            f"source/published/NYC/_manifests/year_month={year_month}.json"
        )
    s3_seam["keys"].add("source/raw/hvfhv/year_month=2026-05/hvfhv.parquet")

    year_month = task_module.resolve_source_year_month(
        datetime(2026, 8, 10, tzinfo=timezone.utc),
        _params(tmp_path),
        is_available=lambda *_: False,
    )

    assert year_month == "2026-05"


def test_입력_검증은_s3_URI를_스킴을_보존해_통과시킨다(tmp_path, s3_seam, monkeypatch):
    hvfhv_key = "source/raw/hvfhv/year_month=2026-08/hvfhv.parquet"
    zone_key = "source/raw/taxi_zone_lookup.csv"
    s3_seam["keys"].update({hvfhv_key, zone_key})
    monkeypatch.setattr(
        task_module, "resolve_vehicle_master_path", lambda *_a, **_k: "s3://x/vm.parquet"
    )

    result = task_module.validate_source_inputs(
        {
            "year_month": "2026-08",
            "hvfhv_input_path": f"s3://{BUCKET}/{hvfhv_key}",
            "zone_lookup_path": f"s3://{BUCKET}/{zone_key}",
        },
        _params(tmp_path, vehicle_master_dir="ignored"),
    )

    assert result["hvfhv_input_path"] == f"s3://{BUCKET}/{hvfhv_key}"
    assert result["zone_lookup_path"] == f"s3://{BUCKET}/{zone_key}"


def test_S3에_입력이_없으면_이름과_URI를_담아_실패한다(tmp_path, s3_seam):
    with pytest.raises(FileNotFoundError, match="hvfhv_input_path"):
        task_module.validate_source_inputs(
            {
                "year_month": "2026-08",
                "hvfhv_input_path": f"s3://{BUCKET}/source/raw/missing.parquet",
                "zone_lookup_path": f"s3://{BUCKET}/source/raw/taxi_zone_lookup.csv",
            },
            _params(tmp_path, vehicle_master_dir="ignored"),
        )


# --- S3 릴리스 검증 -----------------------------------------------------------
#
# `validate_release` 는 로컬 파일만 봅니다. storage=s3 면 릴리스가 S3 에 있어서
# EMR job 이 성공해도 마지막 task 가 항상 실패합니다.

def _release_manifest(year_month="2026-08", seed=42, **overrides):
    manifest = {
        "year_month": year_month,
        "seed": seed,
        "run_id": f"{year_month}_abc123",
        "config_hash": "abc123",
        "datasets": {
            name: {
                "key": (
                    f"source/published/NYC/{name}/year_month={year_month}/data.parquet"
                ),
                "row_count": 10,
            }
            for name in task_module.RELEASE_DATASETS
        },
    }
    manifest.update(overrides)
    return manifest


@pytest.fixture
def s3_release(monkeypatch, s3_seam):
    """manifest·품질리포트 본문을 들고 있는 가짜 S3 읽기."""
    import json

    from shared.common import s3_reader

    bodies: dict[str, bytes] = {}

    def publish(year_month="2026-08", manifest=None, quality=True):
        manifest = manifest if manifest is not None else _release_manifest(year_month)
        manifest_key = (
            f"source/published/NYC/_manifests/year_month={year_month}.json"
        )
        bodies[manifest_key] = json.dumps(manifest).encode("utf-8")
        s3_seam["keys"].add(manifest_key)
        for metadata in manifest.get("datasets", {}).values():
            s3_seam["keys"].add(metadata["key"])
        if quality:
            quality_key = (
                f"source/published/NYC/_quality_reports/year_month={year_month}.json"
            )
            bodies[quality_key] = b'{"clip_rate": 0.0}'
            s3_seam["keys"].add(quality_key)

    monkeypatch.setattr(s3_reader, "get_object_bytes", lambda bucket, key: bodies[key])
    return publish


def test_S3_릴리스는_manifest_3종_품질리포트를_모두_확인한다(s3_release):
    s3_release()

    task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스_manifest가_없으면_실패한다(s3_release):
    with pytest.raises(ValueError, match="manifest가 없습니다"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스_seed가_요청과_다르면_실패한다(s3_release):
    s3_release(manifest=_release_manifest(seed=7))

    with pytest.raises(ValueError, match="seed가 요청과 다릅니다"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스는_과거_hvfhv_폴더를_monthly_taxi_trip으로_인정하지_않는다(
    s3_release,
):
    manifest = _release_manifest()
    manifest["datasets"]["monthly_taxi_trip"]["key"] = (
        "source/published/NYC/hvfhv_taxi_trips/"
        "year_month=2026-08/data.parquet"
    )
    s3_release(manifest=manifest)

    with pytest.raises(ValueError, match="monthly_taxi_trip"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스_run_id가_계보와_어긋나면_실패한다(s3_release):
    s3_release(manifest=_release_manifest(run_id="2026-07_abc123"))

    with pytest.raises(ValueError, match="run_id가 year_month"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스_품질리포트가_없으면_실패한다(s3_release):
    s3_release(quality=False)

    with pytest.raises(ValueError, match="품질 리포트가 없습니다"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)


def test_S3_릴리스_행수가_0이면_실패한다(s3_release):
    manifest = _release_manifest()
    manifest["datasets"]["lease_vehicle_inventory"]["row_count"] = 0
    s3_release(manifest=manifest)

    with pytest.raises(ValueError, match="행 수가 0입니다"):
        task_module.validate_release_s3(BUCKET, "2026-08", 42)
