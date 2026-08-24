"""silver_to_gold `job.py`의 월별 Silver 파일 선택.

연료비 월별 파티션:
1. 더 최신 월이 있어도 Gold 대상 year_month 파일을 고른다
2. 대상 월의 Parquet이나 `_SUCCESS`가 없으면 FileNotFoundError
3. 로컬·S3 모두 같은 지역·월 계약을 쓴다
4. 버전 파일명은 `ny_fuel.parquet`만 허용하고 옛 이름은 읽지 않는다

이슈 #912 (월별 3종의 공개 버전):
5. `_SUCCESS`가 있는 source_collected_at 디렉터리만 공개 버전으로 고른다
6. S3에서도 미완료 디렉터리를 무시하고 완료된 최신 버전을 고른다
7. EMR가 쓰는 EIA 버전 계약은 이미지에 복사되는 `main/common`에서 import한다
이슈 #845 (Gold가 연료비를 올바른 지역으로 읽는지):
11. `main()`이 `--service_area`를 연료비와 월간 3종 최신 경로 조회에 그대로 넘긴다
"""

import boto3
import pytest
from moto import mock_aws

from main.spark.jobs.silver_to_gold import job

S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"
SERVICE_AREA = "NYC"
GAS_TOKEN = "20260824T123456123456Z"
EV_TOKEN = "20260820T123456123456Z"


def test_EMR_EIA버전계약은_main_패키지에_포함된다():
    assert job.fuel_source_tokens.__module__ == "main.common.eia_fuel_version"
    dockerfile = job.PROJECT_ROOT / "shared/spark/Dockerfile"
    assert "COPY main/common/ /home/hadoop/main/common/" in dockerfile.read_text()


def _write_fuel_version(
    partition, gas_token=GAS_TOKEN, ev_token=EV_TOKEN, *, complete=True
):
    version = partition / f"input_version=gas-{gas_token}__ev-{ev_token}"
    version.mkdir(parents=True, exist_ok=True)
    (version / "ny_fuel.parquet").touch()
    if complete:
        (version / "_SUCCESS").touch()
    return version / "ny_fuel.parquet"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        yield client


def test_로컬에_더_최신_월이_있어도_대상월_파일을_고른다(tmp_path):
    for year_month in ("2026-01", "2026-04", "2026-05"):
        partition = tmp_path / "service_area=NYC" / f"year_month={year_month}"
        _write_fuel_version(partition)
    target = tmp_path / "service_area=NYC/year_month=2026-01"
    _write_fuel_version(target, "20260825T123456123456Z", complete=False)
    latest = _write_fuel_version(target, "20260826T123456123456Z")

    result = job.monthly_fuel_price_path(str(tmp_path), "2026-01", SERVICE_AREA)

    assert result == str(latest)


def test_로컬에_대상월_파티션이_없으면_FileNotFoundError(tmp_path):
    with pytest.raises(FileNotFoundError):
        job.monthly_fuel_price_path(str(tmp_path), "2026-01", SERVICE_AREA)


def test_로컬_옛_파일명만_있으면_FileNotFoundError(tmp_path):
    version = (
        tmp_path / "service_area=NYC/year_month=2026-05"
        / f"input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}"
    )
    version.mkdir(parents=True)
    (version / "gas_ev_price.parquet").touch()
    (version / "_SUCCESS").touch()

    with pytest.raises(FileNotFoundError):
        job.monthly_fuel_price_path(str(tmp_path), "2026-05", SERVICE_AREA)


def test_로컬_연료비는_지역별로_자기_파일만_고른다(tmp_path):
    """`max()`/`sorted()` 는 사전순입니다. 지역으로 스코프하지 않으면
    `service_area=TX` 가 뒤로 정렬돼 **월과 무관하게 이기고, 다른 지역 유가로 이 지역
    Gold 를 계산합니다** — 에러 없이 틀린 값이 나오는 경로입니다(#851).

    NYC 를 더 **오래된** 달로 두어, 스코프가 없으면 TX 가 이기는 상황을 만듭니다.
    """
    for area, year_month in (("NYC", "2026-05"), ("TX", "2026-09")):
        partition = tmp_path / f"service_area={area}" / f"year_month={year_month}"
        _write_fuel_version(partition)

    assert job.monthly_fuel_price_path(str(tmp_path), "2026-05", "NYC") == str(
        tmp_path / "service_area=NYC" / "year_month=2026-05"
        / f"input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}" / "ny_fuel.parquet"
    )
    assert job.monthly_fuel_price_path(str(tmp_path), "2026-09", "TX") == str(
        tmp_path / "service_area=TX" / "year_month=2026-09"
        / f"input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}" / "ny_fuel.parquet"
    )


def test_S3_연료비도_지역별로_자기_파일만_고른다(s3_client):
    prefix = "silver/gas_ev_price"
    for area, year_month in (("NYC", "2026-05"), ("TX", "2026-09")):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=(
                f"{prefix}/service_area={area}/year_month={year_month}/"
                f"input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}/ny_fuel.parquet"
            ),
            Body=b"x",
        )
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=(
                f"{prefix}/service_area={area}/year_month={year_month}/"
                f"input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}/_SUCCESS"
            ),
            Body=b"",
        )

    nyc = job.monthly_fuel_price_path(
        f"s3://{S3_BUCKET}/{prefix}", "2026-05", "NYC"
    )

    assert "service_area=NYC" in nyc
    assert "service_area=TX" not in nyc


def test_연료비는_지역없는_옛_경로를_읽지않는다(tmp_path):
    partition = tmp_path / "year_month=2026-05"
    partition.mkdir()
    (partition / "gas_ev_price.parquet").touch()

    with pytest.raises(FileNotFoundError):
        job.monthly_fuel_price_path(str(tmp_path), "2026-05", "NYC")


def test_연료비는_지역없는_옛_경로를_무시한다(tmp_path):
    legacy = tmp_path / "year_month=2026-09"
    legacy.mkdir()
    (legacy / "gas_ev_price.parquet").touch()
    scoped = tmp_path / "service_area=NYC" / "year_month=2026-05"
    scoped_file = _write_fuel_version(scoped)

    assert job.monthly_fuel_price_path(str(tmp_path), "2026-05", "NYC") == str(
        scoped_file
    )


def test_S3에_더_최신_월이_있어도_대상월_파일을_고른다(s3_client):
    prefix = "silver/gas_ev_price"
    for year_month in ("2026-01", "2026-04", "2026-05"):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=(
                f"{prefix}/service_area={SERVICE_AREA}/"
                f"year_month={year_month}/input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}/"
                "ny_fuel.parquet"
            ),
            Body=b"x",
        )
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=(
                f"{prefix}/service_area={SERVICE_AREA}/"
                f"year_month={year_month}/input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}/_SUCCESS"
            ),
            Body=b"",
        )

    result = job.monthly_fuel_price_path(
        f"s3://{S3_BUCKET}/{prefix}", "2026-01", SERVICE_AREA
    )

    assert result == (
        f"s3://{S3_BUCKET}/{prefix}/service_area=NYC/"
        f"year_month=2026-01/input_version=gas-{GAS_TOKEN}__ev-{EV_TOKEN}/"
        "ny_fuel.parquet"
    )


def test_파티션_디렉터리가_없으면_FileNotFoundError(tmp_path):
    with pytest.raises(FileNotFoundError):
        job.latest_partition_file(str(tmp_path), "2026-05", SERVICE_AREA)


def test_로컬_source_collected_at은_SUCCESS가_있는_최신버전만_고른다(tmp_path):
    partition = tmp_path / "service_area=NYC/year_month=2026-05"
    completed = partition / "source_collected_at=20260821T123456123456Z"
    incomplete = partition / "source_collected_at=20260822T123456123456Z"
    for version in (completed, incomplete):
        version.mkdir(parents=True)
        (version / "part-00000.parquet").touch()
    (completed / "_SUCCESS").touch()

    result = job.latest_partition_file(str(tmp_path), "2026-05", SERVICE_AREA)

    assert result == str(completed)


def test_S3_source_collected_at은_SUCCESS가_있는_최신버전만_고른다(s3_client):
    prefix = "silver/driver_vehicle_monthly_snapshot"
    partition = f"{prefix}/service_area=NYC/year_month=2026-05"
    completed = f"{partition}/source_collected_at=20260821T123456123456Z"
    incomplete = f"{partition}/source_collected_at=20260822T123456123456Z"
    for version in (completed, incomplete):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{version}/part-00000.parquet",
            Body=b"x",
        )
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{completed}/_SUCCESS", Body=b"")

    result = job.latest_partition_file(
        f"s3://{S3_BUCKET}/{prefix}", "2026-05", SERVICE_AREA
    )

    assert result == f"s3://{S3_BUCKET}/{completed}"


# --- Gold의 연료비 지역 배선 (#845) -----------------------------------------


class _StopAfterFuelPriceLookup(Exception):
    """나머지 파이프라인(3종 읽기 이후)까지 실행하지 않으려는 신호용 예외."""


class _FakeReader:
    def parquet(self, path):
        return None


class _FakeSpark:
    read = _FakeReader()


def test_main은_service_area를_모든_기본입력_조회에_그대로_넘긴다(monkeypatch):
    monthly_calls = []
    fuel_calls = []

    def _monthly_spy(base_path, year_month, service_area):
        monthly_calls.append((base_path, year_month, service_area))
        return f"{base_path}/service_area={service_area}/year_month={year_month}/data"

    def _spy(fuel_price_dir, year_month, service_area):
        fuel_calls.append((fuel_price_dir, year_month, service_area))
        raise _StopAfterFuelPriceLookup

    monkeypatch.setattr(
        job, "get_or_create_spark_session", lambda name, **kwargs: _FakeSpark()
    )
    monkeypatch.setattr(job, "latest_partition_file", _monthly_spy)
    monkeypatch.setattr(job, "monthly_fuel_price_path", _spy)

    with pytest.raises(_StopAfterFuelPriceLookup):
        job.main(
            [
                "--env", "prod",
                "--bucket", "de-theone",
                "--year", "2026",
                "--month", "5",
                "--service_area", "TX",
            ]
        )

    assert len(monthly_calls) == 3
    assert all(call[2] == "TX" for call in monthly_calls)
    assert fuel_calls == [
        ("s3://de-theone/silver/gas_ev_price", "2026-05", "TX")
    ]


def test_main은_직접_지정한_연료비_파일을_그대로_읽는다(monkeypatch, tmp_path):
    fuel_path = str(tmp_path / "custom-fuel.parquet")
    read_paths = []

    class _Reader:
        def parquet(self, path):
            read_paths.append(path)
            if path == fuel_path:
                raise _StopAfterFuelPriceLookup
            return None

    class _Spark:
        read = _Reader()

    monkeypatch.setattr(
        job, "get_or_create_spark_session", lambda name, **kwargs: _Spark()
    )

    with pytest.raises(_StopAfterFuelPriceLookup):
        job.main(
            [
                "--monthly_taxi_trip_path", str(tmp_path / "trips"),
                "--driver_vehicle_monthly_snapshot_path", str(tmp_path / "drivers"),
                "--lease_vehicle_inventory_path", str(tmp_path / "inventory"),
                "--fuel_price_path", fuel_path,
                "--year", "2026",
                "--month", "1",
                "--service_area", "NYC",
            ]
        )

    assert read_paths[-1] == fuel_path
