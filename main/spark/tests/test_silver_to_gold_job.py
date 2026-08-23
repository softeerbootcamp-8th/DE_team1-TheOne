"""silver_to_gold `job.py`의 최신 파티션 파일 선택.

이슈 #696 (연료비 최신 파티션):
1. 로컬 여러 파티션 중 가장 최근(year_month 최댓값) 파일을 고른다
2. 파티션이 하나도 없으면 FileNotFoundError
3. 파티션 디렉터리는 있는데 parquet 파일이 없으면 FileNotFoundError
4. S3 경로에서도 같은 로직으로 최신 파티션을 고른다

이슈 #759 (월별 3종의 같은 파티션 안 최신 버전):
5. 로컬 파티션 안에 타임스탬프 버전 파일이 여러 개면 최신 파일 하나만 고른다
6. 타임스탬프 버전이 없으면 구 part 파일 전체를 읽는 glob을 반환한다
7. 파티션이 없으면 FileNotFoundError
8. S3에서도 같은 파티션 안 여러 버전 중 최신 하나만 고른다
9. `_SUCCESS`가 있는 source_collected_at 디렉터리만 공개 버전으로 고른다
10. S3에서도 미완료 디렉터리를 무시하고 완료된 최신 버전을 고른다
11. Spark 잡 기본 입력 탐색은 `--service_area`의 지역 경로를 사용한다
"""

import boto3
import pytest
from moto import mock_aws

from main.spark.jobs.silver_to_gold import job

S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        yield client


def test_로컬에서_가장_최근_파티션_파일을_고른다(tmp_path):
    for year_month in ("2026-01", "2026-04", "2026-05"):
        partition = tmp_path / f"year_month={year_month}"
        partition.mkdir()
        (partition / "gas_ev_price.parquet").touch()

    result = job.latest_fuel_price_path(str(tmp_path))

    assert result == str(tmp_path / "year_month=2026-05" / "gas_ev_price.parquet")


def test_로컬에_파티션이_하나도_없으면_FileNotFoundError(tmp_path):
    with pytest.raises(FileNotFoundError):
        job.latest_fuel_price_path(str(tmp_path))


def test_로컬_파티션_디렉터리는_있는데_파일이_없으면_FileNotFoundError(tmp_path):
    (tmp_path / "year_month=2026-05").mkdir()

    with pytest.raises(FileNotFoundError):
        job.latest_fuel_price_path(str(tmp_path))


def test_로컬_연료비는_지역별로_자기_파일만_고른다(tmp_path):
    """`max()`/`sorted()` 는 사전순입니다. 지역으로 스코프하지 않으면
    `service_area=TX` 가 뒤로 정렬돼 **월과 무관하게 이기고, 다른 지역 유가로 이 지역
    Gold 를 계산합니다** — 에러 없이 틀린 값이 나오는 경로입니다(#851).

    NYC 를 더 **오래된** 달로 두어, 스코프가 없으면 TX 가 이기는 상황을 만듭니다.
    """
    for area, year_month in (("NYC", "2026-05"), ("TX", "2026-09")):
        partition = tmp_path / f"service_area={area}" / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / "gas_ev_price.parquet").touch()

    assert job.latest_fuel_price_path(str(tmp_path), "NYC") == str(
        tmp_path / "service_area=NYC" / "year_month=2026-05" / "gas_ev_price.parquet"
    )
    assert job.latest_fuel_price_path(str(tmp_path), "TX") == str(
        tmp_path / "service_area=TX" / "year_month=2026-09" / "gas_ev_price.parquet"
    )


def test_S3_연료비도_지역별로_자기_파일만_고른다(s3_client):
    prefix = "silver/gas_ev_price"
    for area, year_month in (("NYC", "2026-05"), ("TX", "2026-09")):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/service_area={area}/year_month={year_month}/gas_ev_price.parquet",
            Body=b"x",
        )

    nyc = job.latest_fuel_price_path(f"s3://{S3_BUCKET}/{prefix}", "NYC")

    assert "service_area=NYC" in nyc
    assert "service_area=TX" not in nyc


def test_연료비_지역_경로가_없으면_지역없는_경로로_폴백한다(tmp_path):
    """아직 지역 계층으로 옮겨지지 않은 데이터셋도 읽어야 합니다 — 이 폴백이 있어야
    #840~#845 를 데이터셋별로 하나씩 머지할 수 있습니다."""
    partition = tmp_path / "year_month=2026-05"
    partition.mkdir()
    (partition / "gas_ev_price.parquet").touch()

    assert job.latest_fuel_price_path(str(tmp_path), "NYC") == str(
        partition / "gas_ev_price.parquet"
    )


def test_연료비는_지역_경로를_지역없는_경로보다_먼저_본다(tmp_path):
    """순서가 뒤집히면 이미 옮긴 데이터셋이 옛 경로의 낡은 데이터를 집어갑니다."""
    legacy = tmp_path / "year_month=2026-09"
    legacy.mkdir()
    (legacy / "gas_ev_price.parquet").touch()
    scoped = tmp_path / "service_area=NYC" / "year_month=2026-05"
    scoped.mkdir(parents=True)
    (scoped / "gas_ev_price.parquet").touch()

    # 지역 경로가 더 오래된 달이어도 지역 경로가 이겨야 합니다.
    assert job.latest_fuel_price_path(str(tmp_path), "NYC") == str(
        scoped / "gas_ev_price.parquet"
    )


def test_S3에서도_가장_최근_파티션_파일을_고른다(s3_client):
    prefix = "silver/gas_ev_price"
    for year_month in ("2026-01", "2026-04", "2026-05"):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/year_month={year_month}/gas_ev_price.parquet",
            Body=b"x",
        )

    result = job.latest_fuel_price_path(f"s3://{S3_BUCKET}/{prefix}")

    assert result == f"s3://{S3_BUCKET}/{prefix}/year_month=2026-05/gas_ev_price.parquet"


def test_같은_파티션에_버전이_여러개면_최신_하나만_고른다(tmp_path):
    partition = tmp_path / "year_month=2026-05"
    partition.mkdir()
    older = partition / "20260820T123456123456Z.parquet"
    latest = partition / "20260821T123456123456Z.parquet"
    older.touch()
    latest.touch()

    result = job.latest_partition_file(str(tmp_path), "2026-05")

    assert result == str(latest)


def test_타임스탬프_버전이_없으면_구_part파일_전체_glob을_반환한다(tmp_path):
    partition = tmp_path / "year_month=2026-05"
    partition.mkdir()
    (partition / "part-00000.parquet").touch()
    (partition / "part-00001.parquet").touch()

    result = job.latest_partition_file(str(tmp_path), "2026-05")

    assert result == str(partition / "part-*.parquet")


def test_파티션_디렉터리가_없으면_FileNotFoundError(tmp_path):
    with pytest.raises(FileNotFoundError):
        job.latest_partition_file(str(tmp_path), "2026-05")


def test_S3에서도_같은_파티션의_최신_버전만_고른다(s3_client):
    prefix = "silver/driver_vehicle_monthly_snapshot"
    for name in ("20260820T123456123456Z.parquet", "20260821T123456123456Z.parquet"):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/year_month=2026-05/{name}",
            Body=b"x",
        )

    result = job.latest_partition_file(f"s3://{S3_BUCKET}/{prefix}", "2026-05")

    assert result == (
        f"s3://{S3_BUCKET}/{prefix}/year_month=2026-05/20260821T123456123456Z.parquet"
    )


def test_S3_구_part레이아웃은_파일전체_glob을_반환한다(s3_client):
    prefix = "silver/monthly_taxi_trip"
    for name in ("part-00000.parquet", "part-00001.parquet"):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/year_month=2026-05/{name}",
            Body=b"x",
        )

    result = job.latest_partition_file(f"s3://{S3_BUCKET}/{prefix}", "2026-05")

    assert result == (
        f"s3://{S3_BUCKET}/{prefix}/year_month=2026-05/part-*.parquet"
    )


def test_로컬_source_collected_at은_SUCCESS가_있는_최신버전만_고른다(tmp_path):
    partition = tmp_path / "year_month=2026-05"
    completed = partition / "source_collected_at=20260821T123456123456Z"
    incomplete = partition / "source_collected_at=20260822T123456123456Z"
    for version in (completed, incomplete):
        version.mkdir(parents=True)
        (version / "part-00000.parquet").touch()
    (completed / "_SUCCESS").touch()

    result = job.latest_partition_file(str(tmp_path), "2026-05")

    assert result == str(completed)


def test_S3_source_collected_at은_SUCCESS가_있는_최신버전만_고른다(s3_client):
    prefix = "silver/driver_vehicle_monthly_snapshot"
    partition = f"{prefix}/year_month=2026-05"
    completed = f"{partition}/source_collected_at=20260821T123456123456Z"
    incomplete = f"{partition}/source_collected_at=20260822T123456123456Z"
    for version in (completed, incomplete):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{version}/part-00000.parquet",
            Body=b"x",
        )
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{completed}/_SUCCESS", Body=b"")

    result = job.latest_partition_file(f"s3://{S3_BUCKET}/{prefix}", "2026-05")

    assert result == f"s3://{S3_BUCKET}/{completed}"


def test_Spark잡_기본입력탐색은_실행지역을_사용한다(monkeypatch):
    monthly_calls = []
    fuel_calls = []

    def latest_monthly(base_path, year_month, service_area):
        monthly_calls.append((base_path, year_month, service_area))
        return f"{base_path}/service_area={service_area}/year_month={year_month}/data"

    class StopAfterPathResolution(Exception):
        pass

    def latest_fuel(base_path, service_area):
        fuel_calls.append((base_path, service_area))
        raise StopAfterPathResolution

    class FakeRead:
        @staticmethod
        def parquet(_path):
            return object()

    fake_spark = type("FakeSpark", (), {"read": FakeRead()})()
    monkeypatch.setattr(job, "latest_partition_file", latest_monthly)
    monkeypatch.setattr(job, "latest_fuel_price_path", latest_fuel)
    monkeypatch.setattr(job, "get_or_create_spark_session", lambda _name: fake_spark)

    with pytest.raises(StopAfterPathResolution):
        job.main(
            [
                "--env", "prod",
                "--bucket", "de-theone",
                "--service_area", "TX",
                "--year", "2026",
                "--month", "1",
                "--threshold_profit_increase", "600",
            ]
        )

    assert len(monthly_calls) == 3
    assert all(call[2] == "TX" for call in monthly_calls)
    assert fuel_calls == [("s3://de-theone/silver/gas_ev_price", "TX")]
