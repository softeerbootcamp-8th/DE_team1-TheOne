"""기사 차량 월별 스냅샷 Bronze→Silver 정제·적재 시나리오.

1. Extract → 정제 → 원자적 Load 로 수집 버전 파일 하나 생성
2. 같은 수집 시각 재실행은 같은 임시 파일만 덮어씀
3. 새 수집 시각은 별도 파일로 보존
4. driver_id 중복·리스료 품질 위반은 적재 전에 실패
5. 교체 중 실패해도 기존 월 파일이 남음
6. storage=s3 로 실행하면 같은 수집 버전 key 로 S3 silver 에 적재
7. S3 bronze 파티션에 파일이 여러 개면 최신 것만 읽음
8. 같은 수집 시각을 S3로 재실행해도 오브젝트가 늘지 않음
9. 로컬·S3 모두 service_area 지역 Bronze만 읽음
"""

from datetime import date, datetime
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from schema.silver import CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA
from functions.driver_vehicle_monthly_snapshot_bronze_to_silver.handler import lambda_handler
from functions.driver_vehicle_monthly_snapshot_bronze_to_silver.loader import (
    DATASET,
    DriverVehicleMonthlySnapshotSilverLoader,
)


YEAR_MONTH = "2026-08"
S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"
FILE_NAME = "20260821T123456123456Z.parquet"
SOURCE_TOKEN = Path(FILE_NAME).stem
VERSION_DIR = f"source_collected_at={SOURCE_TOKEN}"
SERVICE_AREA = "NYC"


def _rows():
    return [
        {
            "snapshot_month": YEAR_MONTH,
            "driver_id": "driver-1",
            "taxi_id": "taxi-1",
            "vehicle_model_id": "model-1",
            "manufacturer": "KIA",
            "model_name": "SPORTAGE",
            "fuel_type": "GAS",
            "comfort_eligible": True,
            "extra_comfort_eligible": False,
            "weekly_lease_fee": 350.0,
            "join_date": date(2024, 1, 1),
            "exit_date": None,
            "experience_years": 5,
            "vehicle_since": date(2025, 1, 1),
            "snapshot_created_at": datetime(2026, 8, 1),
        }
    ]


def _bronze(
    tmp_path: Path, rows: list[dict], service_area: str = SERVICE_AREA
) -> Path:
    dataset_root = tmp_path / "bronze" / DATASET
    partition = (
        dataset_root
        / f"service_area={service_area}"
        / f"year_month={YEAR_MONTH}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / "20260801T000000000000Z.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    (partition / "_SUCCESS").touch()
    return path


def _event(tmp_path: Path, service_area: str = SERVICE_AREA) -> dict:
    silver_root = tmp_path / "silver" / f"service_area={service_area}"
    return {
        "bronze_dir": str(tmp_path / "bronze"),
        "year_month": YEAR_MONTH,
        "silver_output_path": str(
            silver_root
            / f"year_month={YEAR_MONTH}"
            / VERSION_DIR
        ),
        "service_area": service_area,
    }


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        yield client


def _put_bronze(
    s3_client,
    rows: list[dict],
    timestamp: str,
    year_month: str = YEAR_MONTH,
    *,
    directory_layout: bool = False,
    service_area: str = SERVICE_AREA,
) -> None:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows), sink)
    area = f"service_area={service_area}/"
    prefix = f"bronze/{DATASET}/{area}year_month={year_month}/"
    key = (
        f"{prefix}collected_at={timestamp}/data.parquet"
        if directory_layout
        else f"{prefix}{timestamp}.parquet"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=sink.getvalue().to_pybytes(),
    )
    marker_parent = key.rsplit("/", 1)[0]
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=f"{marker_parent}/_SUCCESS",
        Body=b"",
    )


def _s3_event(
    year_month: str = YEAR_MONTH, service_area: str = SERVICE_AREA
) -> dict:
    area = f"service_area={service_area}/"
    event = {
        "storage": "s3",
        "bucket": S3_BUCKET,
        "year_month": year_month,
        "silver_output_path": (
            f"s3://{S3_BUCKET}/silver/{DATASET}/{area}year_month={year_month}/"
            f"{VERSION_DIR}"
        ),
        "service_area": service_area,
    }
    return event


def _silver_key(
    year_month: str = YEAR_MONTH, service_area: str = SERVICE_AREA
) -> str:
    area = f"service_area={service_area}/"
    return (
        f"silver/{DATASET}/{area}year_month={year_month}/"
        f"{VERSION_DIR}/data.parquet"
    )


def test_정제한_기사차량스냅샷을_최종_버전디렉터리에_적재한다(tmp_path):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "

    _bronze(tmp_path, rows)
    result = lambda_handler(_event(tmp_path))

    path = Path(result["locations"][0])
    assert path == (
        tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}"
        / VERSION_DIR / "data.parquet"
    )
    assert result["row_count"] == 1
    assert pq.read_schema(path) == SCHEMA
    written = pq.ParquetFile(path).read().to_pylist()[0]
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")
    assert written["exit_date"] is None
    assert written["extra_comfort_eligible"] is False
    assert written["vehicle_since"] == date(2025, 1, 1)


def test_service_area로_로컬_지역_Bronze를_읽는다(tmp_path):
    legacy = _rows()
    legacy[0]["driver_id"] = "driver-legacy"
    _bronze(tmp_path, legacy)
    scoped = _rows()
    scoped[0]["driver_id"] = "driver-tx"
    _bronze(tmp_path, scoped, service_area="TX")

    result = lambda_handler(_event(tmp_path, service_area="TX"))

    written = pq.ParquetFile(Path(result["locations"][0])).read().to_pylist()
    assert written[0]["driver_id"] == "driver-tx"
    assert "service_area=TX/year_month=2026-08" in result["locations"][0]


def test_같은수집시각을_다시_정제해도_파일이_늘지않는다(tmp_path):
    _bronze(tmp_path, _rows())

    first = lambda_handler(_event(tmp_path))
    second = lambda_handler(_event(tmp_path))

    assert first == second
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1


def test_새수집시각은_별도_파일로_적재한다(tmp_path):
    _bronze(tmp_path, _rows())
    first_event = _event(tmp_path)
    second_event = {
        **first_event,
        "silver_output_path": str(
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}"
            / "source_collected_at=20260822T123456123456Z"
        ),
    }

    first = lambda_handler(first_event)
    second = lambda_handler(second_event)

    assert first["locations"] != second["locations"]
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 2


def test_driver_id가_중복되면_적재하지_않는다(tmp_path):
    rows = _rows()
    rows.append({**rows[0], "taxi_id": "taxi-2"})

    with pytest.raises(ValueError, match="driver_id가 중복됩니다"):
        _bronze(tmp_path, rows)
        lambda_handler(_event(tmp_path))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("missing_column", "필수 컬럼 누락"),
        ("empty_required", "필수값"),
        ("zero_price", "0 이하"),
    ],
)
def test_스냅샷_품질이_깨지면_적재하지_않는다(tmp_path, broken, message):
    rows = _rows()
    if broken == "missing_column":
        rows = [{k: v for k, v in rows[0].items() if k != "driver_id"}]
    elif broken == "empty_required":
        rows[0]["driver_id"] = "   "
    else:
        rows[0]["weekly_lease_fee"] = 0.0

    with pytest.raises(ValueError, match=message):
        _bronze(tmp_path, rows)
        lambda_handler(_event(tmp_path))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


def test_교체중_실패해도_기존월파일과_임시파일이_남지않는다(tmp_path, monkeypatch):
    _bronze(tmp_path, _rows())
    first = lambda_handler(_event(tmp_path))
    target = Path(first["locations"][0])
    before = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("교체 실패")

    monkeypatch.setattr(type(target), "replace", fail_replace)
    with pytest.raises(OSError, match="교체 실패"):
        lambda_handler(_event(tmp_path))

    assert target.read_bytes() == before
    assert not list(target.parent.glob("*.tmp"))


def test_Silver스키마가_아닌_테이블은_적재하지_않는다(tmp_path):
    loader = DriverVehicleMonthlySnapshotSilverLoader(
        str(
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}"
            / VERSION_DIR
        )
    )

    with pytest.raises(ValueError, match="Silver 스키마와 다릅니다"):
        loader.write(pa.Table.from_pylist([{"driver_id": "driver-1"}]))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    ("event", "message"),
    [
        ({}, "year_month"),
        ({"year_month": "2026-8"}, "year_month"),
        ({"year_month": YEAR_MONTH}, "silver_output_path"),
    ],
)
def test_필수식별자가_없거나_형식이_잘못되면_읽기전에_실패한다(event, message):
    with pytest.raises(ValueError, match=message):
        lambda_handler(event)


def test_bronze_파티션이_없으면_실패한다(tmp_path):
    event = {
        "bronze_dir": str(tmp_path / "bronze"),
        "year_month": YEAR_MONTH,
        "silver_output_path": str(
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}"
            / VERSION_DIR
        ),
        "service_area": SERVICE_AREA,
    }
    with pytest.raises(FileNotFoundError, match="파티션이 없습니다"):
        lambda_handler(event)


def test_S3_storage로_실행하면_S3에서_읽어_S3로_적재한다(s3_client):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "
    _put_bronze(s3_client, rows, "20260801T000000000000Z")

    result = lambda_handler(_s3_event())

    key = _silver_key()
    assert result["locations"] == [f"s3://{S3_BUCKET}/{key}"]
    assert result["row_count"] == 1
    body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    written = pq.ParquetFile(pa.BufferReader(body)).read().to_pylist()[0]
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")


def test_S3_bronze가_여러개면_최신_타임스탬프를_읽는다(s3_client):
    older = _rows()
    older[0]["driver_id"] = "driver-old"
    _put_bronze(s3_client, older, "20260801T000000000000Z")
    newer = _rows()
    newer[0]["driver_id"] = "driver-new"
    _put_bronze(
        s3_client, newer, "20260815T000000000000Z", directory_layout=True
    )

    lambda_handler(_s3_event())

    body = s3_client.get_object(Bucket=S3_BUCKET, Key=_silver_key())["Body"].read()
    written = pq.ParquetFile(pa.BufferReader(body)).read().to_pylist()
    assert written[0]["driver_id"] == "driver-new"


def test_service_area로_S3_지역_Bronze를_읽는다(s3_client):
    legacy = _rows()
    legacy[0]["driver_id"] = "driver-legacy"
    _put_bronze(s3_client, legacy, "20260801T000000000000Z")
    scoped = _rows()
    scoped[0]["driver_id"] = "driver-tx"
    _put_bronze(
        s3_client,
        scoped,
        "20260801T000000000000Z",
        service_area="TX",
    )

    result = lambda_handler(_s3_event(service_area="TX"))

    key = _silver_key(service_area="TX")
    assert result["locations"] == [f"s3://{S3_BUCKET}/{key}"]
    body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    written = pq.ParquetFile(pa.BufferReader(body)).read().to_pylist()
    assert written[0]["driver_id"] == "driver-tx"


def test_S3에_bronze_파티션이_없으면_실패한다(s3_client):
    with pytest.raises(FileNotFoundError, match="파티션이 없습니다"):
        lambda_handler(_s3_event())


def test_같은월을_S3로_다시_실행해도_오브젝트가_늘지않는다(s3_client):
    _put_bronze(s3_client, _rows(), "20260801T000000000000Z")

    first = lambda_handler(_s3_event())
    second = lambda_handler(_s3_event())

    assert first == second
    prefix = f"silver/{DATASET}/service_area=NYC/year_month={YEAR_MONTH}/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    assert response["KeyCount"] == 1
