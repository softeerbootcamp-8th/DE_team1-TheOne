"""보유 차량 Bronze→Silver 정제·적재 시나리오.

1. Extract → 정제 → 원자적 Load 로 월 파티션 파일 하나 생성
2. 같은 월 재실행은 파일을 늘리지 않고 덮어씀
3. 재고 품질 위반은 적재 전에 실패
4. 교체 중 실패해도 기존 월 파일이 남음
5. storage=s3 로 실행하면 S3 bronze 를 읽어 정해진 key 로 S3 silver 에 적재
6. S3 bronze 파티션에 타임스탬프가 다른 파일이 여러 개면 최신 것만 읽음
7. S3 에 bronze 파티션이 없으면 FileNotFoundError
8. 같은 월을 S3 storage 로 재실행해도 실버 오브젝트가 늘지 않음
"""

from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from schema.silver import CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SCHEMA
from functions.lease_vehicle_inventory_bronze_to_silver.handler import lambda_handler
from functions.lease_vehicle_inventory_bronze_to_silver.loader import (
    DATASET,
    LeaseVehicleInventorySilverLoader,
)


YEAR_MONTH = "2026-08"
S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


def _rows():
    return [
        {
            "vehicle_model_id": "model-1",
            "manufacturer": "KIA",
            "model_name": "SPORTAGE",
            "model_year": 2023,
            "fuel_type": "GAS",
            "fuel_efficiency": 28.5,
            "comfort_eligible": True,
            "extra_comfort_eligible": False,
            "weekly_lease_fee": 350.0,
            "image_url": "http://images.example/kia-sportage.png",
            "stock": 12,
        }
    ]


def _bronze(tmp_path: Path, rows: list[dict]) -> Path:
    partition = tmp_path / "bronze" / DATASET / f"year_month={YEAR_MONTH}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / "20260801T000000000000Z.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def _event(tmp_path: Path, bronze: Path) -> dict:
    return {
        "bronze_dir": str(tmp_path / "bronze"),
        "year_month": YEAR_MONTH,
        "silver_dir": str(tmp_path / "silver"),
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


def _put_bronze(s3_client, rows: list[dict], timestamp: str, year_month: str = YEAR_MONTH) -> None:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows), sink)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=f"bronze/{DATASET}/year_month={year_month}/{timestamp}.parquet",
        Body=sink.getvalue().to_pybytes(),
    )


def _s3_event(year_month: str = YEAR_MONTH) -> dict:
    return {"storage": "s3", "bucket": S3_BUCKET, "year_month": year_month}


def _silver_key(year_month: str = YEAR_MONTH) -> str:
    return f"silver/{DATASET}/year_month={year_month}/{DATASET}.parquet"


def test_정제한_보유차량을_월파티션_한파일로_적재한다(tmp_path):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "

    result = lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    path = Path(result["locations"][0])
    assert path == (
        tmp_path / "silver" / f"year_month={YEAR_MONTH}" / "lease_vehicle_inventory.parquet"
    )
    assert result["row_count"] == 1
    assert pq.read_schema(path) == SCHEMA
    written = pq.ParquetFile(path).read().to_pylist()[0]
    # 리스 계약의 make_key·model_key 와 붙일 조인 키라 대문자로 맞춥니다.
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")


def test_같은월을_다시_정제해도_파일이_늘지않는다(tmp_path):
    bronze = _bronze(tmp_path, _rows())

    first = lambda_handler(_event(tmp_path, bronze))
    second = lambda_handler(_event(tmp_path, bronze))

    assert first == second
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("duplicate_model_id", "중복"),
        ("zero_stock", "0 이하"),
        ("zero_price", "0 이하"),
        ("zero_efficiency", "0 이하"),
        ("empty_image_url", "필수값"),
        ("missing_column", "필수 컬럼 누락"),
    ],
)
def test_재고품질이_깨지면_적재하지_않는다(tmp_path, broken, message):
    rows = _rows()
    if broken == "duplicate_model_id":
        rows.append({**rows[0], "model_year": 2024})
    elif broken == "zero_stock":
        rows[0]["stock"] = 0
    elif broken == "zero_price":
        rows[0]["weekly_lease_fee"] = 0.0
    elif broken == "zero_efficiency":
        rows[0]["fuel_efficiency"] = 0.0
    elif broken == "empty_image_url":
        rows[0]["image_url"] = "   "
    else:
        rows = [{k: v for k, v in rows[0].items() if k != "stock"}]

    with pytest.raises(ValueError, match=message):
        lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


def test_교체중_실패해도_기존월파일과_임시파일이_남지않는다(tmp_path, monkeypatch):
    bronze = _bronze(tmp_path, _rows())
    first = lambda_handler(_event(tmp_path, bronze))
    target = Path(first["locations"][0])
    before = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("교체 실패")

    monkeypatch.setattr(type(target), "replace", fail_replace)
    with pytest.raises(OSError, match="교체 실패"):
        lambda_handler(_event(tmp_path, bronze))

    assert target.read_bytes() == before
    assert not list(target.parent.glob("*.tmp"))


def test_Silver스키마가_아닌_테이블은_적재하지_않는다(tmp_path):
    loader = LeaseVehicleInventorySilverLoader(str(tmp_path / "silver"), YEAR_MONTH)

    with pytest.raises(ValueError, match="Silver 스키마와 다릅니다"):
        loader.write(pa.Table.from_pylist([{"vehicle_model_id": "model-1"}]))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    "event",
    [{}, {"year_month": "2026-8"}],
)
def test_year_month이_YYYYMM_형식이_아니면_읽기전에_실패한다(event):
    with pytest.raises(ValueError, match="year_month"):
        lambda_handler(event)


def test_bronze_파티션이_없으면_실패한다(tmp_path):
    event = {
        "bronze_dir": str(tmp_path / "bronze"),
        "year_month": YEAR_MONTH,
        "silver_dir": str(tmp_path / "silver"),
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
    older[0]["vehicle_model_id"] = "model-old"
    _put_bronze(s3_client, older, "20260801T000000000000Z")
    newer = _rows()
    newer[0]["vehicle_model_id"] = "model-new"
    _put_bronze(s3_client, newer, "20260815T000000000000Z")

    lambda_handler(_s3_event())

    body = s3_client.get_object(Bucket=S3_BUCKET, Key=_silver_key())["Body"].read()
    written = pq.ParquetFile(pa.BufferReader(body)).read().to_pylist()
    assert written[0]["vehicle_model_id"] == "model-new"


def test_S3에_bronze_파티션이_없으면_실패한다(s3_client):
    with pytest.raises(FileNotFoundError, match="파티션이 없습니다"):
        lambda_handler(_s3_event())


def test_같은월을_S3로_다시_실행해도_오브젝트가_늘지않는다(s3_client):
    _put_bronze(s3_client, _rows(), "20260801T000000000000Z")

    first = lambda_handler(_s3_event())
    second = lambda_handler(_s3_event())

    assert first == second
    prefix = f"silver/{DATASET}/year_month={YEAR_MONTH}/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    assert response["KeyCount"] == 1
