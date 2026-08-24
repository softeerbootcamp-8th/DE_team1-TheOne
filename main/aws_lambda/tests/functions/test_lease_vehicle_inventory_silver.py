"""보유 차량 Bronze→Silver 정제·적재 시나리오.

1. Extract → 정제 → 원자적 Load 로 수집 버전 파일 하나 생성
2. 같은 수집 시각 재실행은 같은 임시 파일만 덮어씀
3. 새 수집 시각은 별도 파일로 보존
4. 재고 품질 위반은 적재 전에 실패
5. 교체 중 실패해도 기존 월 파일이 남음
6. storage=s3 로 실행하면 같은 수집 버전 key 로 S3 silver 에 적재
7. S3 bronze 파티션에 파일이 여러 개면 최신 것만 읽음
8. 같은 수집 시각을 S3로 재실행해도 오브젝트가 늘지 않음
9. 로컬·S3 service_area 경로의 Bronze를 읽어 지역별 Silver에 적재
10. 지역 경로가 없어도 비지역 Bronze 경로로 폴백하지 않음
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
FILE_NAME = "20260821T123456123456Z.parquet"
SOURCE_TOKEN = Path(FILE_NAME).stem
VERSION_DIR = f"source_collected_at={SOURCE_TOKEN}"
SERVICE_AREA = "NYC"


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


def _bronze(
    tmp_path: Path, rows: list[dict], service_area: str = SERVICE_AREA
) -> Path:
    root = tmp_path / "bronze" / DATASET / f"service_area={service_area}"
    partition = root / f"year_month={YEAR_MONTH}"
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / "20260801T000000000000Z.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def _event(
    tmp_path: Path, bronze: Path, service_area: str = SERVICE_AREA
) -> dict:
    root = tmp_path / "silver" / f"service_area={service_area}"
    return {
        "bronze_dir": str(tmp_path / "bronze"),
        "year_month": YEAR_MONTH,
        "silver_output_path": str(
            root / f"year_month={YEAR_MONTH}"
            / ".staging"
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
    root = f"bronze/{DATASET}/service_area={service_area}"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=(
            f"{root}/year_month={year_month}/collected_at={timestamp}/data.parquet"
            if directory_layout
            else f"{root}/year_month={year_month}/{timestamp}.parquet"
        ),
        Body=sink.getvalue().to_pybytes(),
    )


def _s3_event(
    year_month: str = YEAR_MONTH, service_area: str = SERVICE_AREA
) -> dict:
    root = f"silver/{DATASET}/service_area={service_area}"
    return {
        "storage": "s3",
        "bucket": S3_BUCKET,
        "year_month": year_month,
        "silver_output_path": (
            f"s3://{S3_BUCKET}/{root}/year_month={year_month}/"
            f".staging/{VERSION_DIR}"
        ),
        "service_area": service_area,
    }


def _silver_key(
    year_month: str = YEAR_MONTH, service_area: str = SERVICE_AREA
) -> str:
    root = f"silver/{DATASET}/service_area={service_area}"
    return (
        f"{root}/year_month={year_month}/.staging/{VERSION_DIR}/data.parquet"
    )


def test_정제한_보유차량을_검증전_버전디렉터리_part로_적재한다(tmp_path):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "

    result = lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    path = Path(result["locations"][0])
    assert path == (
        tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}" / ".staging"
        / VERSION_DIR / "data.parquet"
    )
    assert result["row_count"] == 1
    assert pq.read_schema(path) == SCHEMA
    written = pq.ParquetFile(path).read().to_pylist()[0]
    # 리스 계약의 make_key·model_key 와 붙일 조인 키라 대문자로 맞춥니다.
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")


def test_TX_로컬_Bronze를_읽어_지역별_Silver에_적재한다(tmp_path):
    bronze = _bronze(tmp_path, _rows(), service_area="TX")

    result = lambda_handler(_event(tmp_path, bronze, service_area="TX"))

    assert "service_area=TX" in result["locations"][0]
    assert Path(result["locations"][0]).is_file()


def test_TX_지역경로가_없으면_로컬_옛_Bronze를_읽지않는다(tmp_path):
    bronze = _bronze(tmp_path, _rows())

    with pytest.raises(FileNotFoundError):
        lambda_handler(_event(tmp_path, bronze, service_area="TX"))


def test_같은수집시각을_다시_정제해도_파일이_늘지않는다(tmp_path):
    bronze = _bronze(tmp_path, _rows())

    first = lambda_handler(_event(tmp_path, bronze))
    second = lambda_handler(_event(tmp_path, bronze))

    assert first == second
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1


def test_새수집시각은_별도_파일로_적재한다(tmp_path):
    bronze = _bronze(tmp_path, _rows())
    first_event = _event(tmp_path, bronze)
    second_event = {
        **first_event,
        "silver_output_path": str(
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}" / ".staging"
            / "source_collected_at=20260822T123456123456Z"
        ),
    }

    first = lambda_handler(first_event)
    second = lambda_handler(second_event)

    assert first["locations"] != second["locations"]
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 2


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
    loader = LeaseVehicleInventorySilverLoader(
        str(
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}" / ".staging"
            / VERSION_DIR
        )
    )

    with pytest.raises(ValueError, match="Silver 스키마와 다릅니다"):
        loader.write(pa.Table.from_pylist([{"vehicle_model_id": "model-1"}]))

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
            tmp_path / "silver" / "service_area=NYC" / f"year_month={YEAR_MONTH}" / ".staging"
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
    _put_bronze(
        s3_client,
        rows,
        "20260801T000000000000Z",
        service_area="TX",
    )

    result = lambda_handler(_s3_event(service_area="TX"))

    key = _silver_key(service_area="TX")
    assert result["locations"] == [f"s3://{S3_BUCKET}/{key}"]
    assert result["row_count"] == 1
    body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    written = pq.ParquetFile(pa.BufferReader(body)).read().to_pylist()[0]
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")


def test_TX_지역경로가_없으면_S3_옛_Bronze를_읽지않는다(s3_client):
    _put_bronze(s3_client, _rows(), "20260801T000000000000Z")

    with pytest.raises(FileNotFoundError):
        lambda_handler(_s3_event(service_area="TX"))


def test_S3_bronze가_여러개면_최신_타임스탬프를_읽는다(s3_client):
    older = _rows()
    older[0]["vehicle_model_id"] = "model-old"
    _put_bronze(s3_client, older, "20260801T000000000000Z")
    newer = _rows()
    newer[0]["vehicle_model_id"] = "model-new"
    _put_bronze(
        s3_client, newer, "20260815T000000000000Z", directory_layout=True
    )

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
    prefix = f"silver/{DATASET}/service_area=NYC/year_month={YEAR_MONTH}/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    assert response["KeyCount"] == 1
