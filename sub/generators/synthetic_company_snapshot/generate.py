"""가상 회사 고객·택시·리스 원천 스냅샷 생성 CLI.

실행: ``cd main/spark && PYTHONPATH=../.. uv run --frozen python -m sub.generators.synthetic_company_snapshot.generate``
"""

import argparse
import logging
import os
from datetime import date
from pathlib import Path

from shared.common.s3_reader import read_parquet_uri
from sub.config import DEFAULT_CONFIG_PATH, load_config
from sub.generators.synthetic_company_snapshot.snapshot import (
    LEASE_START_MIN,
    MODEL_YEAR,
    build_company_snapshot,
    build_driver_ids,
    build_vehicle_pool,
    evolve_company_snapshot,
    read_snapshot,
    write_snapshot,
    write_snapshot_s3,
)

# 실행 위치가 아니라 이 파일 위치로 저장소 루트를 확정합니다. Makefile은
# main/spark에서 실행하므로 상대경로를 쓰면 main/data를 보게 됩니다.
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# vehicle_master 는 네 개 Curated 를 조인해 만드는 파생 Curated 라 Raw 가 없습니다
# (lambda/functions/common/vehicle_master_layout.py 참고). 경로에 생성일이 들어가서
# 고정값으로 두면 다음 수집일에 낡으므로, 데이터셋 디렉터리에서 최신을 골라 씁니다.
_VEHICLE_MASTER_DIR = PROJECT_ROOT / "data" / "source" / "curated" / "vehicle_master"
_VEHICLE_MASTER_FILE = "vehicle_master.parquet"


def resolve_vehicle_master_path(
    dataset_dir: str | Path,
    *,
    storage: str = "local",
    bucket: str | None = None,
) -> str:
    """가장 최신 `collected_date=` 파티션의 vehicle_master 경로. `storage="s3"` 면 `s3://` URI.

    ISO 날짜라 이름 정렬이 곧 시간 정렬입니다. 도시가 여러 개면 어느 쪽을 쓸지
    정할 근거가 없으므로 조용히 고르지 않고 실패시킵니다.

    예전에는 `storage="s3"` 일 때 파일을 로컬로 내려받아 그 경로를 돌려줬습니다.
    소비하는 쪽이 `s3://` 를 못 읽는다는 이유였는데, 둘 다 해소됐습니다 — Spark 는
    EMR 의 EMRFS 가 `s3://` 를 그대로 읽고(#712), pandas 는 `read_parquet_uri` 가
    boto3 로 받습니다(`s3fs` 는 `aiobotocore` 를 끌고 와 `boto3` 핀과 충돌해 여전히
    쓰지 않습니다). 그리고 내려받으면 EMR 워커가 그 디스크를 못 봐서 아예 실패합니다(#782).

    반환형이 `Path` 가 아니라 `str` 인 이유 — `Path("s3://b/x")` 는 `s3:/b/x` 로
    뭉개져 스킴이 깨집니다.
    """
    if storage == "s3":
        return _latest_s3_uri(bucket)
    if storage != "local":
        raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")

    partitions = sorted(Path(dataset_dir).glob("collected_date=*"))
    if not partitions:
        raise FileNotFoundError(
            f"vehicle_master Curated 가 없습니다: {dataset_dir}. "
            "vehicle_master_curated_to_curated DAG 를 먼저 돌리거나, S3 에 있으면 "
            "storage=s3 로 실행하세요 (--vehicle_master_path 로 직접 지정해도 됩니다)."
        )

    latest = partitions[-1]
    files = sorted(latest.glob(f"city=*/{_VEHICLE_MASTER_FILE}"))
    if not files:
        raise FileNotFoundError(f"도시 파티션이 비어 있습니다: {latest}")
    if len(files) > 1:
        raise ValueError(
            f"도시가 여러 개라 하나를 고를 수 없습니다. --vehicle_master_path 로 직접 지정하세요: "
            f"{[str(f) for f in files]}"
        )
    return str(files[0])


def _resolve_bucket(explicit: str | None, from_env: str | None, env_var: str) -> str:
    """버킷 이름을 정하고 **형식까지** 확인합니다.

    형식을 여기서 보는 이유
    ---------------------
    이름이 틀리면 boto3 가 `ClientError: InvalidBucketName` 을 던지는데, 그 메시지에는
    무엇이 들어왔는지도, 어디서 온 값인지도 안 나옵니다. 실제로 그 에러를 만나고
    원인을 찾는 데 시간이 걸렸습니다.

    S3 이름 규칙 전체를 다시 구현하지는 않습니다 — 흔히 하는 실수 세 개(`s3://` 를
    붙임, 경로를 함께 적음, 공백)만 잡고 나머지는 AWS 에 맡깁니다.
    """
    source = "--bucket" if explicit else env_var
    bucket = (explicit or from_env or "").strip()
    if not bucket:
        raise ValueError(
            f"storage=s3 인데 버킷이 없습니다. --bucket 으로 넘기거나 {env_var} 를 설정하세요."
        )
    if bucket.startswith("s3://"):
        raise ValueError(
            f"{source} 에 스킴이 붙어 있습니다: {bucket!r}. "
            f"버킷 이름만 넣으세요 (예: {bucket.removeprefix('s3://').split('/')[0]!r})."
        )
    if "/" in bucket:
        raise ValueError(
            f"{source} 에 경로가 섞여 있습니다: {bucket!r}. "
            f"버킷 이름만 넣으세요 (예: {bucket.split('/')[0]!r})."
        )
    return bucket


def _latest_s3_uri(bucket: str | None) -> str:
    """S3 의 최신 vehicle_master `s3://` URI. 내려받지 않습니다.

    내려받으면 그 파일이 이 프로세스의 로컬 디스크에만 생겨서, EMR Serverless 워커가
    보지 못합니다(#782). URI 를 넘기면 Spark 는 EMRFS, pandas 는 `read_parquet_uri`
    가 각자 읽습니다.
    """
    from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR
    from shared.common.env import load_local_env
    from shared.common.s3_reader import list_keys
    from sub.aws_lambda.common import vehicle_master_layout as layout

    load_local_env()
    bucket = _resolve_bucket(bucket, os.environ.get(BUCKET_ENV_VAR), BUCKET_ENV_VAR)

    prefix = f"source/curated/{layout.DATASET}/"
    location = f"s3://{bucket}/{prefix}"
    keys = list_keys(bucket, prefix)
    if not keys:
        raise FileNotFoundError(
            f"vehicle_master Curated 가 없습니다: {location}. "
            "vehicle_master_curated_to_curated DAG 를 먼저 돌리세요."
        )

    # 로컬과 같은 규칙으로 고릅니다 — 미래 파티션은 건너뛰어야 과거 날짜 재실행이 재현됩니다.
    collected_date = layout.latest_date_from_keys(keys, date.today(), location)
    partition_prefix = f"{prefix}{layout.DATE_PARTITION_KEY}={collected_date.isoformat()}/"
    matched = sorted(
        key for key in keys
        if key.startswith(partition_prefix) and key.endswith(_VEHICLE_MASTER_FILE)
    )
    if not matched:
        raise FileNotFoundError(f"도시 파티션이 비어 있습니다: s3://{bucket}/{partition_prefix}")
    if len(matched) > 1:
        raise ValueError(
            "도시가 여러 개라 하나를 고를 수 없습니다. --vehicle_master_path 로 직접 지정하세요: "
            f"{[f's3://{bucket}/{key}' for key in matched]}"
        )

    uri = f"s3://{bucket}/{matched[0]}"
    logger.info("vehicle_master 입력: %s", uri)
    return uri


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="합성 회사 원천 DB 스냅샷 생성")
    parser.add_argument(
        "--previous_snapshot_dir",
        default=None,
        help="지정하면 초기 생성 대신 이 파티션을 기준으로 월별 스냅샷 갱신",
    )
    parser.add_argument(
        "--vehicle_master_path",
        default=None,
        help=f"비우면 {_VEHICLE_MASTER_DIR} 의 최신 collected_date 파티션을 씁니다",
    )
    parser.add_argument("--output_dir", default="../data/source/company")
    parser.add_argument(
        "--storage", choices=("local", "s3"), default="local", help="local이면 --output_dir, s3면 DATA_LAKE_S3_BUCKET"
    )
    parser.add_argument("--bucket", default=None, help="비우면 DATA_LAKE_S3_BUCKET 환경변수")
    parser.add_argument(
        "--config", default=None, help=f"비우면 {DEFAULT_CONFIG_PATH}"
    )
    # 아래 값들은 기본값을 갖지 않습니다. 비우면 config 또는 `snapshot.py` 의 이름
    # 붙은 상수를 읽습니다 — 소유자를 한 곳으로 두려면 여기에 값이 있으면 안 됩니다.
    parser.add_argument(
        "--snapshot_date", default=None, help="비우면 config 의 bootstrap.snapshot_date"
    )
    parser.add_argument(
        "--lease_start_min",
        default=None,
        help=f"비우면 snapshot.py 의 LEASE_START_MIN ({LEASE_START_MIN.isoformat()})",
    )
    parser.add_argument("--seed", type=int, default=None, help="비우면 config 의 global_seed")
    parser.add_argument(
        "--model_year", type=int, default=None, help=f"비우면 snapshot.py 의 MODEL_YEAR ({MODEL_YEAR})"
    )
    parser.add_argument(
        "--change_rate",
        type=float,
        default=None,
        help="비우면 MIN~MAX_MONTHLY_CHANGE_RATE 범위에서 무작위 추첨",
    )
    args = parser.parse_args(args_list)

    config = load_config(args.config)
    seed = args.seed if args.seed is not None else config.global_seed
    model_year = args.model_year if args.model_year is not None else MODEL_YEAR
    snapshot_date = (
        date.fromisoformat(args.snapshot_date)
        if args.snapshot_date
        else config.bootstrap.snapshot_date
    )
    lease_start_min = (
        date.fromisoformat(args.lease_start_min) if args.lease_start_min else LEASE_START_MIN
    )
    # `--storage` 는 출력 위치를 정하는 값인데 입력 조회에도 씁니다. 같은 환경에서
    # 읽고 쓰는 것이 정상이고, 굳이 갈라야 하면 `--vehicle_master_path` 로 직접 지정합니다.
    vehicle_master_path = args.vehicle_master_path or resolve_vehicle_master_path(
        _VEHICLE_MASTER_DIR, storage=args.storage, bucket=args.bucket
    )
    vehicle_pool = build_vehicle_pool(
        # `pd.read_parquet` 를 직접 쓰지 않습니다 — `s3://` 를 받으면 `s3fs` 를
        # 요구하고, 그것이 `aiobotocore` 를 끌고 와 `boto3` 핀과 충돌합니다.
        read_parquet_uri(str(vehicle_master_path)),
        model_year=model_year,
    )
    if args.previous_snapshot_dir:
        tables = evolve_company_snapshot(
            read_snapshot(args.previous_snapshot_dir),
            vehicle_pool,
            seed=seed,
            snapshot_date=snapshot_date,
            change_rate=args.change_rate,
        )
    else:
        tables = build_company_snapshot(
            build_driver_ids(config.driver.initial_count),
            vehicle_pool,
            seed=seed,
            snapshot_date=snapshot_date,
            lease_start_min=lease_start_min,
        )
    if args.storage == "s3":
        paths = write_snapshot_s3(tables, snapshot_date, bucket=args.bucket)
    else:
        paths = write_snapshot(tables, args.output_dir, snapshot_date)
    print(f"합성 회사 원천 스냅샷 생성 완료: {', '.join(map(str, paths))}")
    return paths


if __name__ == "__main__":
    main()
