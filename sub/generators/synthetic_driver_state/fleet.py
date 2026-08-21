"""D5 재고 — 모델별 보유 대수와 개별 차량(taxi) 단위 확장.

`sub/prototype/synthesize.py::build_fleet_stock`/`expand_fleet_units` 를 그대로
옮겼습니다.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

# 실행 위치가 아니라 이 파일 위치로 저장소 루트를 확정합니다 (sub/config.py 와
# 같은 규칙). `sub.prototype.paths` 를 쓰지 않는 이유는 운영 모듈이 prototype
# 패키지를 import 하지 않기 위해서입니다 (asistobe.md 8.2).
_PROJECT_ROOT = Path(__file__).resolve().parents[3]

# 리스팅은 대수를 주지 않아서 가정입니다 — 싼 차가 많다는 쪽으로 가중치를
# 줍니다. 재고 제약이 실제로 걸려야 D5 의 "eligible pool → inventory constraint"
# 가 장식이 아닙니다.
# ponytail: 총 재고 = 정원의 1.15배. 리스팅에 대수 컬럼이 생기면 그것을 쓰세요.
FLEET_OVERSUPPLY = 1.15


def _latest_partition(root: Path, prefix: str) -> Path:
    candidates = sorted(p for p in root.glob(f"{prefix}=*") if p.is_dir())
    if not candidates:
        raise FileNotFoundError(f"{prefix} 파티션이 없습니다: {root}")
    return candidates[-1]


def _read_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Parquet 이 없습니다: {path}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


# 연료비 Silver 의 위치·이름. 생산자는 `eia_fuel_price_silver_pipeline` 하나입니다.
# 전에는 크롤링이 만든 `gas_price`·`ev_charging_price` 를 각각 읽었는데, 크롤링 경로를
# 걷어내며(#462) 생산자가 사라져 이 함수만 없는 데이터셋을 찾고 있었습니다.
FUEL_PRICE_DATASET = "gas_ev_price"
FUEL_PRICE_PARTITION_KEY = "year_month"
FUEL_PRICE_FILE_NAME = "gas_ev_price.parquet"


def load_fuel_prices(
    *,
    data_dir: str | Path | None = None,
    storage: str = "local",
    bucket: str | None = None,
) -> dict:
    """실측 유가·전기요금. 합성이 아닙니다 (D5 — 배정 비용의 입력).

    `storage="s3"` 이면 S3 에서 최신 `year_month=` 을 읽습니다. EC2 컨테이너는
    바인드 마운트가 없어 로컬 `data/` 가 비어 있으므로, 로컬만 보면 S3 에 산출물이
    있어도 못 찾습니다 (#720 과 같은 원인).
    """
    if storage == "s3":
        frame = _read_latest_from_s3(bucket)
    elif storage == "local":
        data = Path(data_dir) if data_dir else _PROJECT_ROOT / "data"
        root = data / "silver" / FUEL_PRICE_DATASET
        frame = _read_parquet_dir(_latest_partition(root, FUEL_PRICE_PARTITION_KEY))
    else:
        raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")

    return _fuel_prices_from_frame(frame)


def _fuel_prices_from_frame(frame: pd.DataFrame) -> dict:
    """일별 행을 배정 비용이 쓰는 단가 두 개로 줄입니다.

    한 달치 일별 행이라 평균을 씁니다. 컬럼 이름을 여기서 한 번만 다루는 이유는
    스키마(`schema/silver` 의 `CLEAN_FUEL_PRICE_SCHEMA`)가 바뀔 때 고칠 자리를
    하나로 두기 위해서입니다.
    """
    missing = {"gas_price", "ev_price"} - set(frame.columns)
    if missing:
        raise ValueError(
            f"연료비 Silver 에 컬럼이 없습니다: {sorted(missing)} "
            f"(있는 것: {sorted(frame.columns)})"
        )
    return {
        "gallon_usd": float(frame["gas_price"].mean()),
        "kwh_usd": float(frame["ev_price"].mean()),
    }


def _read_latest_from_s3(bucket: str | None) -> pd.DataFrame:
    """S3 의 최신 `year_month=` 파티션을 DataFrame 으로 읽습니다.

    `s3://` 를 pandas 에 그대로 넘기지 않는 이유는 `s3fs` 가 필요하고, 그것이
    `aiobotocore` 를 끌고 와 airflow 의 `boto3` 핀과 충돌하기 때문입니다. bytes 로
    받아 메모리에서 읽습니다 — 한 달치 일별 행이라 작습니다.
    """
    import io

    from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR
    from shared.common.env import load_local_env
    from shared.common.s3_reader import get_object_bytes, list_keys

    load_local_env()
    bucket = (bucket or os.environ.get(BUCKET_ENV_VAR) or "").strip()
    if not bucket:
        raise ValueError(
            f"storage=s3 인데 버킷이 없습니다. bucket 을 넘기거나 {BUCKET_ENV_VAR} 를 설정하세요."
        )

    prefix = f"silver/{FUEL_PRICE_DATASET}/"
    keys = [k for k in list_keys(bucket, prefix) if k.endswith(FUEL_PRICE_FILE_NAME)]
    if not keys:
        raise FileNotFoundError(
            f"연료비 Silver 가 없습니다: s3://{bucket}/{prefix} — "
            "eia_fuel_price_silver DAG 를 먼저 돌리세요."
        )
    # 키에 `year_month=YYYY-MM` 이 들어가고 ISO 라 문자열 정렬이 곧 시간 정렬입니다.
    return pd.read_parquet(io.BytesIO(get_object_bytes(bucket, max(keys))))


def build_fleet_stock(vehicle_master: pd.DataFrame, *, driver_count: int) -> pd.DataFrame:
    """모델별 보유 대수. 싼 차에 가중치를 줍니다."""
    weight = 1.0 / vehicle_master["weekly_price_usd"].to_numpy()
    share = weight / weight.sum()
    total = int(round(driver_count * FLEET_OVERSUPPLY))
    units = np.maximum(1, np.floor(share * total).astype(int))
    stock = vehicle_master.copy()
    stock["unit_count"] = units
    return stock


def expand_fleet_units(stock: pd.DataFrame) -> pd.DataFrame:
    """모델별 대수를 개별 차량(taxi) 한 대씩으로 펼칩니다.

    차량 개체가 있어야 제약 5(한 차량을 두 기사가 동시에 몰 수 없음)를 재고
    수준에서 지킬 수 있습니다. 모델 단위로만 두면 두 기사가 같은 차를 받습니다.
    """
    rows = []
    for record in stock.to_dict("records"):
        for serial in range(int(record["unit_count"])):
            unit = {k: v for k, v in record.items() if k != "unit_count"}
            unit["taxi_id"] = f"{record['vehicle_model_id']}#{serial:05d}"
            rows.append(unit)
    return pd.DataFrame(rows)
