"""D5 재고 — 모델별 보유 대수와 개별 차량(taxi) 단위 확장.

`sub/prototype/synthesize.py::build_fleet_stock`/`expand_fleet_units` 를 그대로
옮겼습니다.
"""

from __future__ import annotations

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


def load_fuel_prices(*, data_dir: str | Path | None = None) -> dict:
    """실측 유가·전기요금. 합성이 아닙니다 (D5 — 배정 비용의 입력)."""
    data = Path(data_dir) if data_dir else _PROJECT_ROOT / "data"
    gas = _read_parquet_dir(_latest_partition(data / "silver" / "gas_price", "collected_month"))
    ev = _read_parquet_dir(
        _latest_partition(data / "silver" / "ev_charging_price", "collected_month")
    )
    return {
        "gallon_usd": float(gas["price_usd_per_gallon"].mean()),
        "kwh_usd": float(ev["average_price_usd_per_kwh"].mean()),
    }


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
