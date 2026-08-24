"""대시보드 Gold 데이터 소스.

`DASHBOARD_DATA_SOURCE` 환경변수(local|rds, 기본 local)로 로컬 CSV와 RDS를 전환한다.
RDS 쪽 SELECT 컬럼은 `schema.gold`의 dataclass 필드에서 그대로 만든다.

Gold RDS는 같은 지역·월에 재실행 이력이 버전으로 쌓이므로(`postgres_loader.py`),
`service_area`, `year_month`별 최신 version 행만 읽는다.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import fields
from pathlib import Path

import pandas as pd
import psycopg2

from schema.gold import DriverMonthlyProfit

_TABLE_MODELS = {
    "driver_aggregation": DriverMonthlyProfit,
}


class DataSource(ABC):
    @abstractmethod
    def load(self, dataset: str) -> pd.DataFrame:
        """Gold 물리 테이블 `dataset`을 읽는다."""


class LocalCsvDataSource(DataSource):
    """`root/{dataset}/service_area=*/year_month=*/{dataset}.csv`를 이어붙인다."""

    def __init__(self, root: Path):
        self._root = root

    def load(self, dataset: str) -> pd.DataFrame:
        paths = sorted(
            self._root.glob(
                f"{dataset}/service_area=*/year_month=*/{dataset}.csv"
            )
        )
        if not paths:
            return pd.DataFrame()
        return pd.concat((pd.read_csv(p) for p in paths), ignore_index=True)


def _latest_version_query(table: str, columns: list[str]) -> str:
    selected = ", ".join(f"t.{name}" for name in columns)
    return (
        f"SELECT {selected} FROM {table} t "
        f"WHERE t.version = (SELECT MAX(version) FROM {table} "
        f"WHERE service_area = t.service_area AND year_month = t.year_month)"
    )


class RdsDataSource(DataSource):
    """Gold RDS에서 지역·월별 최신 version만 읽는다."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def load(self, dataset: str) -> pd.DataFrame:
        try:
            model = _TABLE_MODELS[dataset]
        except KeyError:
            raise ValueError(f"알 수 없는 Gold 데이터셋: {dataset!r}") from None

        columns = [field.name for field in fields(model)]
        query = _latest_version_query(dataset, columns)

        conn = psycopg2.connect(self._dsn)
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
        finally:
            conn.close()
        return pd.DataFrame(rows, columns=columns)


def build_data_source() -> DataSource:
    kind = os.environ.get("DASHBOARD_DATA_SOURCE", "local")
    if kind == "local":
        root = Path(
            os.environ.get(
                "GOLD_DIR",
                Path(__file__).resolve().parents[2] / "data" / "gold",
            )
        )
        return LocalCsvDataSource(root)
    if kind == "rds":
        dsn = os.environ.get("GOLD_DATABASE_URL")
        if not dsn:
            raise ValueError("DASHBOARD_DATA_SOURCE=rds는 GOLD_DATABASE_URL 환경변수가 필요합니다")
        return RdsDataSource(dsn)
    raise ValueError(f"알 수 없는 DASHBOARD_DATA_SOURCE: {kind!r} (local 또는 rds)")
