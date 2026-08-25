"""대시보드 Gold 데이터 소스.

`DASHBOARD_DATA_SOURCE` 환경변수(local|rds, 기본 local)로 로컬 CSV와 RDS를 전환한다.
RDS 쪽 SELECT 컬럼은 `schema.gold`의 dataclass 필드에서 그대로 만든다.

Gold RDS는 같은 지역·월에 재실행 이력이 버전으로 쌓이므로(`postgres_loader.py`),
`_LATEST_VERSION_PARTITION`에 있는 테이블은 그 파티션(지역·월, `driver_car_suggestion`은
알고리즘 버전까지)별 최신 version 행만 읽는다. 파티션이 없는 테이블
(`recommendation_algorithm`)은 버전 개념이 없는 수동 마스터 테이블이라 전체를 읽는다.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import fields
from pathlib import Path

import pandas as pd
import psycopg2

from schema.gold import (
    DriverCarSuggestion,
    DriverMonthlyProfit,
    RecommendationAlgorithm,
    SilverLineage,
)

_TABLE_MODELS = {
    "driver_aggregation": DriverMonthlyProfit,
    "driver_car_suggestion": DriverCarSuggestion,
    "silver_lineage": SilverLineage,
    "recommendation_algorithm": RecommendationAlgorithm,
}

# 이 파티션 컬럼으로 묶은 그룹마다 최신 version 한 행만 읽는다. 여기 없는
# 테이블(recommendation_algorithm)은 service_area/year_month/version 자체가 없는
# 수동 마스터 테이블이라 전체를 그대로 읽는다.
_LATEST_VERSION_PARTITION = {
    "driver_aggregation": ("service_area", "year_month"),
    # recommendation_algorithm_version_id·threshold 별로 latest version 을 따로
    # 잡아야 다른 알고리즘·threshold 조합이 더 최근에 재실행돼도 이전 조합의
    # 이력이 가려지지 않는다. RDS 지원 인덱스도 이 순서(#997)로 만들어져 있다.
    "driver_car_suggestion": (
        "service_area", "year_month",
        "recommendation_algorithm_version_id", "threshold",
    ),
    # 알고리즘 축이 없는 실행 계보 — service_area/year_month 당 가장 최근 실행만 본다.
    "silver_lineage": ("service_area", "year_month"),
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


def _latest_version_query(
    table: str,
    columns: list[str],
    partition: tuple[str, ...] = ("service_area", "year_month"),
) -> str:
    selected = ", ".join(f"t.{name}" for name in columns)
    condition = " AND ".join(f"{name} = t.{name}" for name in partition)
    return (
        f"SELECT {selected} FROM {table} t "
        f"WHERE t.version = (SELECT MAX(version) FROM {table} WHERE {condition})"
    )


def _select_all_query(table: str, columns: list[str]) -> str:
    return f"SELECT {', '.join(columns)} FROM {table}"


class RdsDataSource(DataSource):
    """Gold RDS에서 테이블별 파티션의 최신 version만 읽는다(파티션 없는 테이블은 전체).

    연결 하나를 만들어 재사용한다. 데이터셋마다 매번 새로 연결하면(SSH 터널 경유
    시 연결당 ~150-300ms) 대시보드 한 번 렌더링에 4번 연결하게 되어 왕복 비용만
    1초 가까이 쌓인다 — `build_data_source()`가 `st.cache_resource`로 감싸져
    프로세스 생존 기간 동안 이 인스턴스(와 연결)를 재사용하는 것을 전제로 한다.
    """

    def __init__(self, dsn: str):
        self._conn = psycopg2.connect(dsn)

    def load(self, dataset: str) -> pd.DataFrame:
        try:
            model = _TABLE_MODELS[dataset]
        except KeyError:
            raise ValueError(f"알 수 없는 Gold 데이터셋: {dataset!r}") from None

        columns = [field.name for field in fields(model)]
        partition = _LATEST_VERSION_PARTITION.get(dataset)
        query = (
            _select_all_query(dataset, columns)
            if partition is None
            else _latest_version_query(dataset, columns, partition)
        )

        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        finally:
            cursor.close()
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
