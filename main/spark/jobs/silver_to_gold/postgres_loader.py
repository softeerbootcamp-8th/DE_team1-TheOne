"""Gold 3종(driver_aggregation, driver_car_suggestion, monthly_report)을 RDS
PostgreSQL에 원자적으로, 버전을 붙여 적재합니다.

같은 year_month에 이미 데이터가 있으면 그 버전 + 1로, 없으면 버전 1로 3개
테이블 전부에 같은 버전을 붙여 적재합니다. 3개 중 하나라도 실패하면 셋 다
반영되지 않아야 하므로 하나의 트랜잭션으로 묶습니다.
"""

import logging
from dataclasses import fields

import pandas as pd
import psycopg2
import psycopg2.extras

from schema.gold import DriverMonthlyProfit, MonthlyReport, MonthlyVehicleRecommendation

logger = logging.getLogger(__name__)

_MONTHLY_REPORT = "monthly_report"
_DRIVER_AGGREGATION = "driver_aggregation"
_DRIVER_CAR_SUGGESTION = "driver_car_suggestion"
TABLES = (_MONTHLY_REPORT, _DRIVER_AGGREGATION, _DRIVER_CAR_SUGGESTION)

_TABLE_MODELS = {
    _MONTHLY_REPORT: MonthlyReport,
    _DRIVER_AGGREGATION: DriverMonthlyProfit,
    _DRIVER_CAR_SUGGESTION: MonthlyVehicleRecommendation,
}

# PRIMARY KEY는 저장소 쪽 결정이라 dataclass에는 없는 정보라 별도로 둡니다.
_PRIMARY_KEYS = {
    _MONTHLY_REPORT: ("year_month", "version"),
    _DRIVER_AGGREGATION: ("year_month", "version", "driver_id"),
    _DRIVER_CAR_SUGGESTION: ("year_month", "version", "driver_id"),
}

_SQL_TYPES = {
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    str: "TEXT",
}


def _create_table_sql(table: str) -> str:
    columns = [
        f"{field.name} {_SQL_TYPES[field.type]} NOT NULL"
        for field in fields(_TABLE_MODELS[table])
    ]
    primary_key = ", ".join(_PRIMARY_KEYS[table])
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n    "
        + ",\n    ".join(columns)
        + f",\n    PRIMARY KEY ({primary_key})\n)"
    )


def _next_version(cursor, year_month: str) -> int:
    """`monthly_report`에서 이 year_month의 기존 버전을 top(1)로 확인해 +1. 없으면 1.

    3개 테이블은 항상 같은 버전으로 함께 적재되므로(이 모듈이 그렇게 보장합니다),
    monthly_report 한 행만 봐도 이 달의 현재 버전을 알 수 있습니다.
    """
    cursor.execute(
        f"SELECT version FROM {_MONTHLY_REPORT} WHERE year_month = %s "
        "ORDER BY version DESC LIMIT 1",
        (year_month,),
    )
    row = cursor.fetchone()
    return row[0] + 1 if row else 1


def _validate_written_rows(
    cursor,
    written: dict[str, int],
    year_month: str,
    version: int,
) -> None:
    """커밋 전에 Gold 3종이 기대한 버전·행 수로 들어갔는지 확인합니다."""
    for table in TABLES:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE year_month = %s AND version = %s",
            (year_month, version),
        )
        actual = cursor.fetchone()[0]
        expected = written[table]
        if actual != expected or actual <= 0:
            raise ValueError(
                "Gold 적재 검증 실패: "
                f"table={table} year_month={year_month} version={version} "
                f"expected={expected} actual={actual}"
            )
        logger.info(
            "Gold 적재 검증 통과: table=%s year_month=%s version=%d rows=%d",
            table,
            year_month,
            version,
            actual,
        )


def write_gold_to_postgres(
    frames: dict[str, pd.DataFrame], dsn: str, year_month: str
) -> dict[str, int]:
    """Gold 3종을 한 트랜잭션으로 적재합니다. 반환값은 `{테이블명: 적재 행 수}`.

    `frames`는 job.py의 `outputs`와 같은 모양(`toPandas()` 이전이 아니라 이후)이어야
    합니다 — CSV로 쓰던 것과 같은 시점의 값을 그대로 재사용합니다.
    """
    missing = set(TABLES) - set(frames)
    if missing:
        raise ValueError(f"frames에 테이블이 빠졌습니다: {sorted(missing)}")

    conn = psycopg2.connect(dsn)
    try:
        with conn:  # 정상 종료 시 commit, 예외 시 rollback
            with conn.cursor() as cursor:
                for table in TABLES:
                    cursor.execute(_create_table_sql(table))

                version = _next_version(cursor, year_month)
                logger.info(
                    "Gold 적재 버전 결정: year_month=%s version=%d", year_month, version
                )

                written: dict[str, int] = {}
                for table in TABLES:
                    frame = frames[table].copy()
                    frame["version"] = version
                    columns = list(frame.columns)
                    rows = list(frame.itertuples(index=False, name=None))
                    psycopg2.extras.execute_values(
                        cursor,
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
                        rows,
                    )
                    written[table] = len(rows)
                    logger.info("Gold 적재: table=%s rows=%d", table, len(rows))
                _validate_written_rows(cursor, written, year_month, version)
        return written
    finally:
        conn.close()
