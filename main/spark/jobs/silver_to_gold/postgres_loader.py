"""Gold 3종(driver_aggregation, driver_vehicle_profit_simulation, monthly_report)을 RDS
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

from schema.gold import DriverMonthlyProfit, DriverVehicleProfitSimulation, MonthlyReport

logger = logging.getLogger(__name__)

_MONTHLY_REPORT = "monthly_report"
_DRIVER_AGGREGATION = "driver_aggregation"
_DRIVER_VEHICLE_PROFIT_SIMULATION = "driver_vehicle_profit_simulation"
TABLES = (_MONTHLY_REPORT, _DRIVER_AGGREGATION, _DRIVER_VEHICLE_PROFIT_SIMULATION)

_TABLE_MODELS = {
    _MONTHLY_REPORT: MonthlyReport,
    _DRIVER_AGGREGATION: DriverMonthlyProfit,
    _DRIVER_VEHICLE_PROFIT_SIMULATION: DriverVehicleProfitSimulation,
}

# PRIMARY KEY는 저장소 쪽 결정이라 dataclass에는 없는 정보라 별도로 둡니다.
# service_area 가 PK 에 없으면 두 지역의 같은 (year_month, version) 행이 충돌합니다.
# driver_id 도 지역 간 유니크하지 않으므로(#805) 지역이 자연 키의 일부입니다.
# 아래 세 함수(_PRIMARY_KEYS / _next_version / _validate_written_rows)는 **함께**
# 지역을 타야 합니다 — 일부만 고치면 안 고친 것보다 나쁩니다(#809):
#   PK 만 고치면 버전이 지역 간 공유 카운터로 남고,
#   검증만 고치면 다른 지역 행을 세어 매번 롤백합니다.
_PRIMARY_KEYS = {
    _MONTHLY_REPORT: ("service_area", "year_month", "version"),
    _DRIVER_AGGREGATION: ("service_area", "year_month", "version", "driver_id"),
    _DRIVER_VEHICLE_PROFIT_SIMULATION: (
        "service_area",
        "year_month",
        "version",
        "driver_id",
        "candidate_vehicle_model_id",
    ),
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


def _create_suggestion_view_sql() -> str:
    """후보 팩트에 기사 선호 순위와 모델별 재고 한도를 적용한 최종 1행 뷰."""
    return """
CREATE OR REPLACE VIEW vw_driver_car_suggestion AS
WITH candidate_base AS (
    SELECT
        simulation.*,
        simulation.candidate_vehicle_model_id = aggregation.vehicle_model_id
            AS is_current
    FROM driver_vehicle_profit_simulation AS simulation
    JOIN driver_aggregation AS aggregation
      ON aggregation.service_area = simulation.service_area
     AND aggregation.year_month = simulation.year_month
     AND aggregation.version = simulation.version
     AND aggregation.driver_id = simulation.driver_id
),
stock_ranked AS (
    SELECT
        candidate_base.*,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) OVER (
            PARTITION BY service_area, year_month, version,
                         candidate_vehicle_model_id
        ) AS occupied_stock,
        ROW_NUMBER() OVER (
            PARTITION BY service_area, year_month, version,
                         candidate_vehicle_model_id, is_current
            ORDER BY expected_net_profit_increase DESC,
                     expected_revenue_increase DESC,
                     driver_id ASC
        ) AS stock_rank
    FROM candidate_base
),
feasible_candidates AS (
    SELECT *
    FROM stock_ranked
    WHERE is_current
       OR stock_rank <= candidate_stock - occupied_stock
),
driver_ranked AS (
    SELECT
        feasible_candidates.*,
        ROW_NUMBER() OVER (
            PARTITION BY service_area, year_month, version, driver_id
            ORDER BY expected_monthly_net_profit DESC,
                     is_current DESC,
                     model_year DESC,
                     candidate_vehicle_model_id ASC
        ) AS driver_rank
    FROM feasible_candidates
)
SELECT
    version,
    driver_id,
    year_month,
    service_area,
    comfort_eligible,
    extra_comfort_eligible,
    candidate_vehicle_model_id AS vehicle_model_id,
    manufacturer,
    model_name,
    model_year,
    recommendation_reason,
    fuel_efficiency,
    recommended_monthly_lease_fee,
    expected_monthly_fuel_cost,
    expected_monthly_net_profit,
    expected_net_profit_increase,
    expected_revenue_increase
FROM driver_ranked
WHERE driver_rank = 1
""".strip()


def _create_compatibility_view_sql() -> str:
    """기존 조회자는 수정하지 않고 canonical 뷰로 연결합니다."""
    return """
CREATE OR REPLACE VIEW driver_car_suggestion AS
SELECT * FROM vw_driver_car_suggestion
""".strip()


def _next_version(cursor, service_area: str, year_month: str) -> int:
    """`monthly_report`에서 이 (지역, year_month)의 기존 버전을 top(1)로 확인해 +1.

    3개 테이블은 항상 같은 버전으로 함께 적재되므로(이 모듈이 그렇게 보장합니다),
    monthly_report 한 행만 봐도 이 달의 현재 버전을 알 수 있습니다.

    지역으로 안 좁히면 버전이 지역 간 공유 카운터가 됩니다 — NYC 가 v1 을 쓴 뒤
    TX 의 **첫** 적재가 v2 로 기록되어 지역별 버전 이력이 무의미해집니다.
    """
    cursor.execute(
        f"SELECT version FROM {_MONTHLY_REPORT} "
        "WHERE service_area = %s AND year_month = %s "
        "ORDER BY version DESC LIMIT 1",
        (service_area, year_month),
    )
    row = cursor.fetchone()
    return row[0] + 1 if row else 1


def _validate_written_rows(
    cursor,
    written: dict[str, int],
    service_area: str,
    year_month: str,
    version: int,
) -> None:
    """커밋 전에 Gold 3종이 기대한 버전·행 수로 들어갔는지 확인합니다.

    `execute_values` 는 영향받은 행 수를 돌려주지 않아 `written`(itertuples 로 센
    값)이 실제로 반영됐는지 확인할 방법이 없었습니다. 여기서 실제 저장된 행을
    다시 세어 대조하고, 여기서 실패하면 트랜잭션 전체가 롤백됩니다.
    """
    for table in TABLES:
        # 지역으로 안 좁히면 다른 지역 행까지 세어 expected 와 어긋나고, 두 지역이
        # 같은 (year_month, version) 을 갖는 순간부터 매번 롤백합니다.
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} "
            "WHERE service_area = %s AND year_month = %s AND version = %s",
            (service_area, year_month, version),
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


def _validate_frame_grains(frames: dict[str, pd.DataFrame]) -> None:
    """DB 연결 전에 기사 N × 후보 차량 M 적재 계약을 확인합니다."""
    aggregation = frames[_DRIVER_AGGREGATION]
    simulation = frames[_DRIVER_VEHICLE_PROFIT_SIMULATION]
    driver_count = aggregation["driver_id"].nunique()
    model_count = simulation["candidate_vehicle_model_id"].nunique()
    candidate_keys = simulation[["driver_id", "candidate_vehicle_model_id"]]
    expected_rows = driver_count * model_count

    if (
        len(aggregation) != driver_count
        or set(simulation["driver_id"]) != set(aggregation["driver_id"])
        or len(simulation) != expected_rows
        or len(candidate_keys.drop_duplicates()) != expected_rows
    ):
        raise ValueError(
            "Gold 시뮬레이션 그레인 불일치: "
            f"drivers={driver_count} vehicle_models={model_count} "
            f"expected={expected_rows} actual={len(simulation)}"
        )


def write_gold_to_postgres(
    frames: dict[str, pd.DataFrame], dsn: str, service_area: str, year_month: str
) -> dict[str, int]:
    """Gold 3종을 한 트랜잭션으로 적재합니다. 반환값은 `{테이블명: 적재 행 수}`.

    `frames`는 job.py의 `outputs`와 같은 모양(`toPandas()` 이전이 아니라 이후)이어야
    합니다 — CSV로 쓰던 것과 같은 시점의 값을 그대로 재사용합니다.
    """
    missing = set(TABLES) - set(frames)
    if missing:
        raise ValueError(f"frames에 테이블이 빠졌습니다: {sorted(missing)}")
    _validate_frame_grains(frames)

    conn = psycopg2.connect(dsn)
    try:
        with conn:  # 정상 종료 시 commit, 예외 시 rollback
            with conn.cursor() as cursor:
                for table in TABLES:
                    cursor.execute(_create_table_sql(table))

                version = _next_version(cursor, service_area, year_month)
                logger.info(
                    "Gold 적재 버전 결정: service_area=%s year_month=%s version=%d",
                    service_area,
                    year_month,
                    version,
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
                _validate_written_rows(
                    cursor, written, service_area, year_month, version
                )
        return written
    finally:
        conn.close()
