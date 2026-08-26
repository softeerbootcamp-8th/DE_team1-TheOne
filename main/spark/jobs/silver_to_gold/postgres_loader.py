"""Gold 3종(driver_aggregation, driver_car_suggestion, silver_lineage)을 RDS
PostgreSQL에 원자적으로, 버전을 붙여 적재합니다.

같은 year_month에 이미 데이터가 있으면 그 버전 + 1로, 없으면 버전 1로 세
테이블에 같은 버전을 붙여 적재합니다. 하나라도 실패하면 셋 다
반영되지 않아야 하므로 하나의 트랜잭션으로 묶습니다.
"""

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import fields
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras

from schema.gold import (
    DriverCarSuggestion,
    DriverMonthlyProfit,
    GoldLoadVersion,
    SilverLineage,
)

logger = logging.getLogger(__name__)

_DRIVER_AGGREGATION = "driver_aggregation"
_DRIVER_CAR_SUGGESTION = "driver_car_suggestion"
_SILVER_LINEAGE = "silver_lineage"
TABLES = (_DRIVER_AGGREGATION, _DRIVER_CAR_SUGGESTION, _SILVER_LINEAGE)
_GOLD_LOAD_VERSIONS = "gold_load_versions"

_LINEAGE_COLUMNS = (
    "silver_monthly_taxi_trip_s3_link",
    "silver_driver_vehicle_monthly_snapshot_s3_link",
    "silver_lease_vehicle_inventory_s3_link",
    "silver_gas_ev_price_s3_link",
)

_TABLE_MODELS = {
    _DRIVER_AGGREGATION: DriverMonthlyProfit,
    _DRIVER_CAR_SUGGESTION: DriverCarSuggestion,
    _SILVER_LINEAGE: SilverLineage,
}

# PRIMARY KEY는 저장소 쪽 결정이라 dataclass에는 없는 정보라 별도로 둡니다.
# service_area 가 PK 에 없으면 두 지역의 같은 (year_month, version) 행이 충돌합니다.
# driver_id 도 지역 간 유니크하지 않으므로(#805) 지역이 자연 키의 일부입니다.
# 아래 세 항목(_PRIMARY_KEYS / _next_version / _validate_written_rows)은 **함께**
# 지역을 타야 합니다 — 일부만 고치면 안 고친 것보다 나쁩니다(#809):
#   PK 만 고치면 버전이 지역 간 공유 카운터로 남고,
#   검증만 고치면 다른 지역 행을 세어 매번 롤백합니다.
# silver_lineage 는 기사 그레인이 아니라 실행 그레인(실행당 한 행)이라 driver_id 가 없습니다.
_PRIMARY_KEYS = {
    _DRIVER_AGGREGATION: ("service_area", "year_month", "version", "driver_id"),
    _DRIVER_CAR_SUGGESTION: (
        "service_area", "year_month", "version", "driver_id",
        "recommendation_algorithm_version_id", "threshold",
    ),
    _SILVER_LINEAGE: ("service_area", "year_month", "version"),
}

_SQL_TYPES = {
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    str: "TEXT",
    datetime: "TIMESTAMPTZ",
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


def _create_version_table_sql() -> str:
    columns = []
    for field in fields(GoldLoadVersion):
        default = " DEFAULT CURRENT_TIMESTAMP" if field.name == "created_at" else ""
        columns.append(f"{field.name} {_SQL_TYPES[field.type]} NOT NULL{default}")
    return (
        f"CREATE TABLE IF NOT EXISTS {_GOLD_LOAD_VERSIONS} (\n    "
        + ",\n    ".join(columns)
        + ",\n    PRIMARY KEY (service_area, year_month, version),\n"
        "    CONSTRAINT gold_load_versions_load_fingerprint_key\n"
        "        UNIQUE (service_area, year_month, load_fingerprint)\n)"
    )


def _record_gold_version(
    cursor,
    service_area: str,
    year_month: str,
    version: int,
    load_fingerprint: str,
) -> None:
    cursor.execute(
        f"INSERT INTO {_GOLD_LOAD_VERSIONS} "
        "(service_area, year_month, version, load_fingerprint) "
        "VALUES (%s, %s, %s, %s)",
        (service_area, year_month, version, load_fingerprint),
    )


def gold_config_hash(
    service_area: str,
    year_month: str,
    silver_inputs: Mapping[str, object],
    recommendation_parameters: Iterable[tuple[int, int]],
) -> str:
    """Silver 입력과 추천 설정을 정렬 직렬화한 Gold 설정 SHA-256입니다.

    job.py가 SilverLineage.config_hash를 만들고 이 적재기가 멱등성 fingerprint를
    검증할 때 같은 함수를 씁니다. 둘이 서로 다른 정규화 규칙을 가지면 같은 실행이
    새 버전으로 쌓이거나, 반대로 다른 설정이 기존 버전을 재사용할 수 있습니다.
    """
    combinations = {
        (int(algorithm_version_id), int(threshold))
        for algorithm_version_id, threshold in recommendation_parameters
    }
    payload = {
        "service_area": service_area,
        "year_month": year_month,
        "silver_inputs": {
            column: str(silver_inputs[column]) for column in _LINEAGE_COLUMNS
        },
        "recommendation_parameters": [
            {
                "recommendation_algorithm_version_id": algorithm_version_id,
                "threshold": threshold,
            }
            for algorithm_version_id, threshold in sorted(combinations)
        ],
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _gold_load_fingerprint(
    frames: dict[str, pd.DataFrame], service_area: str, year_month: str
) -> str:
    """실제 적재 프레임에서 재실행 멱등성 fingerprint를 계산합니다."""
    lineage = frames[_SILVER_LINEAGE].iloc[0]
    recommendation_parameters = frames[_DRIVER_CAR_SUGGESTION][
        ["recommendation_algorithm_version_id", "threshold"]
    ].itertuples(index=False, name=None)
    return gold_config_hash(
        service_area,
        year_month,
        lineage,
        recommendation_parameters,
    )


def _existing_gold_version(
    cursor, service_area: str, year_month: str, load_fingerprint: str
) -> int | None:
    cursor.execute(
        f"SELECT version FROM {_GOLD_LOAD_VERSIONS} "
        "WHERE service_area = %s AND year_month = %s AND load_fingerprint = %s",
        (service_area, year_month, load_fingerprint),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _written_rows_for_version(
    cursor, service_area: str, year_month: str, version: int
) -> dict[str, int]:
    """이미 성공한 실행이 적재한 행 수를 다시 읽어 기존 반환 형식을 유지합니다."""
    written: dict[str, int] = {}
    for table in TABLES:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} "
            "WHERE service_area = %s AND year_month = %s AND version = %s",
            (service_area, year_month, version),
        )
        rows = cursor.fetchone()[0]
        if rows <= 0:
            raise ValueError(
                "기존 Gold 적재 메타데이터와 데이터가 일치하지 않습니다: "
                f"table={table} service_area={service_area} "
                f"year_month={year_month} version={version} rows={rows}"
            )
        written[table] = rows
    return written


def _acquire_partition_lock(cursor, service_area: str, year_month: str) -> None:
    """(service_area, year_month) 파티션 단위로 버전 조회부터 적재까지 직렬화합니다.

    #973 종료 뒤에도 잠금 구현이 없어(#1056) 같은 파티션 동시 실행이 같은
    `_next_version` 결과를 볼 수 있었습니다. 트랜잭션 스코프 잠금
    (`pg_advisory_xact_lock`)이라 commit/rollback 시 자동 해제되어 별도 unlock이
    필요 없고, 두 파티션이 서로 다른 (service_area, year_month) 해시 쌍을 가지므로
    다른 파티션의 동시 실행은 막지 않습니다.
    """
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
        (service_area, year_month),
    )


def _next_version(cursor, service_area: str, year_month: str) -> int:
    """`driver_aggregation`에서 지역·월의 기존 버전을 확인해 +1.

    세 테이블은 항상 같은 버전으로 함께 적재되므로 집계 테이블만 봐도 이 달의
    현재 버전을 알 수 있습니다.

    지역으로 안 좁히면 버전이 지역 간 공유 카운터가 됩니다 — NYC 가 v1 을 쓴 뒤
    TX 의 **첫** 적재가 v2 로 기록되어 지역별 버전 이력이 무의미해집니다.
    """
    cursor.execute(
        f"SELECT version FROM {_DRIVER_AGGREGATION} "
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
    """DB 연결 전에 집계는 기사당 한 행, 최종 추천은 (알고리즘, threshold) 조합마다
    기사당 정확히 한 행인지 확인합니다.

    추천은 #997부터 알고리즘·threshold 조합 수만큼 기사당 여러 행이 쌓이므로
    전체 행 수는 기사 수와 달라도 됩니다 — 조합별로 쪼개서 봐야 합니다.
    """
    aggregation = frames[_DRIVER_AGGREGATION]
    suggestion = frames[_DRIVER_CAR_SUGGESTION]
    driver_ids = set(aggregation["driver_id"])
    driver_count = len(driver_ids)

    if len(aggregation) != driver_count:
        raise ValueError(
            "Gold 기사 그레인 불일치: "
            f"aggregation={len(aggregation)} drivers={driver_count}"
        )

    group_columns = ["recommendation_algorithm_version_id", "threshold"]
    groups = suggestion.groupby(group_columns)
    # suggestion이 비어 있으면 groupby가 그룹을 하나도 안 만들어 루프가 통째로
    # 스킵된다 — 빈 추천이 조용히 통과하지 않도록 그룹이 있어야 함을 먼저 확인.
    if len(groups) == 0:
        raise ValueError(f"Gold 기사 그레인 불일치: suggestion={len(suggestion)}행")

    for (algorithm_version_id, threshold), group in groups:
        if len(group) != driver_count or set(group["driver_id"]) != driver_ids:
            raise ValueError(
                "Gold 기사 그레인 불일치: "
                f"algorithm={algorithm_version_id} threshold={threshold} "
                f"suggestion={len(group)} drivers={driver_count}"
            )

    lineage = frames[_SILVER_LINEAGE]
    if len(lineage) != 1:
        raise ValueError(f"Gold Silver 계보는 실행당 한 행이어야 합니다: rows={len(lineage)}")


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
    load_fingerprint = _gold_load_fingerprint(frames, service_area, year_month)
    recorded_config_hash = str(frames[_SILVER_LINEAGE].iloc[0]["config_hash"])
    if recorded_config_hash != load_fingerprint:
        raise ValueError(
            "Gold config_hash가 실제 입력·추천 설정과 일치하지 않습니다: "
            f"recorded={recorded_config_hash} actual={load_fingerprint}"
        )

    conn = psycopg2.connect(dsn)
    try:
        with conn:  # 정상 종료 시 commit, 예외 시 rollback
            with conn.cursor() as cursor:
                for table in TABLES:
                    cursor.execute(_create_table_sql(table))
                cursor.execute(_create_version_table_sql())

                _acquire_partition_lock(cursor, service_area, year_month)

                existing_version = _existing_gold_version(
                    cursor, service_area, year_month, load_fingerprint
                )
                if existing_version is not None:
                    logger.info(
                        "동일한 Gold 실행 재사용: service_area=%s year_month=%s "
                        "version=%d load_fingerprint=%s",
                        service_area,
                        year_month,
                        existing_version,
                        load_fingerprint,
                    )
                    return _written_rows_for_version(
                        cursor, service_area, year_month, existing_version
                    )

                version = _next_version(cursor, service_area, year_month)
                logger.info(
                    "Gold 적재 버전 결정: service_area=%s year_month=%s "
                    "version=%d load_fingerprint=%s",
                    service_area,
                    year_month,
                    version,
                    load_fingerprint,
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
                _record_gold_version(
                    cursor,
                    service_area,
                    year_month,
                    version,
                    load_fingerprint,
                )
        return written
    finally:
        conn.close()
