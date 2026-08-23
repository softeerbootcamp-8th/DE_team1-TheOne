"""Gold RDS 적재의 커밋 전 행수 검증. 이슈 #746, #809.

1. 저장된 행수가 기대치와 같으면 통과한다
2. 저장된 행수가 기대치보다 적거나 많으면 커밋 전에 실패한다
3. 저장된 행수가 0이면 기대치도 0이어도 실패한다
4. 검증·버전·PK가 모두 지역으로 좁혀진다 — 하나라도 빠지면 안 고친 것보다 나쁘다
5. 시뮬레이션 PK는 같은 기사의 차량 모델별 후보를 구분한다
6. 최종 추천 뷰는 기사 순위와 모델별 재고 제약을 적용한다
"""

from pathlib import Path

import pytest
import pandas as pd

from main.spark.jobs.silver_to_gold import postgres_loader


class _CountCursor:
    """`SELECT COUNT(*) ... WHERE service_area/year_month/version` 만 흉내냅니다."""

    def __init__(self, counts: dict[str, int]):
        self.counts = counts
        self.table = None
        self.sql = None

    def execute(self, sql, parameters):
        self.table = next(
            table for table in postgres_loader.TABLES if f"FROM {table}" in sql
        )
        self.sql = sql
        # 지역이 조건에서 빠지면 다른 지역 행까지 세어 매번 롤백합니다.
        assert "service_area = %s" in sql
        assert parameters == ("NYC", "2026-05", 3)

    def fetchone(self):
        return (self.counts[self.table],)


def test_저장된_행수가_기대치와_같으면_통과한다():
    counts = {table: 1 for table in postgres_loader.TABLES}

    postgres_loader._validate_written_rows(
        _CountCursor(counts), counts, "NYC", "2026-05", 3
    )


@pytest.mark.parametrize("actual", [0, 2])
def test_저장된_행수가_기대치와_다르면_커밋전에_실패한다(actual):
    expected = {table: 1 for table in postgres_loader.TABLES}
    counts = {**expected, "monthly_report": actual}

    with pytest.raises(ValueError, match="Gold 적재 검증 실패"):
        postgres_loader._validate_written_rows(
            _CountCursor(counts), expected, "NYC", "2026-05", 3
        )


def test_기대치가_0이어도_저장된_행이_0이면_실패한다():
    expected = {table: 0 for table in postgres_loader.TABLES}

    with pytest.raises(ValueError, match="Gold 적재 검증 실패"):
        postgres_loader._validate_written_rows(
            _CountCursor(expected), expected, "NYC", "2026-05", 3
        )


class _VersionCursor:
    """`SELECT version FROM monthly_report WHERE ... ORDER BY version DESC` 흉내."""

    def __init__(self, rows_by_key: dict[tuple, int]):
        self.rows_by_key = rows_by_key
        self.sql = None
        self.parameters = None

    def execute(self, sql, parameters):
        self.sql = sql
        self.parameters = parameters

    def fetchone(self):
        row = self.rows_by_key.get(self.parameters)
        return (row,) if row is not None else None


def test_버전은_지역별로_따로_센다():
    """지역으로 안 좁히면 버전이 지역 간 공유 카운터가 됩니다 — NYC 가 v1 을 쓴 뒤
    TX 의 **첫** 적재가 v2 로 기록되어 지역별 버전 이력이 무의미해집니다."""
    cursor = _VersionCursor({("NYC", "2026-05"): 1})

    assert postgres_loader._next_version(cursor, "NYC", "2026-05") == 2
    # 같은 달이라도 TX 는 자기 이력이 없으므로 1 부터 시작해야 합니다.
    assert postgres_loader._next_version(cursor, "TX", "2026-05") == 1
    assert "service_area = %s" in cursor.sql


def test_세_테이블_모두_PK에_지역이_들어간다():
    """PK 에 지역이 없으면 두 지역의 같은 (year_month, version) 행이 충돌해
    IntegrityError 로 트랜잭션 전체가 롤백됩니다. driver_id 도 지역 간 유니크하지
    않으므로(#805) 지역이 자연 키의 일부여야 합니다."""
    for table in postgres_loader.TABLES:
        primary_key = postgres_loader._PRIMARY_KEYS[table]

        assert "service_area" in primary_key, table
        # DDL 에도 실제로 반영되는지 — dataclass 에 필드가 없으면 여기서 걸립니다.
        assert "PRIMARY KEY (service_area," in postgres_loader._create_table_sql(table)


def test_시뮬레이션_PK는_기사별_차량후보를_구분한다():
    assert postgres_loader._PRIMARY_KEYS["driver_vehicle_profit_simulation"] == (
        "service_area",
        "year_month",
        "version",
        "driver_id",
        "candidate_vehicle_model_id",
    )


def test_최종추천뷰는_재고와_기사선호순위를_적용한다():
    sql = postgres_loader._create_suggestion_view_sql()

    assert "CREATE OR REPLACE VIEW vw_driver_car_suggestion" in sql
    assert "candidate_stock - occupied_stock" in sql
    assert sql.count("ROW_NUMBER() OVER") == 2
    assert "WHERE driver_rank = 1" in sql
    assert "candidate_vehicle_model_id AS vehicle_model_id" in sql


def test_기존추천테이블명은_호환뷰로_유지한다():
    sql = postgres_loader._create_compatibility_view_sql()

    assert "CREATE OR REPLACE VIEW driver_car_suggestion" in sql
    assert "SELECT * FROM vw_driver_car_suggestion" in sql


def test_마이그레이션과_적재기의_최종추천뷰_SQL이_같다():
    migration_path = (
        Path(__file__).parents[1]
        / "jobs/silver_to_gold/migrations/2026-08-23_expand_vehicle_recommendations.sql"
    )
    migration = migration_path.read_text()
    view_body = migration.split("CREATE VIEW vw_driver_car_suggestion AS", 1)[1]
    view_body = view_body.split(";\n\nCREATE VIEW driver_car_suggestion AS", 1)[0]
    migration_view_sql = (
        "CREATE OR REPLACE VIEW vw_driver_car_suggestion AS" + view_body
    )

    assert migration_view_sql.strip() == postgres_loader._create_suggestion_view_sql()


def _grain_frames(simulation_rows):
    return {
        "driver_aggregation": pd.DataFrame({"driver_id": ["D1", "D2"]}),
        "driver_vehicle_profit_simulation": pd.DataFrame(
            simulation_rows,
            columns=["driver_id", "candidate_vehicle_model_id"],
        ),
        "monthly_report": pd.DataFrame({"recommended_driver_count": [1]}),
    }


def test_시뮬레이션은_기사수와_후보차량수의_곱을_적재한다():
    frames = _grain_frames(
        [(driver, model) for driver in ("D1", "D2") for model in ("A", "B", "C")]
    )

    postgres_loader._validate_frame_grains(frames)


def test_시뮬레이션의_기사차량조합이_빠지면_적재전에_실패한다():
    frames = _grain_frames(
        [("D1", "A"), ("D1", "B"), ("D2", "A")]
    )

    with pytest.raises(ValueError, match="시뮬레이션 그레인 불일치"):
        postgres_loader._validate_frame_grains(frames)


def test_Gold_3종_스키마에_service_area_컬럼이_있다():
    for table in postgres_loader.TABLES:
        assert "service_area TEXT NOT NULL" in postgres_loader._create_table_sql(table)
