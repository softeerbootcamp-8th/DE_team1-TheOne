"""Gold RDS 적재의 커밋 전 행수 검증. 이슈 #746, #809.

1. 저장된 행수가 기대치와 같으면 통과한다
2. 저장된 행수가 기대치보다 적거나 많으면 커밋 전에 실패한다
3. 저장된 행수가 0이면 기대치도 0이어도 실패한다
4. 검증·버전·PK가 모두 지역으로 좁혀진다 — 하나라도 빠지면 안 고친 것보다 나쁘다
"""

import pytest

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


def test_Gold_3종_스키마에_service_area_컬럼이_있다():
    for table in postgres_loader.TABLES:
        assert "service_area TEXT NOT NULL" in postgres_loader._create_table_sql(table)
