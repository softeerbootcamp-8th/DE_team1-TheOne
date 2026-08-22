"""Gold RDS 적재의 커밋 전 행수 검증. 이슈 #746.

1. 저장된 행수가 기대치와 같으면 통과한다
2. 저장된 행수가 기대치보다 적거나 많으면 커밋 전에 실패한다
3. 저장된 행수가 0이면 기대치도 0이어도 실패한다
"""

import pytest

from main.spark.jobs.silver_to_gold import postgres_loader


class _CountCursor:
    """`SELECT COUNT(*) ... WHERE year_month = %s AND version = %s` 만 흉내냅니다."""

    def __init__(self, counts: dict[str, int]):
        self.counts = counts
        self.table = None

    def execute(self, sql, parameters):
        self.table = next(
            table for table in postgres_loader.TABLES if f"FROM {table}" in sql
        )
        assert parameters == ("2026-05", 3)

    def fetchone(self):
        return (self.counts[self.table],)


def test_저장된_행수가_기대치와_같으면_통과한다():
    counts = {table: 1 for table in postgres_loader.TABLES}

    postgres_loader._validate_written_rows(_CountCursor(counts), counts, "2026-05", 3)


@pytest.mark.parametrize("actual", [0, 2])
def test_저장된_행수가_기대치와_다르면_커밋전에_실패한다(actual):
    expected = {table: 1 for table in postgres_loader.TABLES}
    counts = {**expected, "monthly_report": actual}

    with pytest.raises(ValueError, match="Gold 적재 검증 실패"):
        postgres_loader._validate_written_rows(
            _CountCursor(counts), expected, "2026-05", 3
        )


def test_기대치가_0이어도_저장된_행이_0이면_실패한다():
    expected = {table: 0 for table in postgres_loader.TABLES}

    with pytest.raises(ValueError, match="Gold 적재 검증 실패"):
        postgres_loader._validate_written_rows(
            _CountCursor(expected), expected, "2026-05", 3
        )
