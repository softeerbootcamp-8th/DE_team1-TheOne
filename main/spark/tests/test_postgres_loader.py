"""Gold RDS 3종 적재 검증. 이슈 #746, #809, #927, #1054.

1. 저장된 행수가 기대치와 같으면 통과한다
2. 저장된 행수가 기대치보다 적거나 많으면 커밋 전에 실패한다
3. 저장된 행수가 0이면 기대치도 0이어도 실패한다
4. 검증·버전·PK가 모두 지역으로 좁혀진다 — 하나라도 빠지면 안 고친 것보다 나쁘다
5. 집계와 최종 추천은 모두 기사당 한 행
6. 커밋된 실행을 재시도하면 같은 버전을 재사용하고, 입력·설정이 바뀌면 새 버전을 쓴다
7. 실행·코드·설정 계보는 중앙 스키마와 적재 설정 hash가 일치해야 한다
8. 기존 silver_lineage 행은 migration에서 legacy 식별자로 백필한다
"""

from dataclasses import fields
from pathlib import Path
import pytest
import pandas as pd

from main.spark.jobs.silver_to_gold import postgres_loader
from schema.gold import DriverCarSuggestion, SilverLineage


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
    counts = {**expected, "driver_car_suggestion": actual}

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
    """Gold 재고 테이블의 최신 version 조회를 흉내냅니다."""

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


def test_파티션_잠금은_지역과_월_해시_두_개를_키로_건다():
    """서로 다른 (service_area, year_month) 는 해시 쌍이 달라 잠금이 겹치지 않으므로
    다른 파티션의 동시 실행을 막지 않는다."""
    class Cursor:
        def __init__(self):
            self.sql = None
            self.parameters = None

        def execute(self, sql, parameters):
            self.sql = " ".join(sql.split())
            self.parameters = parameters

    cursor = Cursor()

    postgres_loader._acquire_partition_lock(cursor, "NYC", "2026-05")

    assert cursor.sql == "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))"
    assert cursor.parameters == ("NYC", "2026-05")


def test_버전은_지역별로_따로_센다():
    """지역으로 안 좁히면 버전이 지역 간 공유 카운터가 됩니다 — NYC 가 v1 을 쓴 뒤
    TX 의 **첫** 적재가 v2 로 기록되어 지역별 버전 이력이 무의미해집니다."""
    cursor = _VersionCursor({("NYC", "2026-05"): 1})

    assert postgres_loader._next_version(cursor, "NYC", "2026-05") == 2
    # 같은 달이라도 TX 는 자기 이력이 없으므로 1 부터 시작해야 합니다.
    assert postgres_loader._next_version(cursor, "TX", "2026-05") == 1
    assert "service_area = %s" in cursor.sql


def test_두_테이블_모두_PK에_지역이_들어간다():
    """PK 에 지역이 없으면 두 지역의 같은 (year_month, version) 행이 충돌해
    IntegrityError 로 트랜잭션 전체가 롤백됩니다. driver_id 도 지역 간 유니크하지
    않으므로(#805) 지역이 자연 키의 일부여야 합니다."""
    for table in postgres_loader.TABLES:
        primary_key = postgres_loader._PRIMARY_KEYS[table]

        assert "service_area" in primary_key, table
        # DDL 에도 실제로 반영되는지 — dataclass 에 필드가 없으면 여기서 걸립니다.
        assert "PRIMARY KEY (service_area," in postgres_loader._create_table_sql(table)


def test_최종추천_PK는_기사당_알고리즘_threshold별_한행이다():
    assert postgres_loader._PRIMARY_KEYS["driver_car_suggestion"] == (
        "service_area",
        "year_month",
        "version",
        "driver_id",
        "recommendation_algorithm_version_id",
        "threshold",
    )


def test_최종추천은_내부후보와_재고컬럼을_내보내지_않는다():
    columns = {field.name for field in fields(DriverCarSuggestion)}

    assert "candidate_vehicle_model_id" not in columns
    assert "stock" not in columns


def _grain_frames(suggestion_rows):
    frames = {
        "driver_aggregation": pd.DataFrame({"driver_id": ["D1", "D2"]}),
        "driver_car_suggestion": pd.DataFrame(
            suggestion_rows,
            columns=["driver_id", "recommendation_algorithm_version_id", "threshold"],
        ),
        "silver_lineage": pd.DataFrame({
            "service_area": ["NYC"],
            "year_month": ["2026-05"],
            "airflow_run_id": ["scheduled__2026-05-01T00:00:00+00:00"],
            "code_sha": ["abc1234"],
            "silver_monthly_taxi_trip_s3_link": ["s3://silver/trips/v1"],
            "silver_driver_vehicle_monthly_snapshot_s3_link": [
                "s3://silver/drivers/v1"
            ],
            "silver_lease_vehicle_inventory_s3_link": ["s3://silver/inventory/v1"],
            "silver_gas_ev_price_s3_link": ["s3://silver/fuel/v1"],
        }),
    }
    frames["silver_lineage"]["config_hash"] = postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05"
    )
    return frames


def test_최종추천은_조합별로_기사당_한행을_적재한다():
    """추천은 알고리즘·threshold 조합마다 기사당 한 행씩 쌓여 전체 행 수는
    기사 수의 배수가 된다(#997) — 조합별로는 여전히 기사당 정확히 한 행."""
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100), ("D2", 2, 100),
        ("D1", 2, 200), ("D2", 2, 200),
    ])

    postgres_loader._validate_frame_grains(frames)


def test_추천이_아예_비어있으면_적재전에_실패한다():
    """groupby는 빈 프레임에서 그룹을 하나도 안 만들어 루프가 스킵되므로,
    빈 추천이 조합별 검증을 통과한 것처럼 조용히 넘어가면 안 된다."""
    frames = _grain_frames([])

    with pytest.raises(ValueError, match="Gold 기사 그레인 불일치"):
        postgres_loader._validate_frame_grains(frames)


def test_한_조합에서만_기사가_빠지면_적재전에_실패한다():
    """다른 조합이 정상이어도 한 조합에서 기사가 빠지면 가려지지 않고 잡혀야 한다."""
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100),
    ])

    with pytest.raises(ValueError, match="Gold 기사 그레인 불일치"):
        postgres_loader._validate_frame_grains(frames)


def test_Gold_3종_스키마에_service_area_컬럼이_있다():
    for table in postgres_loader.TABLES:
        assert "service_area TEXT NOT NULL" in postgres_loader._create_table_sql(table)


def test_SilverLineage_중앙스키마와_DDL에_실행코드설정_식별자가_있다():
    columns = {field.name for field in fields(SilverLineage)}
    expected = {"airflow_run_id", "code_sha", "config_hash"}

    assert expected <= columns
    sql = postgres_loader._create_table_sql("silver_lineage")
    for column in expected:
        assert f"{column} TEXT NOT NULL" in sql


def test_Gold_버전_메타데이터는_생성시각과_멱등성_제약을_가진다():
    sql = postgres_loader._create_version_table_sql()

    assert "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP" in sql
    assert "PRIMARY KEY (service_area, year_month, version)" in sql
    assert "load_fingerprint TEXT NOT NULL" in sql
    assert "gold_load_versions_load_fingerprint_key" in sql
    assert "UNIQUE (service_area, year_month, load_fingerprint)" in sql


def test_같은_입력과_추천설정은_행순서가_달라도_같은_fingerprint다():
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100), ("D2", 2, 100),
    ])
    reordered = {name: frame.copy() for name, frame in frames.items()}
    reordered["driver_car_suggestion"] = reordered[
        "driver_car_suggestion"
    ].iloc[::-1]

    first = postgres_loader._gold_load_fingerprint(frames, "NYC", "2026-05")
    second = postgres_loader._gold_load_fingerprint(reordered, "NYC", "2026-05")

    assert first == second
    assert len(first) == 64


def test_입력버전이나_추천설정이_바뀌면_fingerprint도_바뀐다():
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100), ("D2", 2, 100),
    ])
    changed_input = {name: frame.copy() for name, frame in frames.items()}
    changed_input["silver_lineage"].loc[
        0, "silver_monthly_taxi_trip_s3_link"
    ] = "s3://silver/trips/v2"
    changed_setting = {name: frame.copy() for name, frame in frames.items()}
    changed_setting["driver_car_suggestion"].loc[
        changed_setting["driver_car_suggestion"]["threshold"] == 100,
        "threshold",
    ] = 200

    original = postgres_loader._gold_load_fingerprint(frames, "NYC", "2026-05")

    assert original != postgres_loader._gold_load_fingerprint(
        changed_input, "NYC", "2026-05"
    )
    assert original != postgres_loader._gold_load_fingerprint(
        changed_setting, "NYC", "2026-05"
    )


def test_같은_경로라도_입력_내용이_달라지면_fingerprint도_바뀐다():
    """버전 디렉터리 재발행은 경로를 바꾸지 않습니다 — 내용 해시만 잡습니다(#1088)."""
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100), ("D2", 2, 100),
    ])
    original_digests = {
        "monthly_taxi_trip": "a" * 64,
        "driver_vehicle_monthly_snapshot": "b" * 64,
        "lease_vehicle_inventory": "c" * 64,
        "fuel_price": "d" * 64,
    }
    republished_digests = {**original_digests, "monthly_taxi_trip": "f" * 64}

    original = postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05", input_digests=original_digests
    )

    assert original == postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05", input_digests=original_digests
    )
    assert original != postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05", input_digests=republished_digests
    )


def test_계산_상수가_바뀌면_fingerprint도_바뀐다():
    frames = _grain_frames([
        ("D1", 1, -1), ("D2", 1, -1),
        ("D1", 2, 100), ("D2", 2, 100),
    ])
    original = postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05", algorithm_constants_digest="constants-v1"
    )

    assert original != postgres_loader._gold_load_fingerprint(
        frames, "NYC", "2026-05", algorithm_constants_digest="constants-v2"
    )


def test_상수_digest는_실제_상수값에서_계산된다():
    """거리대 구간을 바꾸면 digest 도 함께 바뀌는지 — 단일 출처 계약(#1088)."""
    from main.spark.jobs.silver_to_gold import transformer as gold_transformer

    first = gold_transformer.algorithm_constants_digest()

    assert len(first) == 64
    monkey_patches = pytest.MonkeyPatch()
    try:
        monkey_patches.setattr(
            gold_transformer, "_DISTANCE_BAND_EDGES",
            ((3.0, "0-3"), (5.0, "3-5"), (10.0, "5-10"), (20.0, "10-20")),
        )
        second = gold_transformer.algorithm_constants_digest()
    finally:
        monkey_patches.undo()

    assert first != second


def test_기록한_config_hash가_실제입력추천설정과_다르면_적재전에_실패한다(
    monkeypatch,
):
    frames = _grain_frames([("D1", 1, -1), ("D2", 1, -1)])
    frames["silver_lineage"].loc[0, "config_hash"] = "wrong"
    monkeypatch.setattr(
        postgres_loader.psycopg2,
        "connect",
        lambda dsn: pytest.fail("config_hash 검증 전에 DB에 연결했습니다"),
    )

    with pytest.raises(ValueError, match="config_hash.*일치하지 않습니다"):
        postgres_loader.write_gold_to_postgres(
            frames, "postgresql://gold", "NYC", "2026-05"
        )


def test_Gold_적재가_성공한_버전만_메타데이터에_기록한다():
    class Cursor:
        def __init__(self):
            self.executions = []

        def execute(self, sql, parameters):
            self.executions.append((" ".join(sql.split()), parameters))

    cursor = Cursor()
    postgres_loader._record_gold_version(
        cursor, "NYC", "2026-05", 3, "fingerprint"
    )

    assert cursor.executions == [
        (
            "INSERT INTO gold_load_versions "
            "(service_area, year_month, version, load_fingerprint) "
            "VALUES (%s, %s, %s, %s)",
            ("NYC", "2026-05", 3, "fingerprint"),
        )
    ]


def test_Gold_행과_버전_메타데이터를_같은_트랜잭션에_기록한다(monkeypatch):
    class Cursor:
        def __init__(self):
            self.executions = []
            self.last_sql = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, parameters=None):
            self.last_sql = " ".join(sql.split())
            self.executions.append((self.last_sql, parameters))

        def fetchone(self):
            if self.last_sql.startswith("SELECT version"):
                return None
            if self.last_sql.startswith("SELECT COUNT"):
                return (1,) if "FROM silver_lineage" in self.last_sql else (2,)
            raise AssertionError(self.last_sql)

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def cursor(self):
            return self.cursor_instance

        def close(self):
            pass

    connection = Connection()
    monkeypatch.setattr(postgres_loader.psycopg2, "connect", lambda dsn: connection)
    monkeypatch.setattr(postgres_loader.psycopg2.extras, "execute_values", lambda *args: None)
    frames = _grain_frames([("D1", 1, -1), ("D2", 1, -1)])

    postgres_loader.write_gold_to_postgres(
        frames,
        "postgresql://gold",
        "NYC",
        "2026-05",
    )

    assert any(
        sql.startswith("INSERT INTO gold_load_versions")
        for sql, _ in connection.cursor_instance.executions
    )

    sqls = [sql for sql, _ in connection.cursor_instance.executions]
    lock_index = sqls.index("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))")
    version_lookup_index = next(
        index
        for index, sql in enumerate(sqls)
        if sql.startswith("SELECT version")
    )
    # 버전 조회 전에 잠금을 걸어야 동시 실행이 같은 다음 버전을 보지 않는다(#1056).
    assert lock_index < version_lookup_index


def test_커밋후_같은실행을_재시도하면_기존버전을_재사용한다(monkeypatch):
    """첫 호출의 commit 뒤 성공 응답만 유실된 상황은 DB에 성공 상태가 남은 것과 같다.
    같은 요청을 다시 호출해 bulk insert가 늘지 않는지 확인한다."""
    class Database:
        def __init__(self):
            self.versions = {}
            self.latest_versions = {}
            self.bulk_insert_calls = 0

    class Cursor:
        def __init__(self, database):
            self.database = database
            self.result = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, parameters=None):
            normalized = " ".join(sql.split())
            self.result = None
            if normalized.startswith("SELECT version FROM gold_load_versions"):
                self.result = self.database.versions.get(parameters)
            elif normalized.startswith("SELECT version FROM driver_aggregation"):
                self.result = self.database.latest_versions.get(parameters)
            elif normalized.startswith("SELECT COUNT"):
                self.result = 1 if "FROM silver_lineage" in normalized else 2
            elif normalized.startswith("INSERT INTO gold_load_versions"):
                service_area, year_month, version, fingerprint = parameters
                self.database.versions[
                    (service_area, year_month, fingerprint)
                ] = version
                self.database.latest_versions[(service_area, year_month)] = version

        def fetchone(self):
            return (self.result,) if self.result is not None else None

    class Connection:
        def __init__(self, database):
            self.database = database

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def cursor(self):
            return Cursor(self.database)

        def close(self):
            pass

    database = Database()
    monkeypatch.setattr(
        postgres_loader.psycopg2,
        "connect",
        lambda dsn: Connection(database),
    )

    def record_bulk_insert(*args):
        database.bulk_insert_calls += 1

    monkeypatch.setattr(
        postgres_loader.psycopg2.extras,
        "execute_values",
        record_bulk_insert,
    )
    frames = _grain_frames([("D1", 1, -1), ("D2", 1, -1)])

    first = postgres_loader.write_gold_to_postgres(
        frames, "postgresql://gold", "NYC", "2026-05"
    )
    retry = postgres_loader.write_gold_to_postgres(
        frames, "postgresql://gold", "NYC", "2026-05"
    )

    assert retry == first == {
        "driver_aggregation": 2,
        "driver_car_suggestion": 2,
        "silver_lineage": 1,
    }
    assert database.bulk_insert_calls == 3
    assert list(database.versions.values()) == [1]

    changed_input = {name: frame.copy() for name, frame in frames.items()}
    changed_input["silver_lineage"].loc[
        0, "silver_monthly_taxi_trip_s3_link"
    ] = "s3://silver/trips/v2"
    changed_input["silver_lineage"].loc[
        0, "config_hash"
    ] = postgres_loader._gold_load_fingerprint(
        changed_input, "NYC", "2026-05"
    )
    postgres_loader.write_gold_to_postgres(
        changed_input, "postgresql://gold", "NYC", "2026-05"
    )

    assert database.bulk_insert_calls == 6
    assert sorted(database.versions.values()) == [1, 2]


def test_기존_Gold_버전은_legacy_key로_백필한뒤_unique_제약을_건다():
    migration = (
        Path(__file__).resolve().parents[1]
        / "jobs/silver_to_gold/migrations/2026-08-26_add_gold_load_fingerprint.sql"
    ).read_text()

    assert "SET load_fingerprint = 'legacy-version:' || version::TEXT" in migration
    assert "ALTER COLUMN load_fingerprint SET NOT NULL" in migration
    assert "gold_load_versions_load_fingerprint_key" in migration
    assert "UNIQUE (service_area, year_month, load_fingerprint)" in migration


def test_기존_SilverLineage는_legacy식별자로_백필한뒤_NOT_NULL을_건다():
    migration = (
        Path(__file__).resolve().parents[1]
        / "jobs/silver_to_gold/migrations/2026-08-26_add_gold_lineage_execution_metadata.sql"
    ).read_text()

    for column in ("airflow_run_id", "code_sha", "config_hash"):
        assert f"ADD COLUMN IF NOT EXISTS {column} TEXT" in migration
        assert f"ALTER COLUMN {column} SET NOT NULL" in migration
    assert "legacy__' || service_area" in migration
    assert "code_sha = 'legacy-unknown'" in migration
    assert "config_hash = 'legacy-config:'" in migration
