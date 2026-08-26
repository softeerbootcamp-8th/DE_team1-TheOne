-- driver_car_suggestion 에 threshold 를 추가하고 PK 에 포함시킵니다.
-- 알고리즘별·threshold별 최신 버전 조회용 지원 인덱스도 다시 만듭니다. (#997)
--
-- 왜 손으로 실행해야 하나:
--   postgres_loader._create_table_sql() 은 CREATE TABLE IF NOT EXISTS 라서 이미
--   배포된 driver_car_suggestion 에는 no-op 입니다. dataclass 에 컬럼을 더해도 실제
--   컬럼이 생기지 않고, PRIMARY KEY 변경은 더더욱 불가능합니다.
--
-- 실행 방법과 순서는 docs/GOLD_DB_MIGRATION.md 를 보세요.
--
-- 기존 행은 전부 threshold 를 쓰지 않는 알고리즘(v1, ProfitFirstAlgorithm)이 만든
-- 것이므로 -1(sentinel)로 백필합니다. 실제 threshold 는 항상 0 이상이라 -1 은
-- "이 알고리즘엔 threshold 축이 없다"는 뜻으로 명확히 구분됩니다.

BEGIN;

-- 1) 컬럼 추가. NOT NULL 을 바로 걸면 기존 행 때문에 실패하므로 nullable 로 넣고
--    백필한 뒤 제약을 겁니다.
ALTER TABLE driver_car_suggestion ADD COLUMN IF NOT EXISTS threshold INTEGER;

-- 2) 기존 행 백필.
UPDATE driver_car_suggestion SET threshold = -1 WHERE threshold IS NULL;

-- 3) NOT NULL 확정.
ALTER TABLE driver_car_suggestion ALTER COLUMN threshold SET NOT NULL;

-- 4) PRIMARY KEY 확장. 인라인 PRIMARY KEY 의 기본 제약명은 <table>_pkey 입니다.
ALTER TABLE driver_car_suggestion DROP CONSTRAINT driver_car_suggestion_pkey;
ALTER TABLE driver_car_suggestion
    ADD PRIMARY KEY (
        service_area, year_month, version, driver_id,
        recommendation_algorithm_version_id, threshold
    );

COMMIT;

-- 5) 알고리즘·threshold별 최신 버전 조회 지원 인덱스를 다시 만듭니다.
--    CREATE/DROP INDEX CONCURRENTLY 는 트랜잭션 블록 안에서 못 쓰므로 BEGIN/COMMIT
--    밖에 둡니다. 기존 인덱스(#987 트러블슈팅 중 수동 생성, 마이그레이션 파일로
--    남기지 않았던 것)는 threshold 가 없어 이 조회를 못 탑니다.
DROP INDEX CONCURRENTLY IF EXISTS idx_driver_car_suggestion_area_month_algorithm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_driver_car_suggestion_area_month_algorithm_threshold
    ON driver_car_suggestion (
        service_area, year_month, recommendation_algorithm_version_id, threshold, version
    );
