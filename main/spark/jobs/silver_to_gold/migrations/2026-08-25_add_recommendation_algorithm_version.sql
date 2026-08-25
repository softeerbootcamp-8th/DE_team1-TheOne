-- driver_car_suggestion 에 recommendation_algorithm_version_id 를 추가하고 PK 에
-- 포함시킵니다. recommendation_algorithm 마스터 테이블을 새로 만들고 초기 알고리즘
-- 버전을 시드합니다. (#986)
--
-- 왜 손으로 실행해야 하나:
--   postgres_loader._create_table_sql() 은 CREATE TABLE IF NOT EXISTS 라서 이미
--   배포된 driver_car_suggestion 에는 no-op 입니다. dataclass 에 컬럼을 더해도 실제
--   컬럼이 생기지 않고, PRIMARY KEY 변경은 더더욱 불가능합니다. silver_lineage 는
--   신규 테이블이라 postgres_loader 가 다음 실행에서 자동 생성하므로 여기서 다루지
--   않습니다. recommendation_algorithm 은 Gold 파이프라인이 적재하지 않는 수동
--   마스터 테이블이라 여기서 직접 만들고 시드합니다.
--
-- 실행 방법과 순서는 docs/GOLD_DB_MIGRATION.md 를 보세요.
--
-- 기존 행은 전부 지금 배포된 유일한 알고리즘(#927 재고 기반 배정, #955 매출 우선
-- 정렬)이 만든 것이므로 1 로 백필합니다.

BEGIN;

-- 1) driver_car_suggestion 에 컬럼 추가. NOT NULL 을 바로 걸면 기존 행 때문에
--    실패하므로 nullable 로 넣고 백필한 뒤 제약을 겁니다.
ALTER TABLE driver_car_suggestion
    ADD COLUMN IF NOT EXISTS recommendation_algorithm_version_id INTEGER;

-- 2) 기존 행 백필.
UPDATE driver_car_suggestion SET recommendation_algorithm_version_id = 1
WHERE recommendation_algorithm_version_id IS NULL;

-- 3) NOT NULL 확정.
ALTER TABLE driver_car_suggestion
    ALTER COLUMN recommendation_algorithm_version_id SET NOT NULL;

-- 4) PRIMARY KEY 확장. 인라인 PRIMARY KEY 의 기본 제약명은 <table>_pkey 입니다.
ALTER TABLE driver_car_suggestion DROP CONSTRAINT driver_car_suggestion_pkey;
ALTER TABLE driver_car_suggestion
    ADD PRIMARY KEY (
        service_area, year_month, version, driver_id,
        recommendation_algorithm_version_id
    );

-- 5) recommendation_algorithm 마스터 테이블 생성 — Gold 파이프라인이 적재하지
--    않으므로 여기서 직접 만들고 초기 알고리즘 버전을 시드합니다.
CREATE TABLE IF NOT EXISTS recommendation_algorithm (
    recommendation_algorithm_version_id INTEGER NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (recommendation_algorithm_version_id)
);

INSERT INTO recommendation_algorithm (recommendation_algorithm_version_id, description)
VALUES (1, '기사별 순수익 증가를 최우선. 회사 매출 증대를 2번째 조건')
ON CONFLICT (recommendation_algorithm_version_id) DO NOTHING;

COMMIT;
