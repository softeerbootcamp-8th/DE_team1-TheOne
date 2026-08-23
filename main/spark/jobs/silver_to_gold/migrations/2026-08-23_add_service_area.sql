-- Gold 3종에 service_area 컬럼을 추가하고 PRIMARY KEY 를 확장합니다. (#809, #674)
--
-- 왜 손으로 실행해야 하나:
--   postgres_loader._create_table_sql() 은 CREATE TABLE IF NOT EXISTS 라서 **이미
--   배포된 테이블에는 no-op** 입니다. dataclass 에 컬럼을 더해도 실제 컬럼이 생기지
--   않고, PRIMARY KEY 변경은 더더욱 불가능합니다. 그래서 코드 배포 **전에** 이
--   스크립트를 수동으로 실행해야 합니다. 안 하면 첫 운영 실행이 INSERT 단계에서
--   psycopg2 UndefinedColumn 으로 죽습니다(요란해서 데이터가 틀리지는 않습니다).
--
-- 실행 방법과 순서는 docs/GOLD_DB_MIGRATION.md 를 보세요.
--
-- 기존 행은 전부 뉴욕 데이터이므로 'NYC' 로 백필합니다. 지금 서비스 지역이 하나뿐인
-- 것이 이 가정의 근거입니다 — 두 번째 지역이 들어온 뒤에 이 스크립트를 돌리면
-- 안 됩니다.

BEGIN;

-- 1) 컬럼 추가. NOT NULL 을 바로 걸면 기존 행 때문에 실패하므로 nullable 로 넣고
--    백필한 뒤 제약을 겁니다.
ALTER TABLE monthly_report        ADD COLUMN IF NOT EXISTS service_area TEXT;
ALTER TABLE driver_aggregation    ADD COLUMN IF NOT EXISTS service_area TEXT;
ALTER TABLE driver_car_suggestion ADD COLUMN IF NOT EXISTS service_area TEXT;

-- 2) 기존 행 백필.
UPDATE monthly_report        SET service_area = 'NYC' WHERE service_area IS NULL;
UPDATE driver_aggregation    SET service_area = 'NYC' WHERE service_area IS NULL;
UPDATE driver_car_suggestion SET service_area = 'NYC' WHERE service_area IS NULL;

-- 3) NOT NULL 확정. 적재 코드가 항상 값을 넣으므로 nullable 로 남기면 조용히 NULL
--    지역 행이 생길 수 있습니다.
ALTER TABLE monthly_report        ALTER COLUMN service_area SET NOT NULL;
ALTER TABLE driver_aggregation    ALTER COLUMN service_area SET NOT NULL;
ALTER TABLE driver_car_suggestion ALTER COLUMN service_area SET NOT NULL;

-- 4) PRIMARY KEY 확장. service_area 가 PK 에 없으면 두 지역의 같은
--    (year_month, version) 행이 충돌합니다. driver_id 도 지역 간 유니크하지
--    않으므로(#805) 지역이 자연 키의 일부여야 합니다.
--    인라인 PRIMARY KEY 의 기본 제약명은 <table>_pkey 입니다.
ALTER TABLE monthly_report        DROP CONSTRAINT monthly_report_pkey;
ALTER TABLE monthly_report        ADD PRIMARY KEY (service_area, year_month, version);

ALTER TABLE driver_aggregation    DROP CONSTRAINT driver_aggregation_pkey;
ALTER TABLE driver_aggregation
    ADD PRIMARY KEY (service_area, year_month, version, driver_id);

ALTER TABLE driver_car_suggestion DROP CONSTRAINT driver_car_suggestion_pkey;
ALTER TABLE driver_car_suggestion
    ADD PRIMARY KEY (service_area, year_month, version, driver_id);

COMMIT;
