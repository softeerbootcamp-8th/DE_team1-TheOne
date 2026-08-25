-- Gold 버전 생성 시각을 기록해 90일 보존 기간을 판정합니다. (#1010)
--
-- 기존 Gold 행에는 적재 시각이 없으므로 이 마이그레이션 실행 시각으로 백필합니다.
-- 실제 생성 시각을 추측해 즉시 삭제하는 것보다 기존 버전을 90일 더 보존하는 쪽을
-- 택했습니다. 코드 배포 전에 실행 방법은 docs/GOLD_DB_MIGRATION.md를 확인합니다.

BEGIN;

CREATE TABLE IF NOT EXISTS gold_load_versions (
    service_area TEXT NOT NULL,
    year_month TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (service_area, year_month, version)
);

INSERT INTO gold_load_versions (service_area, year_month, version)
SELECT DISTINCT service_area, year_month, version
FROM driver_aggregation
ON CONFLICT (service_area, year_month, version) DO NOTHING;

COMMIT;
