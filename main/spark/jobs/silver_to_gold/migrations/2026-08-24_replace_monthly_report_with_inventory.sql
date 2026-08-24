-- postgres_loader는 기존 테이블을 ALTER하지 않으므로 새 코드 배포 전에 수동 실행합니다.
-- monthly_report와 추천 뷰의 기존 데이터는 새 Gold 계약에서 사용하지 않아 백필하지 않습니다.
BEGIN;

DROP VIEW IF EXISTS driver_car_suggestion;
DROP VIEW IF EXISTS vw_driver_car_suggestion;

ALTER TABLE driver_vehicle_profit_simulation
    DROP COLUMN IF EXISTS candidate_stock;

DROP TABLE IF EXISTS monthly_report;

CREATE TABLE IF NOT EXISTS lease_vehicle_inventory (
    version INTEGER NOT NULL,
    year_month TEXT NOT NULL,
    service_area TEXT NOT NULL,
    vehicle_model_id TEXT NOT NULL,
    manufacturer TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_year INTEGER NOT NULL,
    fuel_type TEXT NOT NULL,
    fuel_efficiency DOUBLE PRECISION NOT NULL,
    comfort_eligible BOOLEAN NOT NULL,
    extra_comfort_eligible BOOLEAN NOT NULL,
    weekly_lease_fee DOUBLE PRECISION NOT NULL,
    image_url TEXT NOT NULL,
    stock INTEGER NOT NULL,
    PRIMARY KEY (service_area, year_month, version, vehicle_model_id)
);

COMMIT;
