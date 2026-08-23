-- 사용 중지: 이 스크립트는 최종 추천 객체까지 함께 전환하는 과거 초안입니다.
-- Gold 3종 적재 시 실행하지 않습니다. 현재 적재기는 기존 driver_car_suggestion을
-- 변경하지 않고 driver_vehicle_profit_simulation을 새 물리 테이블로 생성합니다.

BEGIN;

ALTER TABLE driver_car_suggestion
    RENAME TO driver_vehicle_profit_simulation;
ALTER TABLE driver_vehicle_profit_simulation
    RENAME COLUMN vehicle_model_id TO candidate_vehicle_model_id;
ALTER TABLE driver_vehicle_profit_simulation
    ADD COLUMN candidate_stock INTEGER;

-- 기존 행은 이미 재고 검증을 통과한 최종 추천입니다. 당시 모델별 배정 수를
-- 최소 재고로 기록하면 과거 버전도 새 뷰에서 같은 추천 결과를 보존합니다.
WITH assigned AS (
    SELECT
        service_area,
        year_month,
        version,
        candidate_vehicle_model_id,
        COUNT(*)::INTEGER AS candidate_stock
    FROM driver_vehicle_profit_simulation
    GROUP BY service_area, year_month, version, candidate_vehicle_model_id
)
UPDATE driver_vehicle_profit_simulation AS simulation
SET candidate_stock = assigned.candidate_stock
FROM assigned
WHERE assigned.service_area = simulation.service_area
  AND assigned.year_month = simulation.year_month
  AND assigned.version = simulation.version
  AND assigned.candidate_vehicle_model_id = simulation.candidate_vehicle_model_id;

ALTER TABLE driver_vehicle_profit_simulation
    ALTER COLUMN candidate_stock SET NOT NULL;
ALTER TABLE driver_vehicle_profit_simulation
    DROP CONSTRAINT driver_car_suggestion_pkey;
ALTER TABLE driver_vehicle_profit_simulation
    ADD PRIMARY KEY (
        service_area,
        year_month,
        version,
        driver_id,
        candidate_vehicle_model_id
    );

CREATE VIEW vw_driver_car_suggestion AS
WITH candidate_base AS (
    SELECT
        simulation.*,
        simulation.candidate_vehicle_model_id = aggregation.vehicle_model_id
            AS is_current
    FROM driver_vehicle_profit_simulation AS simulation
    JOIN driver_aggregation AS aggregation
      ON aggregation.service_area = simulation.service_area
     AND aggregation.year_month = simulation.year_month
     AND aggregation.version = simulation.version
     AND aggregation.driver_id = simulation.driver_id
),
stock_ranked AS (
    SELECT
        candidate_base.*,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) OVER (
            PARTITION BY service_area, year_month, version,
                         candidate_vehicle_model_id
        ) AS occupied_stock,
        ROW_NUMBER() OVER (
            PARTITION BY service_area, year_month, version,
                         candidate_vehicle_model_id, is_current
            ORDER BY expected_net_profit_increase DESC,
                     expected_revenue_increase DESC,
                     driver_id ASC
        ) AS stock_rank
    FROM candidate_base
),
feasible_candidates AS (
    SELECT *
    FROM stock_ranked
    WHERE is_current
       OR stock_rank <= candidate_stock - occupied_stock
),
driver_ranked AS (
    SELECT
        feasible_candidates.*,
        ROW_NUMBER() OVER (
            PARTITION BY service_area, year_month, version, driver_id
            ORDER BY expected_monthly_net_profit DESC,
                     is_current DESC,
                     model_year DESC,
                     candidate_vehicle_model_id ASC
        ) AS driver_rank
    FROM feasible_candidates
)
SELECT
    version,
    driver_id,
    year_month,
    service_area,
    comfort_eligible,
    extra_comfort_eligible,
    candidate_vehicle_model_id AS vehicle_model_id,
    manufacturer,
    model_name,
    model_year,
    recommendation_reason,
    fuel_efficiency,
    recommended_monthly_lease_fee,
    expected_monthly_fuel_cost,
    expected_monthly_net_profit,
    expected_net_profit_increase,
    expected_revenue_increase
FROM driver_ranked
WHERE driver_rank = 1;

CREATE VIEW driver_car_suggestion AS
SELECT * FROM vw_driver_car_suggestion;

COMMIT;
