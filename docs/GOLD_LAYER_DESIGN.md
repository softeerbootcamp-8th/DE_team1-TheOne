# Gold 레이어 설계 초안

수집 소스가 대체로 확정된 시점에서, 목표(순수익 ↑ & 객단가 ↑ 상위 20명 추출)로 역산한 Gold 테이블 제안입니다.

## 현재 재료

- **운행**: HVFHV 트립 단위 실데이터 (`main/spark/jobs/bronze_to_silver/hvfhv/transformer.py` — 거리/시간/driver_pay/tips/zone/platform, `taxi_id`·`driver_id`는 매핑으로 채워짐)
- **기사**: `driver_master` (성향 — 주요 거리대·시간대·요일, 근무/휴식/공차 min-max, joined_at/churned_at)
- **회사 원장**: `customer` / `taxi` / `lease_contract` (`sub/sub/scripts/synthetic_company_snapshot/snapshot.py` — 차량, 주간 리스료, 등급 자격, 계약일)
- **차량 기준정보**: 카탈로그(주간 렌트료) + fueleconomy 제원(`combined_mpg`, `kwh_100mi`, `range_miles`, `atv_type`) + Uber/Lyft 배차 자격(`product`, `min_year`)
- **에너지 단가**: 휘발유 $/gal, EV $/kWh

---

## 1. `gold_dim_vehicle_option` — 추천 후보 차량 마스터
  
모든 시뮬레이션이 이 테이블 한 장을 참조하게 만드는 게 핵심입니다. 여기서 4개 소스(카탈로그·제원·Uber·Lyft·에너지단가)를 한 번만 조인해 두면 아래 테이블들이 전부 단순해집니다.

**그레인**: `make_key` × `model_key` × `model_year`

| 컬럼 | 비고 |
|---|---|
| `make_key`, `model_key`, `model_year` | 조인 키 |
| `weekly_price_usd`, `vendor` | 리스료 = 회사 매출 |
| `fuel_type` (GAS/HYBRID/PHEV/EV) | `atv_type` 정규화 |
| `combined_mpg`, `kwh_100mi`, `range_miles` | 제원 |
| `energy_cost_per_mile_usd` | **핵심 파생** — 아래 계산식 |
| `uber_product`, `uber_min_year`, `lyft_product`, `lyft_min_year` | 자격 |
| `is_uber_comfort_eligible`, `is_lyft_extra_comfort_eligible`, `tier_rank` | 등급 상승 판정 |
| `spec_match_level` (MODEL/BASE_MODEL/NONE) | `base_model_key` 폴백 여부 — 제원 결측 추천을 거르는 데 필요 |
| `energy_price_date`, `catalog_collected_date` | 단가 시점 |

## 2. `gold_fct_driver_weekly` — 기사 주간 운행·수익

**그레인**: `driver_id` × `week_start` (월요일)

| 그룹 | 컬럼 |
|---|---|
| 운행량 | `trip_count`, `active_days`, `total_miles`, `total_trip_hours`, `avg_trip_miles`, `miles_per_active_day` |
| 매출 | `driver_pay_usd`, `tips_usd`, `gross_earnings_usd`, `earnings_per_mile`, `earnings_per_hour` |
| 비용 | `energy_cost_usd`, `lease_cost_usd`, `tolls_usd` |
| 순수익 | `net_earnings_usd`, `net_per_hour` |
| 성향 | `uber_trip_share`, `lyft_trip_share`, `airport_trip_share`, `top_pickup_borough`, `night_trip_share` |
| 차량 | `taxi_id`, `make_key`, `model_key`, `model_year` (그 주에 타던 차 = SCD 스냅샷) |
| 상태 | `is_churned`, `tenure_days` |

여기에 `net_earnings_4w_median` 같은 롤링 컬럼을 같이 넣길 권합니다. 주 단위 편차가 커서 **단일 주로 추천하면 매주 대상자가 뒤집힙니다.**

## 3. `gold_fct_vehicle_swap_sim` — 기사 × 후보차량 손익 시뮬레이션

추천의 실체. 2번 × 1번의 곱집합(자격 필터 후)입니다.

**그레인**: `driver_id` × 후보 `make_key`/`model_key`/`model_year` × `week_start`

| 컬럼 | 의미 |
|---|---|
| `current_*` (net, lease, energy, tier) | 현재 차량 기준선 |
| `sim_energy_cost_usd` | 같은 주행거리에 후보차 연비 적용 |
| `sim_gross_earnings_usd` | 등급 상승 시 요금 프리미엄 반영 |
| `sim_lease_cost_usd` | 후보차 주간 렌트료 |
| `sim_net_earnings_usd` | 시뮬 순수익 |
| **`driver_net_gain_usd`** | 기사 이득 (> 0 조건) |
| **`company_arpu_gain_usd`** | `sim_lease - current_lease` (> 0 조건) |
| `gain_from_fuel_usd`, `gain_from_tier_usd`, `cost_from_lease_usd` | **기여도 분해 — 대시보드에서 "왜"를 설명하는 컬럼** |
| `tier_upgraded`, `is_feasible`, `rank_in_driver` | 필터/정렬 |

기여도 3개를 안 넣으면 CSM이 "이 차 왜 추천됐어요?"에 답을 못 합니다. 꼭 넣습니다.

## 4. `gold_mart_top_customers` — 주간 콜 리스트 (대시보드 메인)

3번에서 `driver_net_gain > 0 AND company_arpu_gain > 0`인 행만 남기고 기사별 1위만 뽑아 상위 20명.

`week_start`, `rank`, `customer_id`, `driver_name`, `lease_id`, `current_vehicle_label`, `recommended_vehicle_label`, `driver_net_gain_usd`, `company_arpu_gain_usd`, `weekly_miles`, `primary_time_blocks`, `primary_distance_bands`, `tenure_days`, `reason_text`

## 5. `gold_mart_kpi_weekly` — 상단 요약 카드 1행

`week_start`, `target_customer_count`, **`total_arpu_gain_usd`** (핵심 지표), `avg_driver_net_gain_usd`, `tier_upgrade_count`, `ev_switch_count`, `avg_fleet_net_earnings_usd`, `active_driver_count`, `churn_rate`

---

## 파생 컬럼 계산식

```
energy_cost_per_mile =
  내연/하이브리드 : gas_price / combined_mpg
  EV              : (kwh_100mi / 100) * ev_price

net_earnings      = driver_pay + tips - energy_cost - lease_cost
company_arpu_gain = Σ(new weekly_price - old weekly_price)
```

`tolls`는 순수익에서 빼지 않습니다 — HVFHV의 `tolls`는 승객 요금 항목이라 기사 부담이 아닙니다. 넣더라도 별도 컬럼으로만 두고 계산식에서는 제외합니다.

---

## 지금 결정해야 할 구멍 3개

1. **등급 프리미엄 계수.** HVFHV 원본에 상품 등급(Comfort/Extra Comfort) 컬럼이 없습니다. 즉 "Comfort로 올리면 매출이 몇 % 오르는가"를 실데이터에서 뽑을 수 없습니다. 상수 가정(예: mile당 요금 1.2배)으로 두고 `docs/decision_making`에 근거를 남기는 것을 권합니다. 이 값이 추천 결과 전체를 좌우합니다.
2. **가격 시점 정합.** 트립은 2024년 실데이터, 가스·EV·렌트료는 최신 스냅샷 1건입니다. 주차별로 조인하지 말고 **최신 단가 1건을 상수로 적용**하고 `energy_price_date`를 남깁니다. 주차별 조인을 시도하면 대부분 NULL이 됩니다.
3. **추천 안정성.** 2번의 `net_earnings_4w_median`을 시뮬 기준선으로 쓸지, 최근 1주를 쓸지. 4주 중앙값 권장 — 아니면 매주 콜 리스트가 통째로 바뀌어 CSM이 쓸 수 없습니다.

1·2·3번 테이블만 있으면 4·5번은 뷰로도 만들 수 있습니다. 물리 테이블 3장 + 마트 2장은 얇게 가는 구조를 권합니다.
