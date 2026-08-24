# 데이터 모델 레퍼런스

모든 계층의 스키마는 [`schema/`](../schema/) 한 곳이 소유합니다.
생산자(Spark job · Lambda 핸들러)와 소비자(검증 태스크 · 하류 job)가 각자 컬럼 목록을 들고 있으면
상류가 컬럼을 늘렸을 때 한쪽만 뒤처지고, 그 어긋남은 **적재 실패가 아니라 Gold의 조용한 NULL**로 드러납니다.

이 문서는 계층별 데이터셋의 **한 행의 의미 · 파티션 · 키 · 소유 파일**을 한눈에 보는 표입니다.
컬럼 단위 정의는 각 소유 파일의 docstring이 정본입니다.

- [1. 회사 원천 시스템](#1-회사-원천-시스템)
- [2. Bronze](#2-bronze)
- [3. Silver](#3-silver)
- [4. Gold](#4-gold)
- [5. 스키마 규칙](#5-스키마-규칙)

---

## 1. 회사 원천 시스템

가상 리스 업체의 내부 원장입니다. 메인 파이프라인은 이 테이블을 직접 읽지 않고,
원천 시스템이 **월 1회 릴리스로 게시하는 2개 데이터셋**만 HTTP API로 받아 갑니다.

### 1.1 내부 원장 (파이프라인 비공개)

| 데이터셋 | 한 행 | 파티션 | 규모 | 생성 |
| --- | --- | --- | --- | --- |
| `customer` | 고객 1명 | `snapshot_date` | 2,000행 | [snapshot.py](../sub/generators/synthetic_company_snapshot/snapshot.py) |
| `taxi` | 차량 1대 | `snapshot_date` | 2,000행 | 〃 |
| `lease_contract` | 계약 1건 | `snapshot_date` | 2,000행 | 〃 |

`snapshot_date` 는 "그 시점의 회사 상태"를 뜻합니다.
리스 시작일이 `[lease_start_min, snapshot_date]` 에서 추첨되므로, 이 값이 곧 **생성 가능한 첫 달**입니다.
기본값은 [snapshot.py](../sub/generators/synthetic_company_snapshot/snapshot.py) 의 `DEFAULT_SNAPSHOT_DATE` 한 곳이 소유합니다.

### 1.2 릴리스 (API 공개)

| 데이터셋 | 한 행 | 파티션 | 규모 | 소유 스키마 |
| --- | --- | --- | --- | --- |
| `monthly_taxi_trip` | 운행 1건 | `year_month` | 월 2,040만 행 | [schema/bronze/__init__.py](../schema/bronze/__init__.py) |
| `driver_vehicle_leases` | 계약 1건 | `year_month` | 2,000행 | [schema/silver/driver_vehicle_leases.py](../schema/silver/driver_vehicle_leases.py) |
| `lease_vehicle_inventory` | 차종·연식별 재고 | `year_month` | 차종 수준 | [schema/silver/lease_vehicle_inventory.py](../schema/silver/lease_vehicle_inventory.py) |

세 데이터셋은 `/v1/data/{YYYY-MM}/datasets/{dataset}`에서 Parquet 파일로 공개됩니다. 원천의 `manifest.json`은 게시 전 내부 검증용이며 메인 수집 계약으로 노출하지 않습니다.

---

## 2. Bronze

원본을 **변형 없이** 적재합니다. 정제는 하지 않습니다.

| 데이터셋 | 원천 | 한 행 | 파티션 | 소유 스키마 |
| --- | --- | --- | --- | --- |
| `monthly_taxi_trip` | 원천 API | 운행 1건 | `year_month` | [bronze/__init__.py](../schema/bronze/__init__.py) |
| `driver_vehicle_leases` | 원천 API | 계약 1건 | `year_month` | [silver/driver_vehicle_leases.py](../schema/silver/driver_vehicle_leases.py) |
| `lease_vehicle_inventory` | 원천 API | 차종 × 연식 1개 | `year_month` | [silver/lease_vehicle_inventory.py](../schema/silver/lease_vehicle_inventory.py) |
| `vehicle_catalog` | FastTrackLease | 차종 1개 | `collected_date` | [bronze/vehicle_catalog.py](../schema/bronze/vehicle_catalog.py) |
| `uber_eligible_vehicles` | Uber | 차종 1개 | `collected_date` | [bronze/uber_eligible_vehicles.py](../schema/bronze/uber_eligible_vehicles.py) |
| `lyft_eligible_vehicles` | Lyft | 차종 1개 | `collected_date` | [bronze/lyft_eligible_vehicles.py](../schema/bronze/lyft_eligible_vehicles.py) |
| `fueleconomy_vehicle_specs` | FuelEconomy.gov | 차종 트림 1개 | `collected_date` | — (원본 CSV 스키마 그대로) |
| `gas_price` | EIA | 주 1건 | `collected_date` | — |
| `ev_charging_stations` | EIA | 월 1건 | `collected_date` | — |

**파티셔닝 규칙**

전 계층이 **Hive-style `key=value`** 디렉터리로 나뉩니다. 키 이름은 데이터셋마다 다르지만 두 계열뿐입니다.

| 계열 | 실제 키 | 의미 | 쓰는 곳 |
| --- | --- | --- | --- |
| **기간** | `year_month` · `collected_month` · `price_date` | 데이터가 가리키는 시점 | 운행·리스·연료비처럼 "몇 월 데이터인가"가 정체성인 것 |
| **관측 시점** | `collected_date` · `snapshot_date` | 그 값을 본 시점 | 카탈로그·자격·제원·회사 원장처럼 "언제 본 상태인가"가 정체성인 것 |

기준정보는 시점마다 값이 달라지므로 관측일로 쌓고, 하류가 *"대상 월 이하의 최신 파티션"* 을 골라 읽습니다.

원천 API의 월별 Bronze 3종은 데이터 기준 월 아래에 실제 수집 시각 디렉터리를 append합니다. 같은 월을 다시 받아도 원본 이력이 유지됩니다.

```text
data/bronze/<dataset>/year_month=YYYY-MM/collected_at=YYYYMMDDTHHMMSSffffffZ/data.parquet
data/bronze/<dataset>/year_month=YYYY-MM/collected_at=YYYYMMDDTHHMMSSffffffZ/_SUCCESS
```

writer는 최종 디렉터리에 원본을 쓰고 Airflow 검증이 끝난 뒤 `_SUCCESS`를 기록합니다.
동일 원본 재사용과 downstream 최신 수집본 선택은 marker가 있는 버전만 대상으로 합니다.

원천 API 3종의 Silver는 Bronze 수집 시각을 `source_collected_at` 자연 키로 보존합니다.

```text
data/silver/monthly_taxi_trip/year_month=YYYY-MM/
└── source_collected_at=YYYYMMDDTHHMMSSffffffZ/
    ├── part-*.parquet
    └── _SUCCESS

data/silver/{driver_vehicle_monthly_snapshot,lease_vehicle_inventory}/year_month=YYYY-MM/
└── source_collected_at=YYYYMMDDTHHMMSSffffffZ/
    ├── data.parquet
    └── _SUCCESS
```

Spark/Lambda writer는 최종 `source_collected_at=.../`에 직접 쓰되 기존 `_SUCCESS`를
먼저 제거합니다. Airflow가 스키마·행 수·품질을 검증한 뒤 `_SUCCESS`를 기록합니다.
Gold는 `_SUCCESS`가 있는 최신 버전만 읽습니다. 같은 Bronze 수집본의
재시도는 같은 최종 경로를 교체하므로 파일 관점에서 멱등하고, 이전 수집 버전은 남습니다.

`monthly_taxi_trip`은 월 2,040만 행을 Spark의 `part-*.parquet` 다중 파일로 적재합니다.
Lambda가 처리하는 소규모 2종만 `data.parquet` 단일 파일을 사용하며, 생산 파이프라인은
서로의 파일 형식을 허용하지 않습니다.

EIA Bronze의 `collected_date=`와 고정 파일명 Silver의 `year_month=`도 파티션 바로 아래
`_SUCCESS`를 둡니다. marker 없는 파티션은 Bronze→Silver·Silver 결합·Gold에서 읽지 않습니다.

그 밖의 Spark Silver 쓰기는 `partitionOverwriteMode=dynamic` 입니다
([shared/spark/common/io.py](../shared/spark/common/io.py)). 재실행하면 **해당 파티션만**
덮어쓰고 다른 달은 그대로 둡니다.

---

## 3. Silver

정제·표준화·통합 계층입니다.

| 데이터셋 | 한 행 | 파티션 | 규모 | 소유 스키마 |
| --- | --- | --- | --- | --- |
| `monthly_taxi_trip` | 운행 1건 | `year_month` | 월 2,040만 행 | [silver/__init__.py](../schema/silver/__init__.py) |
| `driver_vehicle_leases` | 계약 1건 | `year_month` | 2,000행 | [silver/driver_vehicle_leases.py](../schema/silver/driver_vehicle_leases.py) |
| `lease_vehicle_inventory` | 차종 × 연식 1개 | `year_month` | 차종 수준 | [silver/lease_vehicle_inventory.py](../schema/silver/lease_vehicle_inventory.py) |
| **`hvfhv_driver_trip`** | 운행 1건 | `year_month` | 월 2,040만 행 | [silver/hvfhv_driver_trip.py](../schema/silver/hvfhv_driver_trip.py) |
| `vehicle_catalog` | 차종 1개 | `collected_date` | 24행 | [silver/vehicle_catalog.py](../schema/silver/vehicle_catalog.py) |
| `uber_eligible_vehicles` | 차종 1개 | `collected_date` | 59,650행 | [silver/eligible_vehicles.py](../schema/silver/eligible_vehicles.py) |
| `lyft_eligible_vehicles` | 차종 1개 | `collected_date` | 1,008행 | 〃 |
| `fueleconomy_vehicle_specs` | 차종 1개 | `collected_date` | 50,242행 | [silver/vehicle_specs.py](../schema/silver/vehicle_specs.py) |
| **`vehicle_master`** | 차종 × 연식 범위 | `collected_date` | 284행 | [silver/vehicle_master.py](../schema/silver/vehicle_master.py) |
| `gas_price` | 주 1건 | `price_date` | — | [silver/gas_price.py](../schema/silver/gas_price.py) |
| `ev_charging_price` | 월 1건 | `price_date` | — | [silver/ev_charging_price.py](../schema/silver/ev_charging_price.py) |
| **`gas_ev_price`** | 월 1건 | `collected_month` | 월 1행 | [silver/gas_ev_price.py](../schema/silver/gas_ev_price.py) |
| `taxi_zone_travel_times` | 구역 쌍 | 없음 | 49,658행 | — (원천 생성 전용) |

### 3.1 수집 검증

원천 API에서 받은 Bronze는 적재 직후 실제 저장 파일을 확인합니다. 하나라도 어긋나면 태스크가 실패하고 하류로 내려가지 않습니다. ([monthly_bronze.py](../main/airflow/common/monthly_bronze.py))

| 검사 | 잡아내는 실패 |
| --- | --- |
| 파일 크기 | 다운로드 중단 |
| Parquet 가독성·footer 행 수 | 잘못된 파일 또는 부분 적재 |
| 파티션 경로 형식 | `year_month=` 계약 위반 |

### 3.2 `hvfhv_driver_trip` — 운행 × 리스 계약

운행 한 건에 **그 시점의** 리스 계약 한 건을 붙인 표입니다. Gold의 유일한 운행 입력입니다.

> 새 아키텍처에서는 이 조인을 Gold로 옮기고 이 중간 테이블을 두지 않습니다.
> 아래 내용은 **현재 코드 기준**입니다.

| 항목 | 값 |
| --- | --- |
| 한 행 | 운행 1건 (`trip_key`) |
| 키 | `trip_key`, `driver_id`, `customer_id`, `lease_id`, `taxi_id` |
| 조인 규칙 | `taxi_id` + 리스 기간 |
| 기간 조건 | `lease_started_on ≤ 운행일 < lease_ended_on` (**상한 배타적**) |

진행 중인 계약은 `lease_ended_on` 이 NULL입니다.
비교식에 NULL을 그대로 두면 식 전체가 NULL(= 거짓 취급)이 되어 **열린 계약이 아무 운행에도 안 걸립니다.**
그래서 `9999-12-31` 로 치환한 뒤 비교합니다.

컬럼 목록은 손으로 적지 않고 **상류 두 스키마에서 파생**합니다.

```python
TRIP_COLUMNS  = tuple(f.name for f in _TRIP_SCHEMA  if f.name not in TRIP_PLACEHOLDER_COLUMNS)
LEASE_COLUMNS = tuple(n for n in _LEASE_SCHEMA.names if n not in LEASE_JOIN_COLUMNS)
COLUMNS       = (*TRIP_COLUMNS, *LEASE_COLUMNS)
```

`TRIP_PLACEHOLDER_COLUMNS`(`driver_id`, `taxi_model_id`)를 빼는 이유:
HVFHV Silver가 NULL 자리표시로 들고 오는 컬럼인데 채우는 값이 리스 쪽에 있습니다.
빼지 않으면 `select` 에 같은 이름이 두 번 들어가는데, `select` 는 중복 이름을 허용해 조용히 지나가고
**쓰기 단계에서야** `COLUMN_ALREADY_EXISTS` 로 죽습니다.

### 3.3 `vehicle_master` — 추천 후보 차량 마스터

4개 원천(카탈로그 · 제원 · Uber 자격 · Lyft 자격)을 **한 번만** 조인해 두어 하류 시뮬레이션을 단순화합니다.

| 컬럼군 | 컬럼 | 쓰이는 곳 |
| --- | --- | --- |
| 식별 | `make_key`, `model_key`, `spec_year_min`, `spec_year_max` | 조인 키 |
| 매출 | `weekly_lease_fee` | 회사 렌탈 매출 |
| 연비 | `combined_mpg`, `kwh_100mi`, `fuel_type` | 연료비 계산 |
| 자격 | `uber_comfort_eligible`, `lyft_extra_comfort_eligible`, `vehicle_group` | 등급 상승 판정 |

`taxi_id` 가 없는 **차종(스펙) 테이블**입니다. 실제 보유 차량이 아니라
`(make_key, model_key, 연식)` 3개로 추천 차량을 식별합니다.

### 3.4 `estimated_service_tier` — 원천에서 확정된 운행 등급

합성 원천 API가 매칭 전에 확정한 상품 등급을 Raw→Bronze→Silver에서 그대로 보존합니다.
Silver는 운임으로 등급을 다시 추정하지 않으며 아래 license–등급 조합만 허용합니다.

| `hvfhs_license_num` | 허용 등급 |
| --- | --- |
| `HV0003` (Uber) | `Standard`, `Comfort` |
| `HV0005` (Lyft) | `Standard`, `Extra Comfort` |

---

## 4. Gold

| 데이터셋 | 한 행 | 파티션 | 규모 | 소유 스키마 |
| --- | --- | --- | --- | --- |
| `driver_aggregation` | 기사 × 월 | `year_month` | 2,000행/월 | [gold/driver_aggregation.py](../schema/gold/driver_aggregation.py) |
| `driver_vehicle_profit_simulation` | 기사 × 후보 차량 모델 × 월 | `year_month` | 기사 수 × 차량 모델 수/월 | [schema/gold](../schema/gold/__init__.py) |
| `lease_vehicle_inventory` | 차량 모델 × 월 | `year_month` | Silver 재고 모델 수/월 | [schema/gold](../schema/gold/__init__.py) |

세 물리 테이블은 기사×월, 기사×후보 차량 모델×월, 차량 모델×월을 각각 자연 키로 갖습니다.
최종 추천 객체는 이 Gold 적재 범위에서 생성하거나 변경하지 않습니다.

### 4.1 `driver_aggregation` — 기사 월간 집계

| 컬럼군 | 내용 |
| --- | --- |
| 운행 패턴 | `ratio_00_03` … `ratio_21_24` (3시간 8구간, 합 1.0) |
| 운행 구역 | `top1_zone_id`/`ratio` … `top3_*` (승차 구역 상위 3개) |
| 현재 차량 | `current_taxi_id`, `current_make_key`, `current_model_key` |
| 수익 | 월 총수입 · 연료비 · 렌트료 · 순수익 |

`current_make_key`/`current_model_key` 를 함께 싣는 이유: `taxi_id` 만으로는 사람이 무슨 차인지 알 수 없어
콜 리스트에서 *"지금 〈현재 차량〉 타시는데 〈추천 차량〉 으로"* 를 못 씁니다.

### 4.2 `driver_vehicle_profit_simulation` — 후보 차량 수익 시뮬레이션

| 컬럼 | 내용 |
| --- | --- |
| `candidate_vehicle_model_id` / `model_year` | 평가한 후보 차량 (연식은 스펙 트림 범위 중 최신) |
| `recommendation_reason` | `연비` / `차량등급` / `더 저렴한 렌트료` 중 해당 항목을 `, ` 로 나열. 셋 다 아니면 `현재 차량 유지` |
| `expected_net_profit_increase` | 기사 예상 순수익 증가액 |
| `expected_revenue_increase` | 회사 렌탈 객단가 증가액 |

`recommendation_reason` 이 없으면 CSM이 *"이 차 왜 추천됐어요?"* 에 답을 못 합니다.

### 4.3 `lease_vehicle_inventory` — 월별 리스 차량 재고

Silver `lease_vehicle_inventory`의 업무 컬럼과 행을 그대로 보존하고 Gold 공통 키
(`version`, `service_area`, `year_month`)만 추가합니다. `stock`은 수익 시뮬레이션이나
추천 선택에 사용하지 않으며 소비 계층이 필요할 때 별도로 조인합니다.

`assignment_version` 은 **없습니다**(#471). 기사-운행 매칭이 원천 API로 옮겨가면서
Silver는 `taxi_id` + 리스 기간으로 결정적으로 조인만 하므로, 같은 입력이면 같은 결과입니다 — 구분할 버전이 생기지 않습니다.

---

## 5. 스키마 규칙

### 5.1 소유권은 한 곳

생산자와 소비자가 같은 파일을 import 합니다. 컬럼을 양쪽에서 세지 않습니다.

```python
# spark/jobs/driver_trip/transformer.py — 생산자
from schema.silver.hvfhv_driver_trip import LEASE_COLUMNS, TRIP_COLUMNS

# airflow/scripts/hvfhv_driver_trip_silver/tasks.py — 소비자(검증)
from schema.silver.hvfhv_driver_trip import REQUIRED_COLUMNS
```

### 5.2 파생 테이블은 컬럼을 다시 적지 않는다

상류 스키마에서 유도합니다. 손으로 적으면 상류가 컬럼을 늘렸을 때 이 파일만 뒤처지고,
그 어긋남은 적재 실패가 아니라 **Gold의 빈 값**으로 드러납니다.

### 5.3 전체 계약과 필수 컬럼을 구분한다

| 상수 | 역할 |
| --- | --- |
| `COLUMNS` | 출력 전체 계약 |
| `KEY_COLUMNS` | 행 식별 + 하류 조인 성립 |
| `REQUIRED_COLUMNS` | 적재 후 검증이 **반드시** 확인할 것 (틀렸을 때 하류에서 조용히 새는 값만) |

검증이 전체 컬럼을 다 보면 컬럼 하나 추가에도 검증이 깨져 실제 계약 위반과 구분되지 않습니다.

### 5.4 스키마 드리프트는 수집 시점에 잡는다

외부 원천은 예고 없이 컬럼을 바꿉니다. 수집 시점에 기대 스키마와 대조해
**누락 / 타입 불일치 / 신규 추가**를 구분해 보고합니다. ([schema_validator.py](../shared/lambda_runtime/common/schema_validator.py))

```
❌ 누락된 컬럼: `combined_mpg` (기대 타입: `double`)
⚠️ 타입 불일치 컬럼 `model_year`: 기대=`int32`, 실제=`string`
➕ 신규 추가된 컬럼: `range_miles_epa` (`double`)
```

누락·타입 불일치는 파이프라인을 세우고, 신규 추가는 로그로만 남깁니다 —
원천이 컬럼을 더한 것만으로 수집을 멈출 이유는 없습니다.
