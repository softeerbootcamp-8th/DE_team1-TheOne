# 스키마 드리프트 및 데이터 불량률 정책

> 기준: 현재 `main` 파이프라인 코드, 불량률 = 불량 레코드 수 / 전체 레코드 수

## 한눈에 보기

| 데이터 성격 | 대표 데이터 | 허용 정책 | 목적 |
|---|---|---|---|
| 대용량 운행 사실 데이터 | 월별 택시 운행 | 1% 이상 경고, 5% 이상 차단 | 일부 불량 행 제거 후 정상 행 활용 |
| 계산 기준 마스터 | 기사 차량 스냅샷, 리스 업체 보유 차량 | 불량 1건부터 차단 | 조인, 수익 계산 오염 방지 |
| 일별 가격 시계열 | EIA 휘발유·전력·통합 연료비 | 1% 이상 경고, 5% 이상 차단 | 소량 결측 감시와 월 단위 완결성 보장 |

### 판정 기호

| 표기 | 의미 |
|---|---|
| 통과 | 다음 단계 진행 |
| 경고 | 알림·기록 후 진행 |
| 재수집 | 원천부터 1회 다시 수집 |
| 차단 | 작업 실패 및 격리 |

## 스키마 드리프트 정책

### Raw/Bronze → Silver

| 데이터 | 계층 | 필수 컬럼 누락 | 타입 변경 | 추가 컬럼 |
|---|---|---|---|---|
| 월별 택시 운행 | Bronze | 재수집 1회 → 계속 누락 시 차단 | 변환 가능하면 통과, 변환 불가 행은 불량률에 포함 | 경고 |
| 월별 택시 운행 | Silver | 차단 | 논리 타입 불일치 시 차단 | 차단 |
| 기사 차량 스냅샷 | Bronze | 재수집 1회 → 계속 누락 시 차단 | 차단 | 경고 |
| 기사 차량 스냅샷 | Silver | 차단 | 차단 | 차단 |
| 리스 업체 보유 차량 | Bronze | 재수집 1회 → 계속 누락 시 차단 | 차단 | 경고 |
| 리스 업체 보유 차량 | Silver | 차단 | 차단 | 차단 |
| EIA 휘발유 가격 | Silver | 차단 | 차단¹ | 차단 |
| EIA 전력 가격 | Silver | 차단 | 차단¹ | 차단 |
| 통합 연료비 | Silver | 차단 | 차단¹ | 차단 |

¹ EIA 타입 검증은 운영 S3의 공통 GX 검증에서 수행한다. 로컬 검증은 컬럼명·순서·행 수·날짜 완결성을 검사한다.

### Silver → Gold

| 검사 항목 | 정책 |
|---|---|
| 필수 컬럼 누락 | 차단 |
| 입력 데이터 0행 | 차단 |
| 추가 컬럼 | 허용 |
| 입력 전체 타입 서명 | 별도 사전 검사 없음 |
| 월·조인·집계·추천 불변식 위반 | 차단 |

### 호환 예외

| 데이터 | 예외 |
|---|---|
| 월별 택시 운행 Silver | timestamp의 `ms`·`us` 차이는 같은 논리 타입으로 허용 |
| 월별 택시 운행 Silver | timestamp timezone 차이는 차단 |
| 기사 차량 스냅샷 Bronze | `snapshot_created_at: timestamp[ns]`를 `timestamp[us]` 호환으로 인정 |

## 불량률 정책

### 1%·5% 정책 경계값

| 불량률 | 판정 |
|---:|---|
| `0% 이상 ~ 1% 미만` | 통과 |
| `1% 이상 ~ 5% 미만` | 경고 후 진행 |
| `5% 이상` | 차단 |

> 임계치와 같은 값도 상위 단계에 포함한다. 예: 정확히 1%는 경고, 정확히 5%는 차단.

### 데이터별 기준

| 데이터·계층 | 필수값 범위 | 불량 행 정의 | 경고 | 차단 |
|---|---|---|---:|---:|
| 월별 택시 운행 Bronze | `on_scene_datetime` 제외 | 필수값·타입 변환·시간·값 범위·서비스 등급 중 하나 이상 위반 | 1% 이상 | 기본 5% 이상² |
| 월별 택시 운행 Silver | `on_scene_datetime` 제외 | 필수 컬럼 NULL | 없음 | 1건 이상 |
| 기사 차량 스냅샷 Bronze/Silver | `exit_date` 제외 | 필수 컬럼 NULL·빈 문자열·NaN | 없음 | 1건 이상 |
| 리스 업체 보유 차량 Bronze/Silver | 전체 컬럼 | 필수 컬럼 NULL·빈 문자열·NaN | 없음 | 1건 이상 |
| EIA 휘발유 가격 Silver | 전체 컬럼 | 필수 컬럼 NULL·빈 문자열·NaN | 1% 이상 | 5% 이상 |
| EIA 전력 가격 Silver | 전체 컬럼 | 필수 컬럼 NULL·빈 문자열·NaN | 1% 이상 | 5% 이상 |
| 통합 연료비 Silver | 전체 컬럼 | 필수 컬럼 NULL·빈 문자열·NaN | 1% 이상 | 5% 이상 |
| Silver → Gold | 해당 없음 | 비율 대신 업무 불변식으로 판정 | 없음 | 위반 1건 이상 |

² 기본값은 5%이며 Airflow Variable `hvfhv_error_threshold`로 변경할 수 있다. 경고 기준 1%는 고정이다.

### 불량 레코드 집계 방식

| 원칙 | 예시 |
|---|---|
| 셀이 아닌 행 단위로 집계 | 한 행에서 필수값 3개가 비어도 불량 1건 |
| 여러 불합격 사유의 중복 합산 금지 | NULL과 범위 오류가 함께 있어도 불량 1건 |
| 문자열 공백도 결측으로 처리 | `""`, `"   "`는 불량 |
| 실수형 NaN도 결측으로 처리 | `NaN`은 불량 |

## 데이터별 업무 규칙

| 데이터 | 검사 조건 | 처리 |
|---|---|---|
| 월별 택시 운행 | 승차 시각 ≥ 하차 시각 | 불량 행으로 집계 |
| 월별 택시 운행 | `trip_miles`가 `(0, 1000]` 범위 밖 | 불량 행으로 집계 |
| 월별 택시 운행 | `trip_time`이 `[1, 86400]` 범위 밖 | 불량 행으로 집계 |
| 월별 택시 운행 | `driver_pay`, `tips`가 `[0, 5000]` 범위 밖 | 불량 행으로 집계 |
| 월별 택시 운행 | Uber·Lyft별 허용 서비스 등급 위반 | 불량 행으로 집계 |
| 기사 차량 스냅샷 | `weekly_lease_fee ≤ 0`, `driver_id` 중복, 유효 기사 0명 | 즉시 차단 |
| 리스 업체 보유 차량 | 연식 범위 위반, 연비·리스료·재고가 0 이하, `vehicle_model_id` 중복 | 즉시 차단 |
| EIA 가격 | 해당 월의 일자 누락·중복, 기대 행 수 불일치 | 즉시 차단 |
| 통합 연료비 | `price_source != eia`, 수집 계보 누락·혼합, 월 내 `ev_price_status` 혼합 | 즉시 차단 |

> 통합 연료비의 `Preliminary`·`Interpolated` 상태는 실패가 아니라 경고 대상이다.

## 실패 결과

| 검증 결과 | `_SUCCESS` | `_QUARANTINED.json` | 다음 계층 사용 |
|---|---:|---:|---:|
| 통과 | 생성 | 제거 | 가능 |
| 경고 | 생성 | 제거 | 가능 |
| 차단 | 제거 | 실패 원인·계층·실행 ID·시각 기록 | 불가 |

## 관련 코드

| 구분 | 경로 |
|---|---|
| 공통 스키마 드리프트 분류 | `shared/aws_lambda/common/schema_validator.py` |
| 공통 불량률·GX 정책 | `shared/airflow/common/validation.py` |
| 월별 택시 Airflow 검증 | `main/airflow/scripts/monthly_taxi_trip_raw_to_silver/tasks.py` |
| 월별 택시 Spark 정제 | `main/spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/transformer.py` |
| 기사 차량 스냅샷 검증 | `main/airflow/scripts/driver_vehicle_monthly_snapshot_raw_to_silver/tasks.py` |
| 리스 업체 보유 차량 검증 | `main/airflow/scripts/lease_vehicle_inventory_raw_to_silver/tasks.py` |
| EIA 휘발유 검증 | `main/airflow/scripts/eia_gas_price_bronze_to_silver/tasks.py` |
| EIA 전력 검증 | `main/airflow/scripts/eia_electricity_price_bronze_to_silver/tasks.py` |
| 통합 연료비 검증 | `main/airflow/scripts/eia_fuel_price_silver/tasks.py` |
| Silver → Gold 입력 검증 | `main/spark/jobs/silver_to_gold/transformer.py` |
