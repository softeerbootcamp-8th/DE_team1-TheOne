# 파이프라인 계층·DAG 의존성·버전 계약

- 요약
  - 계층 : Bronze(원본, S3, Lambda) → Silver(정제, S3, Lambda 또는 Spark) → Gold(집계·추천, RDS, Spark)
  - DAG 의존성 : Airflow Asset으로 체이닝. 커스텀 센서·브랜치 없이 파티션 키(`{service_area}:{year_month}`) 단위로 자동 트리거
  - 버전 계약 : Bronze `collected_at`, Silver `source_collected_at`, Gold `version`+`load_fingerprint`. 같은 입력이면 fingerprint가 같아 재실행해도 버전이 늘지 않음

## 계층 구조

| 데이터셋 | 계층 | 계산 런타임 | 저장 위치 |
| --- | --- | --- | --- |
| `monthly_taxi_trip`, `driver_vehicle_monthly_snapshot`, `lease_vehicle_inventory`, `eia_gas_price`, `eia_electricity_price` | Bronze | Lambda | S3 (`bronze/{dataset}/service_area=*/year_month=*/collected_at=*/data.parquet`) |
| `monthly_taxi_trip` | Silver | **Spark**(EMR) | S3 (`.../source_collected_at=*/`) |
| `driver_vehicle_monthly_snapshot`, `lease_vehicle_inventory`, `eia_gas_price`, `eia_electricity_price`, `fuel_price`(=`gas_ev_price`) | Silver | Lambda | S3 |
| `driver_aggregation`, `driver_car_suggestion`, `silver_lineage`, `gold_load_versions` | Gold | Spark(EMR) | RDS Postgres |

Silver 런타임은 데이터 규모로 가른다: `monthly_taxi_trip`(월 70~90만 행)만 Spark, 나머지(12~2,000행)는 Lambda. Gold는 4개 Silver를 한 번에 읽어야 해서 Spark 한 실행으로 처리.

Gold 추천 알고리즘(v1/v2) 흐름은 [GOLD_RECOMMENDATION_LOGIC.md](./GOLD_RECOMMENDATION_LOGIC.md) 참고.

## DAG 의존성

`main/airflow/dags/` 8개:

| DAG | 트리거 |
| --- | --- |
| `source_api_refresh_dag.py` | cron `0 3 * * *` — 원천 3종 변경 여부 HEAD 확인 |
| `monthly_taxi_trip_raw_to_silver_dag.py`, `driver_vehicle_monthly_snapshot_raw_to_silver_dag.py`, `lease_vehicle_inventory_raw_to_silver_dag.py` | `schedule=None` — `source_api_refresh`가 `TriggerDagRunOperator`로 실행 |
| `eia_gas_price_raw_to_silver_dag.py`, `eia_electricity_price_raw_to_silver_dag.py` | cron (매월 1일) |
| `eia_fuel_price_silver_dag.py` | cron + `ExternalTaskSensor`로 위 두 DAG `validate_silver` 대기 |
| `monthly_taxi_trip_silver_to_gold_dag.py` | Airflow Asset |
| `data_lifecycle_dag.py` | cron `0 3 * * *` — 오래된 버전 정리 |

원천 3종 Raw→Silver DAG는 `source_api_refresh_pipeline`이 `TriggerDagRunOperator`로 실행시킨다. 셋 다 끝나면 `publish_api_refresh_ready_task`가 Asset 이벤트를 낸다.

Gold DAG는 Asset 파티션 스케줄로 실행된다. 파티션 키는 `"{service_area}:{year_month}"` — Airflow Asset 파티션 키가 문자열 하나만 받는 제약 때문에 지역·월을 한 문자열로 합쳤다. 지역이 늘어도 DAG를 새로 안 만든다(#674).

```python
GOLD_INPUTS = (
    (API_SILVER_REFRESH_READY & FUEL_PRICE_SILVER)
    | (GOLD_INPUTS_READY & (API_SILVER_REFRESH_READY | FUEL_PRICE_SILVER))
)
```

최초 실행은 원천 3종 Silver + 연료비 Silver가 둘 다 준비돼야 한다. 이후는 직전 Gold 실행이 낸 `GOLD_INPUTS_READY`가 있어 둘 중 하나만 갱신돼도 재실행된다 — Asset 이벤트는 한 번 쓰면 소비되므로 자기 참조로 재무장한다.

## 입출력·버전 계약

- **Bronze**: 버전 키 `collected_at`. 수집 시각을 UTC 00:00으로 고정해 같은 날짜 재수집도 같은 값.
- **Silver**: 버전 키 `source_collected_at`(`source_collected_at=YYYYMMDDTHHMMSSssssssZ`). 최신 버전 = `_SUCCESS`와 데이터 파일이 모두 있는 디렉터리 중 타임스탬프 최댓값.
- **Gold**: `gold_load_versions` 테이블에 `(service_area, year_month, version)` 기본키, `(service_area, year_month, load_fingerprint)` 유니크 제약.

`load_fingerprint`(`gold_config_hash()`)는 지역·연월, Silver 4종 S3 경로, Silver 입력 내용 SHA-256 다이제스트(#1088), 알고리즘 상수 다이제스트, 알고리즘 버전×threshold 조합을 직렬화해 해시한다. `job.py`(계보 기록)와 `postgres_loader.py`(멱등성 검증)가 같은 정규화 규칙으로 호출한다 — 어긋나면 같은 실행이 새 버전으로 중복되거나 다른 설정이 기존 버전을 잘못 재사용한다.

## 재처리(멱등성) 계약

`write_gold_to_postgres()`:

1. `load_fingerprint` 계산 → 같은 지역·월·fingerprint 조합 조회
2. 있으면 기존 `version` 재사용, 새로 쓰지 않음
3. 없으면 최대 `version`+1로 4개 테이블(`driver_aggregation`, `driver_car_suggestion`, `silver_lineage`, `gold_load_versions`)을 한 트랜잭션으로 적재

동시 실행은 `pg_advisory_xact_lock(hashtext(service_area), hashtext(year_month))`로 막는다(#1056).

## 참고

- [`main/airflow/common/assets.py`](../../main/airflow/common/assets.py): Asset·파티션 키 정의
- [`main/airflow/dags/source_api_refresh_dag.py`](../../main/airflow/dags/source_api_refresh_dag.py): 원천 감시와 트리거
- [`main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py): Gold Asset 스케줄
- [`main/spark/jobs/silver_to_gold/monthly_silver.py`](../../main/spark/jobs/silver_to_gold/monthly_silver.py): Silver 최신 버전 선택
- [`main/spark/jobs/silver_to_gold/postgres_loader.py`](../../main/spark/jobs/silver_to_gold/postgres_loader.py): fingerprint·버전 테이블·트랜잭션 적재
- [`main/spark/jobs/silver_to_gold/input_digest.py`](../../main/spark/jobs/silver_to_gold/input_digest.py): Silver 입력 내용 다이제스트
- [`shared/aws_lambda/common/collected_at.py`](../../shared/aws_lambda/common/collected_at.py): Bronze 수집 시각 고정
- [`docs/troubleshooting/pipeline/ASSET_EVENT_CONSUMPTION_STOPS_GOLD_REFRESH.md`](../troubleshooting/pipeline/ASSET_EVENT_CONSUMPTION_STOPS_GOLD_REFRESH.md): Asset 재무장 문제
