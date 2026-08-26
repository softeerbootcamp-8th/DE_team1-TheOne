# 02. E2E 파이프라인 — 원천부터 대시보드까지

![메인 데이터 파이프라인 아키텍처](../../assets/main_data_product_architecture.png)

## 계층별 흐름

| 단계 | 런타임 | 하는 일 | 적재 위치 | 실행 DAG |
| --- | --- | --- | --- | --- |
| **원천** | `sub/` 파이프라인 + 외부 사이트 | TLC HVFHV 실데이터에 taxi_id·driver_id 결정적 배정, API 공개 / EIA 파일 다운로드 | 원천 시스템 | `source_api_refresh_dag`(원천 갱신) |
| **Bronze** | AWS Lambda (VPC 내) | 원본 그대로 적재 + 수집 품질 검증. 월 데이터는 `year_month=…/collected_at=…` 버전 디렉터리로 쌓고 `_SUCCESS` 마커로 완료 표시 | S3 | `monthly_taxi_trip_raw_to_silver_dag`, `eia_gas_price_raw_to_silver_dag`, `eia_electricity_price_raw_to_silver_dag`, `driver_vehicle_monthly_snapshot_raw_to_silver_dag`, `lease_vehicle_inventory_raw_to_silver_dag` |
| **Silver** | Lambda 또는 Spark(EMR) | 원본별 정제(컬럼 필터·타입·범위·퇴사 기사 제외), 스키마 계약 검증. EIA 두 종은 일별로 펼쳐 통합 연료비(`gas_ev_price`)로 합침 | S3 | 위 DAG 후반부 + `eia_fuel_price_silver_dag` (상류 2개의 `validate_silver` 성공을 ExternalTaskSensor 로 대기) |
| **Gold** | Spark on EMR Serverless | Silver 4종 조인, control total·비즈니스 불변식 검증, 추천 알고리즘 v1/v2 계산, RDS 버전 적재(fingerprint 멱등성 + advisory lock) | RDS PostgreSQL | `monthly_taxi_trip_silver_to_gold_dag` (Silver asset 파티션 의존) |
| **대시보드** | Streamlit on EC2 | Gold 최신 버전 조회 → 필터·지표·차트·추천 테이블 | — | 상시 서비스(Nginx 리버스 프록시) |

수집 규모와 버전 계보 개요는 [README INPUT/OUTPUT 표](../../README.md#데이터-파이프라인) 참고.

## 검증 장치 (계층별)

| 계층 | 장치 | 실패 시 동작 |
| --- | --- | --- |
| Bronze | Lambda 내 수집 품질 검증 + Airflow 태스크에서 Great Expectations 표 검증 (`shared/airflow/common/validation.py`, 데이터독스 `data/gx_data_docs/`) | DAG 실패 + Slack 품질 경보(`send_gx_quality_warning`) |
| Silver | parquet 물리 스키마 계약 대조, `_SUCCESS` 없는 미완료 버전은 하류가 읽지 않음, 검증 실패 산출물은 격리(`_QUARANTINED.json`) 후 `_SUCCESS` 미발행 | 하류 DAG 가 파일 부재로 명확히 실패 |
| Gold | 운행 합계 control total 대조(`reconcile_gold_control_totals`), 차원 유일성·재고 한도·음수 증가액 불변식(`validate_gold_business_invariants`), 커밋 전 행수 대조(`_validate_written_rows`) | 적재 전 ValueError 로 중단, 부분 커밋 없음 |
| 전 구간 | 태스크별 Slack 실패·재시도 알림(`shared/airflow/common/slack_failure_callback.py`) | 온콜 인지 |

## 실행 절차

### 전체 로컬 (docker compose)

```bash
make sync          # 런타임별 uv sync + tesseract
docker compose up -d   # postgres / airflow / postgres-db
# Airflow UI(http://localhost:8080) 에서 monthly_taxi_trip_raw_to_silver_dag 등 트리거
```

### 단계별 수동 실행

```bash
# Bronze/Silver: Lambda 함수 로컬 실행
cd main/aws_lambda && PYTHONPATH=".:../.." uv run --frozen python -m functions.<함수명>.handler

# Gold: 로컬 CSV 산출(data/gold)
cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
  --year 2026 --month 1 --service_area NYC

# 대시보드
cd main/dashboard && DASHBOARD_DATA_SOURCE=local uv run --frozen streamlit run app.py
```

### 검증

```bash
make lint    # ruff (F·E9)
make test    # 런타임별 pytest (main 4개 + libs/pipeline_core + sub/shared)
make check   # uv lock --check
```

CI(GitHub Actions)는 PR 마다 `make test` 를 돌려 통과해야 머지됩니다.

## 운영 관측

- EC2 호스트 4대 — Prometheus + Grafana ([docs/MONITORING.md](../MONITORING.md))
- EMR Serverless — CloudWatch 지표·로그
- 파이프라인 실패·재시도 — Slack 알림
