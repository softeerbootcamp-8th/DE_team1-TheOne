# 코드 구조와 설정값 분리

- 요약
  - 코드 구조 : Lambda·Spark·Airflow 3개 런타임이 공유하는 순수 인터페이스(`libs/pipeline_core`)와, 런타임별 SDK를 쓰는 헬퍼(`shared/{aws_lambda,spark,airflow}/common`)를 분리
  - 설정값 : 재배포 없이 바꿔야 하는 값(threshold, freshness SLA 등)은 Airflow Variable로, 환경마다 달라지는 값(버킷, DSN, 리전)은 환경변수로, 코드 로직에 붙는 상수는 그 로직 옆에 둠

## 코드 구조: 공통 인터페이스와 런타임별 계약

`libs/pipeline_core`: `Extractor`/`Transformer`/`Loader`/`Pipeline` 네 개 인터페이스만. `abc`/`typing`/`dataclasses`만 쓰고 외부 의존성 0개(`dependencies = []`) — pyarrow·boto3·pyspark를 넣으면 3개 런타임 공유가 깨진다. `Pipeline.run()`은 실패 시 로그 후 예외 재발생, 재시도·알림은 Airflow 책임.

구현체는 Lambda에 집중 — `Extractor`/`Loader` 각 17개(`main/aws_lambda/`). Spark는 데이터셋마다 새로 안 만들고 공용 `SparkParquetExtractor`/`SparkParquetLoader`(`shared/spark/common/io.py`)를 재사용.

| 디렉터리 | 의존성 | 예시 |
| --- | --- | --- |
| `shared/common/` | stdlib+boto3 (Lambda+Spark 공통) | `bronze_manifest.py`, `success_marker.py` |
| `shared/aws_lambda/common/` | boto3·pyarrow | `schema_validator.py`, `storage_config.py` |
| `shared/spark/common/` | pyspark | `io.py`, `session.py` |
| `shared/airflow/common/` | airflow provider | `validation.py`(GX), `lambda_invoke.py` |

Spark는 EMR 7.13 고정 버전(`pandas==3.0.1`), Lambda는 이미지 크기 때문에 pandas 미사용 — 공통 계약(`pipeline_core`, `shared/common/`)에는 어느 쪽 의존성도 넣지 않는다.

## 설정값 분리

**운영 중 조정 값 — Airflow Variable.**

| Variable | 조정 대상 | 기본값 |
| --- | --- | --- |
| `gold_recommendation_thresholds` | v2 알고리즘이 스윕할 순수익 증가 하한 목록 | `[100, 200, 300, 400, 500]` |
| `gold_stale_sla_days` | Gold 데이터 stale 판정 기준일 | `31`, Variable 조회 실패 시에도 이 값 |
| `hvfhv_error_threshold` | Bronze 검증 허용 오류 행 비율 | `0.05` |
| `eia_electricity_markup` | 전기요금 공공 충전 마크업 배수 | `2.0` |

**환경별 값 — 환경변수.** `DATA_LAKE_S3_BUCKET`, `GOLD_DATABASE_URL`, `SPARK_JOB_ENV`(local/prod), `AWS_DEFAULT_REGION`. 로컬은 `shared/common/env.py`가 `.env`를 읽고, Lambda(`AWS_LAMBDA_FUNCTION_NAME` 존재)는 `.env` 로딩을 건너뛴다.

환경변수 해석은 핸들러마다 흩어놓지 않고 `shared/aws_lambda/common/storage_config.py` 한 곳으로 모았다. Lambda 런타임에서 값이 없으면 로컬 기본값 대신 `ValueError`를 던진다 — 로컬 실행(`AWS_LAMBDA_FUNCTION_NAME` 없음)에서만 기본값 허용.

**로직 상수 — 그 로직 옆.** `DEFAULT_THRESHOLDS`(`revenue_first.py`)는 `RevenueFirstAlgorithm` 바로 위, `CONTROL_TOTAL_REL_TOL`/`CONTROL_TOTAL_ABS_TOL`(`transformer.py`)는 `reconcile_gold_control_totals()` 바로 위. 전역 설정 모듈로 모으지 않는다.

**배포 시점 설정 — GitHub Actions repo Variable.** `AWS_ROLE_ARN_*`, ECR 저장소 이름, EMR Application ID는 `gh variable set`으로 관리.

## 참고

- [`libs/pipeline_core/pipeline_core/`](../../libs/pipeline_core/pipeline_core/): `Extractor`/`Transformer`/`Loader`/`Pipeline`
- [`shared/README.md`](../../shared/README.md): `shared/` 디렉터리 구조 개요
- [`shared/aws_lambda/common/storage_config.py`](../../shared/aws_lambda/common/storage_config.py): 저장 위치 설정 해석과 실패 정책
- [`shared/common/env.py`](../../shared/common/env.py): 로컬 `.env` 로딩
- [`main/airflow/common/gold_staleness.py`](../../main/airflow/common/gold_staleness.py): freshness SLA 설정
- [`main/spark/jobs/silver_to_gold/job.py`](../../main/spark/jobs/silver_to_gold/job.py): CLI 인자와 환경변수 기본값
- [`main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py`](../../main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py): `DEFAULT_THRESHOLDS`
