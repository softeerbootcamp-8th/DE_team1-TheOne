# 스키마 검증·품질 게이트·계보 추적

- 요약
  - 스키마 : 계층별 `pyarrow.Schema`(`schema/`)를 기준으로 컬럼 누락·타입 불일치를 진단
  - 품질 게이트 : Bronze·Silver는 Great Expectations로 행 수·필수값·스키마를 검사하고, 통과 여부를 `_SUCCESS`/`_QUARANTINED.json`으로 남김
  - Gold : GX 대신 Spark에서 직접 짠 비즈니스 불변식 검사를 통과해야 적재가 시작되고, 계보(`silver_lineage`)는 검사와 별개로 항상 기록

## 스키마 정의와 검증

계층별 `pyarrow.Schema`: `schema/bronze`, `schema/silver`(필수 non-null 컬럼 별도 상수), `schema/gold`(`@dataclass` → Postgres `CREATE TABLE` 컬럼 직접 생성, `postgres_loader.py`).

- `shared/aws_lambda/common/schema_validator.py`의 `validate_parquet_schema()` — 컬럼 누락·타입 불일치(하드 오류) / 여분 컬럼(경고)
- `shared/airflow/common/validation.py`의 `table_quality_summary()` — 위 비교 + 필수값 non-null 비율, GX가 이 요약을 평가

## Great Expectations 품질 게이트

GX는 Bronze·Silver에만 적용. Gold는 미적용.

`run_gx_validation()`(`shared/airflow/common/validation.py`)이 pandas DataFrame 배치를 ExpectationSuite로 검증하고, `severity=warning`(경고, 계속 진행)과 하드 실패(`ValueError`, 이후 단계 차단)를 나눈다.

`run_table_gx_validation()` 조건:

- 행 수 1개 이상
- 컬럼 누락 없음, 타입 불일치 없음 (하드 실패)
- 필수 컬럼 non-null 비율 — 경고/실패 임계값 분리 (예: 1%까지 경고, 5% 초과 실패)
- 여분 컬럼(경고만)

Bronze·Silver 검증은 Airflow task(`main/airflow/scripts/*/tasks.py`)에서 호출한다. Spark Silver 작업 자체는 GX를 돌리지 않고, Spark가 다 쓴 뒤 Airflow task가 검증한다.

Gold 품질 기준(재고 초과 배정 금지, 순수익 감소 배정 금지 등)은 행 수·null 비율 같은 범용 검사가 아니라 도메인 규칙이라 GX 대신 Spark 코드로 직접 검사한다.

## 공개/격리 마커(`_SUCCESS`/`_QUARANTINED.json`)

`shared/common/success_marker.py`:

- `_SUCCESS`(빈 파일) : 품질 게이트 통과, 후속 단계 읽기 가능
- `_QUARANTINED.json`(`{failed_at, layer, reason, retryable, run_id}`) : 검증 실패, 재시도 전까지 종료 상태
- `_RECON.json` : Spark가 이번 변환에서 몇 건을 걸렀는지 남기는 sidecar — 계산(Spark)과 집계(Airflow) 프로세스가 달라 숫자를 맞대볼 근거로 필요

두 마커는 항상 배타적. `run_quality_gate(directory, validator, ...)`가 `validator()` 예외 시 격리 마커를 쓰고 재던지고, 성공 시 격리 마커를 지운 뒤 `_SUCCESS`를 쓴다. 다운스트림은 `require_success_marker()`로 `_SUCCESS` 없는 디렉터리를 읽지 않는다.

## 최신 완료 버전 선택

Silver 최신 버전 = `_SUCCESS`와 데이터 파일이 모두 있는 디렉터리 중 최댓값(`monthly_silver.py`). 격리된 최신 버전은 건너뛰고 이전 완료 버전을 쓴다.

Gold 최신 버전은 S3 마커와 별개로 `gold_load_versions` 테이블의 정수 `version` 컬럼으로 관리한다([PIPELINE_LAYERS_AND_CONTRACTS.md](./PIPELINE_LAYERS_AND_CONTRACTS.md) 참고).

## Gold 계보와 품질 불변식

`SilverLineage`(`schema/gold/__init__.py`)는 Gold 실행마다 남기는 계보: Silver 4종 S3 경로, `airflow_run_id`, `code_sha`, `config_hash`. 품질 판정이 아니라 입력 출처 기록이다.

품질 판정은 `validate_gold_business_invariants()`(`transformer.py`)가 한다. Postgres 적재 **전** 호출, 아래 중 하나라도 어긋나면 `ValueError`로 적재를 막는다.

- `driver_id` null·중복 없음
- 두 결과의 기사 수 일치
- 알고리즘·threshold 조합별 재고 초과 배정 없음
- 순수익 증가가 음수인 배정 없음

`reconcile_gold_control_totals()`는 별도로 Silver 운행거리·정산액·팁 합계와 Gold 집계 합계를 비교한다.

두 검사 모두 결과를 저장하지 않는다 — 통과하면 다음 단계로, 실패하면 예외로 즉시 멈춘다. Gold에는 GX의 "경고만 남기고 계속 진행" 단계가 없다.

## Bronze manifest

`shared/common/bronze_manifest.py`의 `manifest.json`은 계보·품질 체계와 별도 계약이다. SHA-256, 행 수, 파일 크기, 원천 API `ETag`/`Last-Modified`를 담아 다음 수집 때 원천 변경 여부를 조건부 HTTP로 먼저 확인한다. `config_hash`는 이 manifest sha256이 아니라 Silver 콘텐츠 다이제스트를 따로 계산한다 — manifest는 "원천이 바뀌었는가", `config_hash`는 "Gold 입력이 바뀌었는가"를 본다.

## 참고

- [`shared/airflow/common/validation.py`](../../shared/airflow/common/validation.py): GX 실행, 스키마·품질 요약
- [`shared/common/success_marker.py`](../../shared/common/success_marker.py): `_SUCCESS`/`_QUARANTINED.json`/`_RECON.json` 계약
- [`shared/common/bronze_manifest.py`](../../shared/common/bronze_manifest.py): 원천 변경 감시 manifest
- [`main/spark/jobs/silver_to_gold/transformer.py`](../../main/spark/jobs/silver_to_gold/transformer.py): `validate_gold_business_invariants`, `reconcile_gold_control_totals`
- [`schema/gold/__init__.py`](../../schema/gold/__init__.py): `SilverLineage`, `GoldLoadVersion`
- [`main/airflow/tests/test_operational_gx_quality.py`](../../main/airflow/tests/test_operational_gx_quality.py): GX 경고/실패 임계값 테스트
