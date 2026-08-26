# DAG 성공이 아닌 데이터 정합성으로 완료를 판단

## 요약

- 입력, 행 수, 조인, 집계, 적재 결과를 검증해 DAG 성공과 데이터 완료를 구분한다.
- 검증을 통과한 결과만 공개하고, Gold 일부 적재가 실패하면 전체를 되돌린다.

## 문제

DAG가 성공해도 빈 결과, 조인 누락, 일부 적재처럼 잘못된 데이터가 남을 수 있다.
따라서 코드 실행 성공과 데이터 공개 가능 여부를 분리해야 한다.

이런 오류는 실패 기록이 남지 않아 더 늦게 발견된다. 대시보드 수치가 달라진 뒤 사람이
확인할 때까지 잘못된 결과가 정상 데이터처럼 사용될 수 있다.

## 검증 기준

| 단계 | 확인 항목 |
| --- | --- |
| 입력 | 지역·월, 스키마, 파일 크기·행 수, manifest SHA-256 |
| 정제 | 입력 행 수 = 정상 출력 + 품질 제외 |
| 조인 | 매칭되지 않은 기사·차량 키 |
| 집계 | 운행 건수·거리·지급액·팁 합계 |
| 추천 | 기사별 결과, 차량 존재 여부, 재고와 순수익 조건 |
| 적재 | 예상 행 수와 실제 DB 행 수 |

정제 단계에서는 제외된 행까지 합쳐 입력 행 수와 일치하는지 확인한다. 조인 결과가
0행보다 크다는 것만으로 통과시키지 않고, 매칭되지 않은 기사·차량 키도 따로 검사한다.

다음 단계에서 안전하게 쓸 수 없는 문제는 실패 처리한다. 기존 계산을 깨지 않는 추가
컬럼은 경고만 남긴다.

## 공개 조건

- 검증을 통과한 파일에만 `_SUCCESS`를 기록한다.
- 실패한 버전은 원인과 실행 ID를 남기고 격리한다.
- Gold 적재는 하나의 트랜잭션으로 처리한다.
- 한 테이블이라도 행 수가 다르면 전체 적재를 되돌린다.

적재 작업이 예외 없이 끝나도 실제 DB 행 수를 다시 조회한다. 즉, Airflow 성공 상태가
아니라 입력 선택부터 최종 적재까지의 검증 통과를 완료 기준으로 사용한다.

## 검증

| 재현한 문제 | 기대 결과 |
| --- | --- |
| 필수 컬럼 누락 또는 타입 오류 | 정제 전 실패 |
| 행 수 보존식 불일치 | 완료 표시 없음 |
| 조인 누락 또는 집계 합계 불일치 | Gold 적재 전 실패 |
| 일부 테이블 적재 실패 | 전체 rollback |

## 관련 자료

- [`shared/airflow/common/validation.py`](../../shared/airflow/common/validation.py)
- [`main/airflow/scripts/monthly_taxi_trip_raw_to_silver/tasks.py`](../../main/airflow/scripts/monthly_taxi_trip_raw_to_silver/tasks.py)
- [`main/spark/jobs/silver_to_gold/transformer.py`](../../main/spark/jobs/silver_to_gold/transformer.py)
- [`main/spark/jobs/silver_to_gold/postgres_loader.py`](../../main/spark/jobs/silver_to_gold/postgres_loader.py)
- [같은 입력을 재실행해도 결과가 늘어나지 않는 버전 관리](./PIPELINE_IDEMPOTENCY_AND_LINEAGE.md)
