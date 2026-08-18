# Shared

원천 DB와 메인 데이터 프로덕트가 함께 사용하는 최소 기술 계약입니다. 제품별 비즈니스 로직은 이곳에 두지 않습니다.

- `airflow/common`: DAG 경로, 검증, 알림, 실행 헬퍼
- `spark/common`: Spark 세션과 입출력 헬퍼
- `lambda_runtime/common`: S3 입출력, 스키마 검증, 데이터셋 레이아웃
