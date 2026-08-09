# airflow/dags/sql

DAG가 참조하는 SQL/DDL 파일(예: Gold 테이블 생성 DDL, 복잡한 집계 쿼리) 저장
DAG 코드 안에 SQL 문자열을 직접 넣지 않고 `.sql` 파일로 분리해서 여기 저장

파일명 규칙: `<dataset_or_purpose>.sql` (예: `create_driver_weekly_stats.sql`).
