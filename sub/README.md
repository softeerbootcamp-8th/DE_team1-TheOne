# Source DB

메인 제품에 필요한 원천 데이터를 생산하고 API로 발행하는 독립 데이터 프로덕트입니다.
합성 기사·운행 데이터뿐 아니라 EIA 연료·충전 요금, 차량 제원, Uber·Lyft Eligible
차량, 차량 카탈로그와 차량 마스터를 소유합니다.

- `airflow/`: 원천 수집·합성·발행 오케스트레이션
- `lambda_runtime/`: 요금·차량 원천 수집과 정제
- `spark/`: Curated·Synthesize·Attribution·Published 처리
- `scripts/`: 회사 스냅샷과 월별 상태 생성
- `synthetic_source_api/`: Published 데이터 제공 API

메인 데이터 프로덕트는 이 폴더의 내부 구현을 직접 읽지 않고 Published API를 통해 데이터를 소비합니다.
