# Main Data Product

`sub`가 발행한 원천을 소비해 기사·운행 Silver와 추천 Gold를 만들고 차량 추천
결과를 제공하는 데이터 프로덕트입니다.

- `airflow/`: 메인 파이프라인 오케스트레이션
- `lambda/`: Published 기사·운행 원천 수집
- `spark/`: Silver·Gold 처리
- `dashboard/`: 추천 결과 제공

요금·차량 대장 생산 책임은 `sub`에 둡니다.
