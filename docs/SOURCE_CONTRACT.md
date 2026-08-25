# 원천을 신뢰하지 않는 전제로 수집합니다

원천과 메인 데이터 프로덕트는 HTTP API 경계로 분리합니다. 메인은 원천 내부의 릴리스 정보가 아니라 공개된 Parquet 파일만 사용합니다.

| 어디서 | 무엇을 가정하지 않는가 | 어떻게 |
| --- | --- | --- |
| 원천 시스템의 공개 원천 수집 | 사이트가 늘 같은 형식으로 응답한다 | 원천 내부에서 기대 스키마와 릴리스 완성도 확인 |
| 메인 파이프라인의 API 수집 | 응답이 읽을 수 있는 파일이다 | HTTP 응답·파일 크기·Parquet 가독성 확인 |
| 메인 파이프라인의 값 품질 | 원천 메타데이터가 값 품질까지 보장한다 | Silver 품질 규칙은 후속 이슈 #543에서 별도 정의 |

---

## 공개 API 계약

메인은 아래 세 데이터셋의 월별 URL만 호출합니다. 응답 본문은 Parquet 파일이며 원천 행 수·SHA-256·실행 계보를 담은 원천 시스템의 JSON Manifest는 공개하지 않습니다.

```text
GET /v1/data/{YYYY-MM}/datasets/monthly_taxi_trip
GET /v1/data/{YYYY-MM}/datasets/driver_vehicle_leases
GET /v1/data/{YYYY-MM}/datasets/lease_vehicle_inventory
```

`YYYY-MM` 대신 `latest`를 요청하면 최신 월의 같은 데이터셋 URL로 리다이렉트합니다. 실제 월은 최종 URL에 나타나며 응답은 동일하게 Parquet 파일만 반환합니다.
월별 GET/HEAD 응답은 조건부 요청에 쓸 `ETag`와 `Last-Modified`를 함께 제공합니다.

## 내부 릴리스 Manifest

원천은 세 파일을 모두 생성했는지 게시 전에 확인하기 위해 `manifest.json`을 내부에서 유지합니다. 이 파일의 행 수·SHA-256·실행 계보는 원천 구현 정보이며 메인 수집 계약이 아닙니다.

메인 파이프라인은 다운로드한 세 API 데이터셋에 한해 별도의 Bronze sidecar
`manifest.json`을 생성합니다. 여기에는 다운로드 바이트의 SHA-256·크기·Parquet 행 수,
지역·월·수집 시각과 GET 응답의 HTTP validator가 들어갑니다. Airflow는 sidecar와 실제
파일을 다시 대조한 뒤 `_SUCCESS`를 공개하며, 다음 refresh는 최신 정상 sidecar에서
조건부 HEAD 값을 복원합니다.

## 가정하는 실패와 방어

| 가정하는 실패 | 방어 | 위치 |
| --- | --- | --- |
| 요청한 월이나 데이터셋이 없음 | API `404` 응답 | 원천 API |
| 최신 URL이 다른 호스트나 데이터셋으로 이동 | 최종 URL의 host·월·데이터셋 확인 | 수집 |
| 빈 응답 또는 Parquet이 아닌 응답 | 응답 바이트와 `pq.ParquetFile` 확인 | 적재 |
| 같은 달을 다시 수집 | `year_month=YYYY-MM/<UTC 수집시각>.parquet`으로 원본 이력 추가 | 적재 |
| 저장 결과가 수집 응답과 다름 | 파일 크기·Parquet footer 행 수·SHA-256·파티션 계보를 Bronze manifest와 대조 | 검증 태스크 |

행 수는 원천이 알려 준 값과 비교하지 않고, 다운로드한 Parquet footer에서 계산해 Main 내부 결과로만 사용합니다.

([monthly_dataset.py](../main/aws_lambda/common/monthly_dataset.py) · [monthly_bronze.py](../main/airflow/common/monthly_bronze.py))
