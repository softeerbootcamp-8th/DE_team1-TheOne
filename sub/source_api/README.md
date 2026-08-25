# source_api

`main`이 원천 데이터를 직접 안 읽고 이 API만 거치도록 하는, 사내 가짜 회사 원천 API입니다.
월별 릴리스 하나를 데이터셋별로 Parquet 파일 그대로 내려줍니다 

## 엔드포인트

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET/HEAD | `/v1/data/{year_month}/datasets/{dataset}` | 해당 월 릴리스의 Parquet 다운로드 |
| GET/HEAD | `/v1/data/latest/datasets/{dataset}` | 가장 최신 월로 307 리다이렉트 |
| GET | `/health` | `{"status": "ok"}` |

- `year_month`은 `YYYY-MM` 형식만 허용합니다 (`2026-1`, `202601` 등은 404).
- `service_area` 쿼리는 대문자 지역 코드입니다. 생략하면 기존 NYC로 처리합니다.
- `latest` 리다이렉트는 요청한 `service_area`를 유지합니다.
- 트레일링 슬래시와 그 외 쿼리스트링은 무시합니다.
- HEAD는 헤더만 주고 본문은 안 내려줍니다 (`Content-Length`는 정확히 채워짐).

### dataset 목록

| dataset | 대응 스키마 |
|---|---|
| `monthly_taxi_trip` | `schema.bronze.MONTHLY_TAXI_TRIP_SCHEMA` |
| `driver_vehicle_monthly_snapshot` | `schema.bronze.DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA` |
| `lease_vehicle_inventory` | `schema.bronze.LEASE_VEHICLE_INVENTORY_SCHEMA` |

이 목록 밖의 이름(대문자 포함)은 전부 404입니다 — `DATASETS` 화이트리스트로 막습니다.

### 응답

성공 시 `200`, 본문은 Parquet 바이트 그대로, 헤더는:

```
Content-Type: application/vnd.apache.parquet
Content-Length: <바이트 수>
Content-Disposition: attachment; filename="<dataset>.parquet"
ETag: "<원본 식별자>"
Last-Modified: <HTTP date>
```

500MB급 파일도 청크 단위로 스트리밍하므로 서버가 파일 전체를 메모리에 올리지 않습니다.

### 에러

| 상태 | 상황 |
|---|---|
| `404` | 알 수 없는 경로·dataset·월 형식, 또는 해당 월 릴리스/파일이 없음 |
| `500` | 릴리스는 있는데 읽을 수 없음 (예: local 모드의 manifest.json이 깨짐) |

## 저장소 백엔드 (`SOURCE_API_ENV`)

| 값 | 동작 |
|---|---|
| `local` (기본값) | NYC는 기존 `year_month=YYYY-MM/manifest.json`, 그 외 지역은 `service_area=<지역>/year_month=YYYY-MM/manifest.json`을 읽음 |
| `prod` | S3의 `source/published/<service_area>/<dataset>/year_month=YYYY-MM/data.parquet` 고정 키를 직접 읽음. manifest 없음 |

dataset 이름은 한 벌뿐입니다 — 공개 API 경로, local manifest의 키, S3 폴더명이 모두 같습니다.
예전에는 발행 쪽만 `hvfhv_taxi_trips`를 써서 local은 번역표로 가리고 prod는 발행한 폴더와
읽는 폴더가 어긋나 404였습니다(#859). 이름을 나눠 쓰면 그 사고가 그대로 돌아옵니다.

## 환경변수

`.env.example`의 `SOURCE_API_*` 참고. 필수는 `prod`일 때의 `SOURCE_API_S3_BUCKET`뿐이고,
나머지는 전부 기본값이 있습니다.

## 로컬 실행

```bash
python -m sub.source_api.server
```
