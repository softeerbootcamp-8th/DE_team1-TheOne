# 같은 입력을 다시 실행해도 결과가 늘어나지 않도록 버전 관리

## 요약

- 원천 API의 변경 정보와 실제 파일 내용을 Bronze manifest에 함께 기록한다.
- Bronze의 `collected_at`을 Silver의 `source_collected_at`으로 이어 원본 계보를 남긴다.
- Gold는 Silver 내용과 계산 설정이 같으면 기존 결과를 재사용한다.

## 문제

재시도와 과거 월 재처리 때 실행 시각을 버전으로 쓰면 같은 입력도 새 데이터로 쌓인다.
반대로 경로만 비교하면 같은 위치의 파일 내용이 바뀌어도 이전 결과를 재사용할 수 있다.

버전은 DAG 실행 횟수가 아니라 실제 입력과 계산 설정으로 구분해야 한다. 또한 최종
결과에서 어떤 API 응답과 Bronze 수집본을 사용했는지 역으로 추적할 수 있어야 한다.

## 단계별 버전 기준

| 단계 | 버전 판단 기준 |
| --- | --- |
| 원천 API | `ETag`, `Last-Modified`로 변경 가능성 확인 |
| Bronze | 실제 파일 내용과 `collected_at` |
| Silver | 사용한 Bronze의 `collected_at` |
| Gold | Silver 파일 내용과 계산 설정 |

`ETag`와 `Last-Modified`는 불필요한 다운로드를 줄이는 변경 감지 정보다. 실제 저장
버전과 무결성은 다운로드한 파일의 SHA-256과 manifest로 확인한다.

## 원천 API에서 Bronze까지

매일 API를 감시하는 DAG는 최신 Bronze manifest의 `source_etag`와 `source_last_modified`를 읽어 API의
현재 값과 비교한다. 값이 같고 내부 Bronze·Silver도 정상이면 본문 다운로드를 생략하고,
값이 바뀌었거나 내부 결과가 없으면 API 본문을 다시 받는다.

API 응답을 수집하면 원본을 다음 경로에 저장한다.

```text
bronze/<dataset>/service_area=<지역>/year_month=<월>/
└── collected_at=<원본 수집 시각>/
    ├── data.parquet
    ├── manifest.json
    └── _SUCCESS
```

`collected_at`은 DAG 실행 시각이 아니라 해당 원본을 저장한 수집 버전이다.

새로 받은 파일의 내용이 기존 Bronze와 같으면 이전 `collected_at` 경로를 재사용한다.
이때 최신 API 변경 정보만 manifest에 갱신한다. 파일 내용이 달라졌을 때만 새
`collected_at` 버전을 만든다.

Silver 처리 전에는 manifest와 실제 Parquet의 경로, 크기, 행 수, SHA-256을 대조한다.
manifest가 없거나 원본과 다르면 정상 Bronze로 사용하지 않는다.

## Bronze에서 Silver까지

Silver 경로에는 변환 실행 시각 대신 사용한 Bronze의 수집 시각을 기록한다.

```text
silver/<dataset>/service_area=<지역>/year_month=<월>/
└── source_collected_at=<Bronze의 collected_at>/
    ├── data.parquet 또는 part-*.parquet
    └── _SUCCESS
```

`source_collected_at`은 “이 Silver가 어떤 Bronze 수집본에서 만들어졌는가”를 뜻한다.
Bronze와 Silver의 토큰이 같으므로 경로만으로 원본 계보를 찾을 수 있다. 같은 Bronze를
재처리해도 같은 Silver 버전 경로를 사용하고, 새 Bronze가 생기면 새 Silver 버전을 만든다.

Gold는 데이터 파일과 `_SUCCESS`가 모두 있는 Silver 버전만 입력으로 선택한다.

## Silver에서 Gold까지

Gold는 지역·월, Silver 파일의 내용 해시, 알고리즘 버전·기준값, 계산 상수로 SHA-256
fingerprint를 만든다. 파일명과 실제 내용을 함께 해시하므로 파일 추가·삭제·내용 변경을
모두 감지한다.

같은 fingerprint가 이미 있으면 기존 Gold 버전을 사용한다. Silver 경로가 같아도 내용이
바뀌거나 계산 설정이 달라지면 새 Gold 버전을 만든다.

## 안전 장치

- 품질 검증을 통과한 결과만 완료 버전으로 인정한다.
- 같은 지역·월의 동시 Gold 적재는 DB 잠금으로 순서를 정한다.
- 다른 지역·월은 서로 기다리지 않는다.
- 로컬 파일은 임시 파일에 쓴 뒤 최종 경로로 교체한다.

## 검증

| 상황 | 결과 |
| --- | --- |
| API 파일 내용이 같음 | 기존 Bronze `collected_at` 재사용 |
| API 파일 내용이 바뀜 | 새 Bronze·Silver 버전 생성 |
| manifest와 원본이 다름 | Silver 처리 전 실패 |
| 같은 입력과 설정으로 Gold 재실행 | 기존 Gold 버전 재사용 |
| 같은 경로의 Silver 내용 변경 | 새 fingerprint 생성 |
| 알고리즘 또는 기준값 변경 | 새 Gold 버전 생성 |

## 관련 자료

- [`shared/common/bronze_manifest.py`](../../shared/common/bronze_manifest.py)
- [`main/aws_lambda/common/monthly_dataset.py`](../../main/aws_lambda/common/monthly_dataset.py)
- [`main/airflow/common/monthly_bronze.py`](../../main/airflow/common/monthly_bronze.py)
- [`main/spark/jobs/silver_to_gold/input_digest.py`](../../main/spark/jobs/silver_to_gold/input_digest.py)
- [`main/spark/jobs/silver_to_gold/postgres_loader.py`](../../main/spark/jobs/silver_to_gold/postgres_loader.py)
- [DAG 성공이 아닌 데이터 정합성으로 완료를 판단](./CORRECTNESS_BEFORE_SUCCESS.md)
