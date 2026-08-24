# 멱등성을 고려한 수집 계보 기반 버전 경로

## 1. 문서 목적

월별 원천을 다시 수집하거나 Silver를 재시도할 때 같은 입력은 같은 결과로 취급하고,
다른 원본은 새 버전으로 보존해야 한다. 이 문서는 `collected_at`,
`source_collected_at`, `input_version`으로 재시도와 새 입력을 구분한 기준을 정리한다.
같은 입력의 재시도가 새 버전을 늘리지 않는 것을 이 파이프라인의 멱등성 기준으로 삼는다.

- 경로 계약: [`monthly_bronze.py`](../../main/airflow/common/monthly_bronze.py)
- EIA 입력 버전 계약: [`eia_fuel_version.py`](../../main/common/eia_fuel_version.py)
- 데이터 모델: [`DATA_MODEL.md`](../DATA_MODEL.md)

## 2. 기존 경로에서 발견한 문제

`year_month=YYYY-MM/data.parquet` 같은 고정 경로는 같은 달을 다시 수집할 때 이전 원본을
지운다. 반대로 Silver 실행 시각을 매번 새 버전으로 사용하면 같은 Bronze를 재시도해도
폴더가 계속 늘어난다.

필요한 동작은 다음 두 가지였다.

1. 같은 원본으로 재시도하면 같은 경로를 교체한다.
2. 다른 원본으로 재처리하면 이전 결과를 남기고 새 버전을 만든다.

## 3. 적용 판단

결과를 결정하는 입력의 자연 키를 버전으로 사용했다.

| 계층 | 버전 키 | 의미 |
|---|---|---|
| Bronze | `collected_at` | 원본을 실제로 수집한 UTC 시각 |
| 단일 입력 Silver | `source_collected_at` | 사용한 Bronze의 수집 시각 |
| 통합 연료비 Silver | `input_version` | Gas·EV Silver 입력 버전 조합 |

Silver 작업을 실행한 시각은 결과의 정체성이 아니다. 같은 Bronze에서 같은 변환 규칙을
재시도한 결과는 같은 버전이어야 한다.

## 4. 적용 경로

### 4.1 Bronze

```text
bronze/<dataset>/service_area=<지역>/year_month=YYYY-MM/
└── collected_at=YYYYMMDDTHHMMSSffffffZ/
    ├── data.parquet 또는 원본 파일
    └── _SUCCESS
```

`year_month`는 데이터 기준 월이고 `collected_at`은 관측 시각이다. 같은 월의 원천이
수정돼 다시 수집되면 새로운 `collected_at` 디렉터리가 생긴다.

### 4.2 단일 입력 Silver

```text
silver/<dataset>/service_area=<지역>/year_month=YYYY-MM/
└── source_collected_at=YYYYMMDDTHHMMSSffffffZ/
    ├── data.parquet 또는 part-*.parquet
    └── _SUCCESS
```

Bronze token을 이름만 바꿔 계승하므로 Silver에서 원천 Bronze까지 경로로 역추적할 수
있다.

### 4.3 통합 연료비 Silver

```text
silver/gas_ev_price/service_area=<지역>/year_month=YYYY-MM/
└── input_version=gas-<gas-token>__ev-<ev-token>/
    ├── fuel.parquet
    └── _SUCCESS
```

두 입력 중 하나라도 바뀌면 결과를 결정하는 입력 조합이 달라지므로 새 버전을 만든다.

## 5. 재처리 시나리오

| 실행 상황 | 저장 결과 |
|---|---|
| 같은 Bronze로 Silver 재시도 | 같은 `source_collected_at` 경로 교체 |
| 새로운 Bronze로 Silver 실행 | 새 `source_collected_at` 경로 생성 |
| 과거 Bronze 버전 백필 | 해당 token의 Silver 경로 재생성 |
| 같은 Gas + 같은 EV 재처리 | 같은 `input_version` 교체 |
| Gas 또는 EV 중 하나 변경 | 새 `input_version` 생성 |

## 6. 공개와 실패 조건

writer는 재쓰기 전에 기존 `_SUCCESS`를 제거하고 최종 버전 경로에 데이터를 쓴다.
Airflow 검증 성공 후 marker를 다시 기록하며 하류는 marker가 있는 버전만 읽는다.

다음 구현은 계보 계약을 깨뜨린다.

- Silver의 현재 실행 시각을 `source_collected_at`으로 사용
- 통합 결과에 Gas·EV 중 한쪽 token만 사용
- 빈 디렉터리나 marker 없는 최신 버전을 선택
- 생산자와 소비자가 서로 다른 token 형식을 허용
- 생산자와 소비자에서 서로 다른 디렉터리 구조 사용

## 7. 재검증 절차

1. 같은 Bronze 입력으로 Silver를 두 번 실행해 같은 경로가 교체되는지 확인한다.
2. 새 Bronze token으로 실행해 이전 Silver와 새 Silver가 함께 남는지 확인한다.
3. Gas·EV 한쪽 token만 바꿔 새 `input_version`이 만들어지는지 확인한다.
4. 재처리 중 실패시 기존 `_SUCCESS`가 제거돼 하류에서 숨겨지는지 확인한다.
5. marker 없는 더 최신 디렉터리보다 이전 완료 버전이 선택되는지 확인한다.
6. Gold가 선택한 Silver 경로에서 Bronze token을 역추적한다.

## 8. 결론

버전은 job 실행 시각이 아니라 결과를 결정한 입력으로 정의해야 한다. Bronze의 수집
시각을 Silver가 계승하고, 다중 입력 결과는 입력 조합을 자연 키로 사용함으로써 같은
입력 재시도의 멱등성과 다른 원본의 이력 보존을 동시에 확보했다.
