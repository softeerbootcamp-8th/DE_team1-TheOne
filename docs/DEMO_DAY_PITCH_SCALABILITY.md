# 데모데이 발표 스토리 — 수집·정제 확장성 어필 포인트 (A~F)

> 이 문서는 발표 대본이 아니라 발표를 다듬을 때 쓰는 근거 자료다. `docs/DEMO_DAY_PITCH.md`
> (C3/C4/C6)와는 별개의 어필 포인트를 다룬다. 조사·설계 대상은 `main/`
> (수집→Bronze→Silver→Gold, 실데이터 파이프라인)이며 `sub/`(합성 데이터 생성기)는
> 무관하다.
>
> **구현 상태를 반드시 구분해서 말할 것.** A는 이미 구현되어 있다. B/D/E/F는 이번
> `/grill-me` 세션에서 설계만 확정했고 코드는 아직 없다. C는 6개 데이터셋 중
> 1개(monthly_taxi_trip)만 구현됐고 나머지 5개는 같은 패턴으로 확장하는 설계까지만
> 정리했다. 심사에서 "구현됐냐"고 물으면 각 절의 **구현 상태** 표시를 그대로 답할 것 —
> 없는 걸 있다고 말하지 않는다.
>
> **F는 멀티리전이다.** 다른 4개(A/B/C/D/E)는 "확장하면 대비해야 할 것"을 다루면서도
> 지역 확장 자체(#674)는 계속 미루기로 했었다. F는 그 결정을 다시 열어, C3(Gold
> 비동기 트리거 설계)의 자연스러운 연장선에서 지역 축을 추가하는 구체적인 설계를
> 만든 것이다 — "이 설계라면 지역이 늘어도 각자 독립적으로 처리된다"는 걸 보여주려는
> 목적. 대신 이 설계로도 못 푸는 부분(택시존 스키마 문제)을 숨기지 않고 같이 밝혔다.

## 0. 왜 이 여섯 개인가

말씀하신 문제의식은 하나다: **지금은 뉴욕시 하나, 데이터도 적지만, 지역이 늘고
데이터량이 늘수록 "수집 → 검증 → 실패 처리 → Bronze 적재 → Silver 전환"의 각 단계에서
사람이 다 볼 수 없는 것들이 늘어난다.** 이걸 하나의 큰 서사로 묶기보다, 파이프라인의
각 단계에 대응하는 독립된 포인트로 쪼갰다 — 심사위원이 "왜 확장성과 관련 있는지"를
매번 다시 설명받지 않고, 포인트 하나하나가 스스로 완결되게 하려는 목적이다.

| 포인트 | 파이프라인 단계 | 구현 상태 | 한 줄 요약 |
|---|---|---|---|
| A — 수집 재요청 dedup | 수집(Raw→Bronze) | ✅ 이미 구현됨 | 내용이 같으면 원천을 다시 안 부른다 |
| D — 검증 강도 차등화 | Bronze 검증 | ⚠️ 관찰된 패턴을 이번에 원칙화 | 데이터 특성에 맞는 검증 비용 배분 |
| B — 스키마 드리프트 3단계 | Bronze/Silver 검증 | ❌ 설계만, 코드 없음 | 컬럼 추가는 통과, 필수 소실만 차단 |
| C — 검증-후-커밋 | Bronze→Silver 적재 | ⚠️ 1/6만 구현, 5/6 설계 | 검증 통과 못 한 데이터는 최종 경로에 안 남는다 |
| E — 월별 이상치 탐지 | Silver/Gold 결과 | ❌ 설계만, 코드 없음 | 사람이 다 못 보는 양이 되면 이상한 달을 스스로 짚어준다 |
| F — 지역 축(`service_area`) 파티셔닝 | 전 단계 + Gold 트리거 | ⚠️ 데이터 격리·3개 동시 실행 구현, 지역 등록·fan-out 별도 | 세 지역까지 수집·집계를 병렬 실행 |

발표 순서 제안: **A → D → B → C → E → F**. "지금도 이미 하고 있는 것"(A)으로 신뢰를
얻고, "왜 데이터셋마다 검증이 다른가"(D)로 설계 원칙을 보여준 뒤, 그 원칙이
스키마 변화(B)와 적재 무결성(C)에서 구체적으로 어떻게 동작하는지 보여주고,
"데이터가 늘어난 미래에 대한 대비"(E)를 거쳐, 마지막에 "지역이 늘어난 미래"(F)로
확장 비전을 마무리한다.

---

## 1. A — 수집 단계 재요청 dedup (이미 구현됨)

### 문제/계기

원천 API를 매번 무조건 다시 호출하면, 원천이 아직 그 달 데이터를 갱신하지
않았을 때도 같은 내용을 반복해서 받는다. 지역이 늘고 원천 수가 늘어날수록 이
낭비가 원천 수에 비례해서 커진다.

### 설계 결정 — 두 가지 메커니즘이 공존한다

- **조건부 HTTP (ETag/Last-Modified)** — `main/airflow/scripts/source_api_refresh/`.
  HVFHV(monthly_taxi_trip), 기사-차량 스냅샷, 리스 차량 인벤토리 3종이 대상.
  `check_and_should_refresh_task`가 원천에 HEAD 요청을 보내 ETag/Last-Modified를
  직전 처리 상태(`Variable("source_api_processed__{dataset}")`)와 비교하고,
  바뀌지 않았으면 `@task.short_circuit`으로 다운로드 자체를 건너뛴다.
- **콘텐츠 해시 비교** — EIA 가스·전력 수집 Lambda(`main/aws_lambda/functions/
  eia_gas_price_raw_to_bronze/loader.py`, `eia_electricity_price_raw_to_bronze/loader.py`).
  `layout.is_duplicate_of_newest(base_dir, dataset, file_name, body)`가 방금 받은
  바이트를 최신 Bronze 파일과 직접 비교하고, 동일하면 새 파티션 파일을 만들지 않고
  기존 파일의 `WriteResult`를 그대로 반환한다.
- 두 메커니즘은 원천의 성격이 달라서 나뉜 것이다 — 내부 모의 API 3종은 ETag를
  지원하니 표준 HTTP 캐시 프로토콜을 그대로 쓰고, EIA 스프레드시트 배포는 그런
  헤더가 없으니 콘텐츠 자체를 비교한다.
- **결과: main의 raw 수집 지점 5곳(HVFHV/기사차량/리스인벤토리/EIA가스/EIA전력)
  전부가 dedup 처리돼 있다.** (`eia_fuel_price_silver`는 raw 수집이 아니라 이
  둘을 합치는 Silver 결합 단계라 별개다.)

### 발표 멘트 초안

> "지역이 늘고 원천이 늘어날수록 매번 원천을 다시 호출하는 비용도 늘어납니다.
> 저희는 이미 5개 수집 지점 전부에 재요청 방지를 넣어뒀습니다 — 내부 API 3종은
> ETag/Last-Modified 조건부 요청으로 '바뀌었는지'만 먼저 물어보고, EIA처럼 그런
> 헤더가 없는 원천은 받은 내용을 직전 파일과 직접 비교해서 같으면 버립니다.
> 두 방식을 원천 특성에 맞게 다르게 골랐다는 게 핵심입니다."

### 예상 질문 & 답변

- **Q. 두 메커니즘을 하나로 통일하지 않은 이유는?**
  A. ETag/Last-Modified는 원천이 그 헤더를 지원할 때만 쓸 수 있는 표준 캐시
  프로토콜이라 더 싸다(본문을 안 받아도 됨). EIA는 그 헤더가 없어 본문을 받아본
  뒤 비교하는 방식으로 갈 수밖에 없었다 — 통일이 아니라 원천 제약에 따른 선택.
- **Q. dedup이 실패하면(예: 원천이 실제로 바뀌었는데 못 잡으면) 어떻게 되나?**
  A. 두 메커니즘 모두 "확실히 같다"고 판단될 때만 건너뛰고, 애매하면 그냥
  다시 받는다 — 놓치는 대신 낭비하는 쪽으로 안전하게 설계돼 있다.

---

## 2. D — 데이터 특성에 맞춘 검증 강도 차등화

### 문제/계기

데이터가 늘어나면 "모든 데이터셋을 똑같이 무겁게 검증한다"는 전략은 유지비가
선형으로 늘어난다. 실제로 지금 코드를 보면 데이터셋마다 검증 강도가 이미
다른데, 이게 우연인지 설계인지 이번에 정리했다.

### 설계 결정 — 3단계 기준

| 데이터 특성 | 해당 데이터셋 | 검증 방식 |
|---|---|---|
| 고빈도·행 단위로 비즈니스 로직에 바로 들어감 | monthly_taxi_trip (HVFHV) | GX 기반 행 단위 값 검증(범위·enum) + severity 2단계(경고 1%, 차단 임계값) |
| 저빈도·참조성(룩업 테이블) | driver_vehicle_monthly_snapshot, lease_vehicle_inventory | 컬럼 존재·스키마 일치 확인 수준 |
| 하루 단위 시계열이 통째로 집계에 들어감 | eia_gas_price, eia_electricity_price | 달력 완전성 체크(그 달 일수만큼 다 채워졌는지, 중복 날짜 없는지) |

**정직하게 밝힐 것**: 이 3단계는 처음부터 이렇게 설계하고 시작한 게 아니라,
구현 과정에서 데이터 성격에 맞게 자연스럽게 나뉜 것을 이번에 명시적 원칙으로
정리한 것이다. "처음부터 이 기준으로 설계했다"고 말하면 안 되고, "지금
구현을 보고 원칙을 뽑아냈다"고 정직하게 말할 것.

### 발표 멘트 초안

> "데이터마다 위험도가 다릅니다. 매출·추천 로직에 바로 들어가는 운행 데이터는
> 행 단위로 값 범위와 enum까지 검증하고, 반대로 존재만 확인하면 되는 참조성
> 데이터는 스키마 확인 정도로 충분합니다. 이 구분을 이번에 명시적인 기준으로
> 정리했고, 뒤에 나올 이상치 탐지(E)도 이 기준에서 나온 지표를 그대로 재사용합니다."

### 예상 질문 & 답변

- **Q. 이 기준을 처음부터 의도하고 설계한 건가?**
  A. 아니다. 구현하면서 데이터 성격에 따라 자연스럽게 검증 강도가 갈렸고,
  이번에 그 패턴을 원칙으로 정리했다 — 사후 정리라는 걸 정직하게 인정하는 게
  맞다.
- **Q. 새 지역 데이터가 들어오면 이 중 어디에 속하는지 어떻게 정하나?**
  A. 그 데이터가 "행 단위로 매출/추천에 바로 들어가는가", "참조용 룩업인가",
  "시계열 완전성이 핵심인가"를 보고 세 카테고리 중 하나에 배정하면 된다 —
  이번 정리 덕분에 판단 기준 자체는 이미 있다.

---

## 3. B — 스키마 드리프트 3단계 대응 (설계, 코드 없음)

### 문제/계기

원천이 예고 없이 컬럼을 바꾼다. 지금은 스키마가 조금이라도 다르면 대부분
무조건 멈추거나(컬럼 누락) 아예 검사를 안 하는(컬럼 존재만 확인) 극단적인
두 상태만 있다. 지역이 늘면 원천마다 스키마 관례가 다를 수 있어 이 이분법이
버티기 어려워진다.

### 설계 결정

| 변경 종류 | 판정 | 동작 |
|---|---|---|
| 컬럼 추가(기대 밖 신규 컬럼) | 확장 | 기대 컬럼만 골라 통과, 신규 컬럼명은 Slack 경고로만 남김 |
| 필수 컬럼 소실 | 축소(필수) | 하드 차단 (현재와 동일) |
| 선택 컬럼 소실 | 축소(선택) | 차단하지 않고 경고만 남기고 진행 |

- 필수/선택 판정 기준: **Silver→Gold(또는 하류 Silver 결합) 변환 로직에서
  실제로 참조되는가.** 참조되면 필수, 아니면 선택.
- 이번 세션에서 6개 데이터셋 전부를 이 기준으로 실제 분류했다(아래).
- **필요 전제조건**: "선택 컬럼 소실 시 경고만 하고 진행"이 성립하려면 하류
  Spark/pandas 변환 코드가 그 컬럼이 없을 때 `KeyError` 대신 null로 채우는
  방어 로직을 갖춰야 한다. 이게 없으면 "경고만 하고 진행"이 실제로는 하류에서
  죽는 거짓 약속이 된다 — 구현 시 반드시 같이 넣어야 하는 부분.

#### 데이터셋별 필수/선택 컬럼 분류

- **monthly_taxi_trip (Silver, 14컬럼)** — 필수: `taxi_id`, `hvfhs_license_num`,
  `estimated_service_tier`, `trip_miles`, `driver_pay`, `tips`, `pickup_datetime`.
  선택: `on_scene_datetime`, `dropoff_datetime`, `PULocationID`, `DOLocationID`,
  `pickup_zone`, `dropoff_zone`, `trip_time`.
- **driver_vehicle_monthly_snapshot (15컬럼)** — 필수: `snapshot_month`,
  `driver_id`, `taxi_id`, `vehicle_model_id`, `manufacturer`, `model_name`,
  `fuel_type`, `comfort_eligible`, `extra_comfort_eligible`, `weekly_lease_fee`.
  선택: `join_date`, `exit_date`, `experience_years`, `vehicle_since`,
  `snapshot_created_at`.
- **lease_vehicle_inventory (11컬럼)** — 필수: `vehicle_model_id`, `manufacturer`,
  `model_name`, `model_year`, `fuel_type`, `fuel_efficiency`, `comfort_eligible`,
  `extra_comfort_eligible`, `weekly_lease_fee`, `stock`. 선택: `image_url`만.
- **eia_gas_price (3컬럼, eia_fuel_price_silver 결합 단계가 소비)** — 전부 필수:
  `date`, `gas_price`, `bronze_collected_date`.
- **eia_electricity_price (4컬럼, 같은 결합 단계가 소비)** — 전부 필수: `date`,
  `ev_price`, `bronze_collected_date`, `ev_price_status`.
- **eia_fuel_price/gas_ev_price (결합 결과, 6컬럼, Gold가 소비)** — 필수: `date`,
  `gas_price`, `ev_price`. 선택: `price_source`, `bronze_collected_date`,
  `ev_price_status`(결합 결과를 만드는 데만 쓰이고 Gold까지는 안 감).

### 발표 멘트 초안

> "지금은 스키마가 조금만 달라도 대부분 멈추거나, 아니면 컬럼이 있는지만
> 봅니다. 저희는 이걸 세 단계로 나누는 설계를 정리했습니다 — 컬럼이 늘면
> 통과, 실제로 계산에 쓰이는 필수 컬럼이 없어지면 차단, 안 쓰이는 선택
> 컬럼만 없어지면 경고만 하고 계속 진행합니다. 6개 데이터셋 전부에 대해
> 어떤 컬럼이 필수인지 실제 변환 코드를 추적해서 이미 분류까지 마쳤고,
> 다음 단계로 코드에 반영할 예정입니다."

### 예상 질문 & 답변

- **Q. 지금 구현돼 있나?**
  A. 아니다. 설계와 6개 데이터셋의 필수/선택 분류까지 이번에 확정했고, 코드
  반영은 다음 단계다 — 정직하게 "설계 단계"라고 답할 것.
- **Q. 필수/선택 기준이 자의적이지 않나?**
  A. "하류 변환 로직에서 실제로 참조되는가"라는 기계적으로 확인 가능한
  기준이다. 실제로 각 데이터셋의 Spark/pandas 변환 코드를 grep해서 분류했다.

---

## 4. C — Bronze→Silver 검증-후-커밋 (과거 설계)

> 2026-08-24 #912에서 이 staging copy+delete 설계를 폐기했다. 현재는 최종
> 경로에 직접 쓴 뒤 검증 성공 시에만 `_SUCCESS`로 공개한다.

### 문제/계기

지금은 대부분의 데이터셋에서 적재 태스크가 **검증을 통과하기도 전에** Silver
파일을 최종 이름으로 써버린다. 검증이 실패해도 이미 써진 파일은 지워지지
않고 그대로 남아, 다음에 그 달 데이터를 찾는 쪽(Gold 등)이 검증 안 된 데이터를
"최신"으로 집어갈 위험이 있다. 데이터량이 늘고 검증할 게 많아질수록 이 창(window)이
문제가 될 확률도 늘어난다.

### 설계 결정 — 이미 구현된 패턴을 6개로 확장

`main/airflow/common/monthly_bronze.py`에 이미 있는 `staged_silver_version_path`
/`commit_staged_silver`/`STAGED_FILE_PATTERN`(#742, monthly_taxi_trip에 적용
완료)을 나머지 5개로 확장하는 설계다. 조사 결과 6개 데이터셋의 Silver 쓰기
방식이 **두 그룹**으로 나뉘어, 그룹마다 확장 방법이 다르다.

| 그룹 | 데이터셋 | 파일명 방식 | 확장 방법 |
|---|---|---|---|
| 1 | driver_vehicle_monthly_snapshot, lease_vehicle_inventory | `<수집시각>.parquet` 타임스탬프 버저닝 | 기존 `staged_silver_version_path`/`commit_staged_silver`를 그대로 재사용 |
| 2 | eia_gas_price, eia_electricity_price, eia_fuel_price | 매달 같은 고정 파일명(`eia_gas_price.parquet` 등) 덮어쓰기 | `<파일명>.staged.parquet` 규칙을 새로 만들고, validate task가 (기존처럼 이전 태스크가 미리 계산해준 경로를 받는 게 아니라) **자기 xcom에서 staging 경로를 스스로 도출**하도록 배선을 바꿔야 함 |

- 그룹 2는 그룹 1과 배선 구조 자체가 다르다는 게 이번 조사의 핵심 발견이다 —
  단순히 "같은 패턴을 5번 복붙"이 아니라, 두 개의 서로 다른 확장 작업이 필요하다.
- `eia_fuel_price_silver`는 특히 실제 검증(`_validate_table`)이 지금 dry-run
  경로에서만 불리고, 실제 쓰기 경로에서는 별도 태스크(`validate_silver_task`)가
  쓰기가 끝난 뒤에 검증한다 — 다른 5개와 같은 종류의 순서 문제를 그대로 갖고 있다.

### 발표 멘트 초초안

> "저희는 이미 검증을 통과하기 전에는 최종 경로를 채우지 않는 원자적 커밋을
> 주력 데이터셋 하나에 적용했습니다. 나머지 5개도 같은 원칙으로 확장하는
> 설계를 마쳤는데, 조사해보니 데이터셋들이 파일 버저닝 방식이 두 그룹으로
> 갈려서, 그룹마다 다른 확장 방법이 필요하다는 것까지 미리 파악해뒀습니다."

### 예상 질문 & 답변

- **Q. 왜 6개를 한 번에 다 안 고쳤나?**
  A. 실제로 구현한 1개(monthly_taxi_trip)에서 나온 패턴이 나머지 5개에
  그대로 복붙되지 않는다는 걸 이번에 발견했다 — 두 그룹으로 나눠서 순서대로
  진행하는 게 더 안전하다고 판단했다.
- **Q. 검증 실패 시 데이터가 어떻게 남나?**
  A. staging 이름(예: `.staged.parquet`)으로만 남고, 이 이름은 "최신 버전"을
  찾는 로직의 정규식과 안 겹치게 설계되어 있어 하류가 절대 집어가지 않는다.

---

## 5. E — 월별 이상치(회귀) 탐지 (설계, 코드 없음)

### 문제/계기

지금 모든 검증은 **고정된 절대 기준**과의 비교뿐이다(GX 임계값, 달력 일수).
"이번 달 값이 과거 몇 달과 비교해 이상한가"를 보는 장치가 전혀 없다. 지금은
사람이 대시보드를 보고 감으로 이상함을 느낄 수 있지만, 지역·데이터가 늘어나면
사람이 매달 모든 데이터셋을 다 훑어볼 수 없게 된다.

### 설계 결정 — Bronze/Silver와 Gold를 다르게 취급

**Bronze/Silver 계층** — 지금은 과거 지표를 쌓아두는 곳이 전혀 없다(GX Data
Docs는 정적 리포트, 히스토리가 아님). 원본을 매번 다시 스캔해서 과거 값을
재계산하면 데이터가 늘수록 이 검사 자체가 비싸진다는 역설이 생긴다. 그래서:

- 검증 통과 시점마다 그 데이터셋의 지표(D에서 이미 계산되는 값 — 아래 표) 한 줄을
  `data/quality_history/<dataset>.jsonl`에 append(`{year_month, metric_name, value}`).
  새 DB·테이블 없이 파일 하나로 해결되고 로컬 실행 제약과도 맞는다.
- D의 지표를 그대로 재사용한다 — 새 지표를 만들지 않는다.

| 데이터셋 | 감시 지표 (D에서 재사용) |
|---|---|
| monthly_taxi_trip | `invalid_required_row_ratio` |
| driver_vehicle_monthly_snapshot, lease_vehicle_inventory | `row_count` |
| eia_gas_price, eia_electricity_price | 달력 완전성 비율 |

**Gold 계층** — Silver→Gold는 추천 KPI를 계산하지 않고 수익 시뮬레이션과 재고 원본을
분리해 적재한다. 추천 KPI 이상 탐지는 최종 추천을 계산하는 소비 계층의 책임으로 둔다.

**판정 규칙 (Bronze/Silver·Gold 공통)**:

1. 누적 개월 수가 **3개월 미달**이면 비교 자체를 하지 않는다(그냥 쌓기만 함).
2. 3개월 이상부터 `window = min(누적 개월 수, 12)`개월의 평균·표준편차를 계산.
3. 이번 달 값이 **평균 ± 2표준편차**를 벗어나면 Slack **경고만** 보낸다 —
   차단은 하지 않는다. 이상치는 "원천이 이상하게 바뀌었을 수 있다"는 신호일
   뿐 확정된 불량이 아니므로, 잘못된 이상탐지로 정상 데이터를 막으면 안 된다.

### 발표 멘트 초안

> "지금 검증은 전부 고정된 절대 기준과의 비교뿐입니다. 데이터가 늘어나면
> 사람이 매달 모든 지표를 다 훑어볼 수 없어지기 때문에, 각 데이터셋이 이미
> 만들어내는 품질 지표를 매달 가벼운 파일에 쌓아두고, 최근 최대 12개월(데이터가
> 쌓이는 대로 자동으로 늘어남) 평균에서 크게 벗어나면 경고만 보내는 설계를
> 준비했습니다. Gold는 추천 계산 전의 수익 시뮬레이션과 재고를 분리해 제공하고,
> 최종 추천 KPI 감시는 이를 계산하는 소비 계층에서 맡도록 경계를 정했습니다."

### 예상 질문 & 답변

- **Q. 지금 데이터가 1개월치도 안 되는데 의미가 있나?**
  A. 지금은 의미가 없다는 걸 인정한다. 그래서 최소 3개월이 쌓이기 전엔
  비교 자체를 안 하도록 설계했다 — 데이터가 없을 때 억지로 판정을 내리지
  않는 게 핵심이다.
- **Q. 표준편차 2배라는 기준은 어떻게 정했나?**
  A. 실측이 아니라 통상적인 이상탐지 관례값이다. 실제 데이터가 쌓이면
  재보정이 필요할 수 있다는 걸 정직하게 인정할 것.
- **Q. 왜 항상 경고만 하고 차단은 안 하나?**
  A. 이상치가 "원천이 실제로 바뀐 정상적인 변화"일 수도 있어서, 잘못
  차단하면 정상 데이터가 막힌다. 사람이 보고 판단하게 하는 게 더 안전하다.

---

## 6. F — 지역 축 파티셔닝 (F-5 경로 계층 구현 완료)

### 2026-08-23 구현 상태 갱신

F-5 하위 이슈 `#839`, `#851`, `#840`~`#849`를 통해 main 파이프라인의
Bronze·Silver·Gold 경로와 대시보드 조회에
`service_area=<sa>/year_month=<ym>` 계층을 반영했다. EIA Bronze는 월 대신
수집일을 축으로 쓰므로 `service_area=<sa>/collected_date=<date>` 순서를 사용한다.

최종 정리 `#849`에서 경로 함수의 `service_area`를 필수로 바꾸고,
지역 경로가 없을 때 비지역 예전 경로를 읽던 이중 탐색을 제거했다. 따라서
지역 누락은 다른 지역이나 예전 데이터로 대신되지 않고 명시적으로 실패한다.
로컬에 남은 `hvfhv`, `gas_price` 등 비활성 예전 디렉터리는 삭제하지 않았지만,
현재 코드는 그 경로를 읽지 않는다.

### 문제/계기

이슈 `#674`에서 팀이 이미 4가지 NYC 종속 지점을 확인해뒀다.

1. `main/aws_lambda/functions/eia_gas_price_raw_to_bronze/extractor.py:22` —
   EIA 유가 시리즈 URL이 뉴욕 시리즈(`EMM_EPMR_PTE_SNY_DPGw`)로 고정.
2. Bronze/Silver/Gold 전부 파티션 축이 `year_month` 하나뿐, 지역 축이 없음.
3. `main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py`의
   `PartitionedAssetTimetable`(`assets.GOLD_INPUTS`)이 지역 단위 트리거를
   표현하지 못함.
4. HVFHV Silver의 `PULocationID`/`DOLocationID`/`pickup_zone`/`dropoff_zone`이
   TLC 265구역 택시존 체계에 종속 — 다른 도시/주는 이 체계가 없다.

`#674`는 "확장 시점이 미정이라 설계 대신 논의만 남긴다"고 결론냈었다. 이번엔
C3(Gold의 비동기 다중소스 트리거)를 지역 축까지 확장하면 위 1~3번이 어떻게
풀리는지 구체적으로 설계했다. **4번은 이번 설계로도 못 푼다** — 아래에서
왜 못 푸는지, B와의 관계까지 정직하게 밝힌다.

### 설계 결정

**핵심 통찰**: 3번이 실제로 문제인 것은 `PartitionedAssetTimetable`/
`IdentityMapper` 자체가 아니라, 그 안에 흘러가는 **파티션 키 문자열이
`year_month` 하나뿐**이라는 점이다. 파티션 키를 `"{service_area}:{year_month}"`
복합 문자열(예: `"NYC:2026-08"`)로 바꾸면, Asset이나 Timetable 구조는 손대지
않고도 지역별로 완전히 독립된 파티션으로 취급된다 — Gold는 `"NYC:2026-08"`의
4개 Silver가 다 준비되면 그때 트리거되고, `"TX:2026-08"`은 그것과 무관하게
자기 소스들이 준비될 때 따로 트리거된다. **리전마다 새 DAG를 만들 필요가
없다.**

0. **이름은 `service_area`. `region`을 쓰면 안 된다.** `region`은 이미 AWS
   리전으로 점유돼 있다 — `region_name=os.getenv("AWS_DEFAULT_REGION", ...)`
   (`monthly_taxi_trip_silver_to_gold_dag.py:124`,
   `monthly_taxi_trip_raw_to_silver_dag.py:161`), `.env.example:13`의
   `AWS_REGION`, 6개 워크플로 중 5개의 `aws-region:`. 같은 오퍼레이터 호출
   안에 `region="NYC"`와 `region_name="ap-northeast-2"`가 나란히 놓이는 배치는
   3am에 사고를 만든다. 대안은 `service_area`(채택) / `market` / `city`.
1. **파티션 키**: `year_month` 단일값 → `"{service_area}:{year_month}"` 복합 문자열.
   `resolve_target_year_month`/`silver_version_path` 등 파티션 키를 파싱하는
   함수들이 이 형식을 나눠 읽도록 변경.
2. **저장 경로**: `year_month=<ym>` → `service_area=<sa>/year_month=<ym>` 계층을
   한 단계 추가. 같은 지역 안에서는 기존 `year_month=*` glob 로직이 그대로
   동작한다. **단 EIA 3종은 파티션 축이 달라 이 규칙이 그대로 안 먹힌다** —
   아래 별도 절 참고.
3. **Gold 스키마**: `DriverMonthlyProfit`/`DriverVehicleProfitSimulation`/
   `LeaseVehicleInventory`에 `service_area` 컬럼을 두고, PK를
   `(service_area, year_month, version[, driver_id|vehicle_model_id])`로 구성.
4. **지역별 설정 레지스트리**: `main/airflow/common/service_areas.py` 신설 제안 —
   지역 코드 → {EIA 가스/전력 시리즈 URL, EIA 파일명, 택시존 스키마 참조} 매핑.
   지금은 `NYC` 하나만 등록. 새 지역을 추가한다는 건 "이 레지스트리에 항목을
   추가하는 일"이 된다(단, 4번 문제가 해결된 지역만 등록 가능).
5. **실행 동시성**: 파티션의 독립성과 실행 상한은 별도 계약이다. 지금 설정은
   서로 다른 세 지역 DagRun까지 허용하고 네 번째부터 대기시킨다 — 아래 별도 절 참고.
6. **`service_area`는 데이터로만 흘린다 — 설정(env var)으로 만들면 안 된다.**
   지금 경로 env var 5종(`BRONZE_DIR`, `SILVER_DIR`, `GOLD_DIR`,
   `DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER_DIR`,
   `LEASE_VEHICLE_INVENTORY_SILVER_DIR`)은 Airflow 쪽에서 **모듈 import 시점
   상수**로 읽히므로(`monthly_taxi_trip_raw_to_silver/tasks.py:49,52` 등)
   실행마다 바꿀 수 없다. 누군가 "`SERVICE_AREA` env var 추가"로 해결하면 배포
   전체가 한 지역에 고정된다. 다행히 이 env var들은 *데이터셋 루트*를 가리키고
   `service_area=`는 그 아래로 들어가므로 **env var 자체는 손댈 필요가 없다.**

### 5번(실행 동시성) — 이 설계의 가장 큰 실무 제약

**핵심 통찰의 이면**: "리전마다 새 DAG를 만들 필요가 없다"는 게 이 설계의
장점인데, 바로 그래서 **모든 리전이 한 DAG의 DAG run으로 들어온다.** 그리고
현재 지역 파티션을 받는 `main/` DAG 8개는 공용 계약
`MAX_ACTIVE_SERVICE_AREA_RUNS=3`을 사용한다. Executor는 단일 노드
LocalExecutor(`docker-compose.yml:50`)이고 전역 `parallelism`은 기본값 32다.

결과: `"NYC:2026-08"`과 `"TX:2026-08"`은 Airflow 입장에서 서로 다른 파티션이지만
세 지역까지는 **물리적으로 병렬 실행**되고 네 번째 지역부터 큐에 줄을 선다.
파티션의 논리적 독립과 실행 상한 3개는 별도 계약이며, `max_active_runs`가 지역
DagRun 자체를 만들어 주는 것은 아니다.

Bronze→Silver와 Silver→Gold의 EMR 오퍼레이터, 하위 DAG를 기다리는 TriggerDagRun
오퍼레이터는 모두 `deferrable=True`다. 외부 실행을 기다리는 동안 triggerer로
넘어가 worker slot을 반환하므로, 지역 수만큼 장시간 폴링 슬롯을 확보할 필요는 없다.

| 선택지 | 비용 | 판단 |
|---|---|---|
| `max_active_runs=3`, `parallelism=32` 유지 | 세 지역까지 병렬, 네 번째부터 대기 | **현재 채택** |
| `parallelism` 상향 | 단일 EC2에서 scheduler와 CPU·메모리 경쟁 증가 | queued 병목을 실측한 뒤 검토 |
| Executor를 Celery/Kubernetes로 교체 | 로컬 실행 제약과 충돌, 운영 복잡도 급증 | 이번 범위 밖 |

**결정**: 세 지역 규모까지는 `max_active_runs=3`과 기존 `parallelism=32`로 간다.
worker slot 증설과 Executor 교체는 실제 queued 지연이 확인되기 전에는 과잉이다.

### 이 설계로 **손댈 게 없는** 것 — Silver 단일 파일 적재

리전 축을 넣을 때 Silver 물리 파일 구조(`SingleParquetFileLoader`,
`main/spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/job.py:95`)는
**변경이 필요하지 않다.** 확인한 근거:

- Loader는 최종 경로를 문자열로 그대로 받는다 — 경로에 `service_area=<sa>/` 계층이
  끼어도 Loader는 그걸 해석하지 않는다.
- 수집 시각 파일명 검증(`TIMESTAMP_FILE_PATTERN`, `job.py:271-279`)은 **파일명만**
  본다. 디렉터리 부분을 검사하지 않으므로 리전 계층에 영향받지 않는다.
- 검증-후-커밋 승격(C, #742)은 같은 디렉터리 안의 이름 변경/복사라 리전과 무관하다.
- `latest_partition_file`(`job.py:185`)은 주어진 `input_path` 아래에서
  `year_month=` 를 찾으므로, `input_path`가 `.../monthly_taxi_trip/service_area=NYC`로
  한 단계 길어지는 것만으로 그대로 동작한다.

**단, 예외 한 곳**: `latest_partition_files`(`job.py:201-209`)는 데이터셋 루트에서
`year_month=????-??` 를 **한 레벨만** glob한다. `service_area=` 계층이 들어가면 아무것도
못 찾는다. 이 함수는 `--input_path`도 `--start/end_year_month`도 주지 않은 로컬
편의 경로에서만 쓰이고 DAG는 항상 xcom으로 명시 파일 경로를 넘기지만
(`monthly_taxi_trip_raw_to_silver_dag.py:110-116`), F 구현 시 이 glob 깊이는
같이 고쳐야 한다.

**왜 이걸 문서에 적어두나**: "단일 parquet 적재가 나중에 멀티리전의 걸림돌이
되지 않겠나"는 검토를 실제로 돌렸고, 걸리지 않는다는 결론이 나왔기 때문이다.
멀티리전은 파일 하나의 크기를 키우는 게 아니라 파일 개수를 늘리므로, 단일 파일
쓰기 비용은 리전 수와 무관하다. 미리 구조를 바꾸지 않기로 한 근거를 남겨,
나중에 같은 검토를 반복하지 않게 한다. (단일 파일 쓰기가 실제로 아파지는 건
리전 축이 아니라 **한 리전의 한 달이 커질 때** — 아래 별도 항목 참고.)

### 리전 축과 무관하지만 같이 알아둘 것 — 데이터량 축의 한계

지금 Silver 적재 경로에는 리전과 무관한 성능 한계가 하나 있었다(**#818로 해결됨**).
F를 구현하기 전에 고칠 필요는 없었지만, "리전이 늘어서 터진 것"으로 오진하지
않으려면 구분해둘 가치가 있었다.

- `payload.coalesce(1)` — 이 잡의 변환은 셔플이 없어서(`transformer.py`가
  select/filter만 씀) `coalesce(1)`이 상류로 전파돼 Bronze read부터 write까지
  전 구간이 단일 task로 돌았다. `repartition(1)`로 바꿔 셔플 경계를 만들어
  변환은 병렬로 돌고 write 1 task만 직렬이 되도록 고쳤다(파일은 그대로 하나).

현재 합성 원천(월 20만 행 규모)에서는 안 드러나고, 실 NYC HVFHV(월 ~2000만
행)로 갈아탈 때 체감되는 축이라 실패가 아니라 성능 이슈였다.

**정정**: 이전 버전의 이 문서는 "S3 승격의 `client.copy`(`job.py:134`)가 5GB를
넘으면 실패한다"고 적었으나, 이는 틀린 정보였다. boto3 소스(`boto3/s3/inject.py`
`copy()`)와 `s3transfer.copies.CopySubmissionTask._submit`을 직접 읽어 확인한
결과, 여기서 실제로 쓰이는 `client.copy(...)`는 저수준 `copy_object`(5GB 제한
있음)가 아니라 **boto3의 managed transfer**다 — 크기가
`TransferConfig.multipart_threshold`(기본 8MB)를 넘으면 자동으로 멀티스레드
`UploadPartCopy`로 전환해 S3 최대 객체 크기(5TB)까지 문제없이 처리한다.
당시 `main/airflow/common/monthly_bronze.py:81`의 `client.copy`도 동일한 managed
transfer였다. #912 이후 해당 copy 경로 자체가 제거됐다.

### 구현 시 함께 고쳐야 하는 지점 — 조용히 틀린 값을 만드는 것

여기가 이 설계의 실무 핵심이다. 아래는 코드를 읽어 확인한 목록이며, **전부
"실패하지 않고 틀린 값을 만드는" 부류**다. 알림도 안 가고 DAG는 초록불이므로,
구현 시 하나라도 빠뜨리면 발견이 분기 마감으로 밀린다.

| # | 지점 | 지역 2개일 때 벌어지는 일 |
|---|---|---|
| 1 | `source_api_refresh/tasks.py:26,178,208` — dedup 상태 키가 `source_api_processed__{dataset}` | **지역이 서로를 굶긴다.** NYC가 ETag 기록 → TX가 304 수신 → Bronze 존재 확인(`:34-53`)마저 지역을 안 봐서 NYC 파티션을 자기 것으로 착각 → **수집 통째로 skip.** 실패가 아니라 성공적 skip이라 알림도 없다. 그다음엔 TX가 키를 덮어써서 NYC가 굶는다 |
| 2 | `assets.py:18-26` `publish_month_partition` — 생산자가 bare `year_month` 발행 (`source_api_refresh/tasks.py:234`, `eia_fuel_price_silver/tasks.py:209`) | Gold가 `"NYC:2026-08"`을 기다리는데 생산자가 `"2026-08"`을 발행하면 **Gold가 영원히 트리거되지 않는다.** 에러도 없는 완전한 무음. 소비자 쪽만 고치면 파이프라인 전체가 멈춘 채 초록불 |
| 3 | `postgres_loader.py:32-35,58-71,74-104` — `_next_version`·`_PRIMARY_KEYS`·`_validate_written_rows` 전부 `year_month`만 봄 | 버전이 지역 간 공유 카운터가 된다(TX 첫 적재가 v2). **셋을 반드시 같이 고쳐야 한다** — PK만 빼면 IntegrityError, 검증만 빼면 다른 지역 행을 세서 매번 롤백. *일부만 고치면 안 고친 것보다 나쁘다* |
| 4 | `dashboard/datasource.py:47-52` — `MAX(version) WHERE year_month = t.year_month` 상관 서브쿼리 | NYC v3, TX v1이면 서브쿼리가 둘 다 3을 반환 → **TX 행이 대시보드에서 전부 사라진다.** `:67`이 컬럼을 dataclass에서 자동 유도하므로 컬럼 추가만으론 에러도 안 난다 |
| 5 | `dashboard/app.py:191` `.iloc[0]`, `:60` `merge(on=["driver_id","year_month"])` | 헤드라인 지표가 **아무 지역이나 하나 집어온다**(동전 던지기). **`driver_id`는 지역 간 유니크하지 않음이 확인됐다**(#805) — `build_driver_ids()`가 `SD0000`~`SD1999`를 지역 성분 없이 만든다. 조인이 fan-out돼 모든 집계가 부풀어 오르므로 조인 키에 `service_area`를 넣어야 한다 |
| 6 | `silver_to_gold/job.py:133-137` `latest_fuel_price_path` S3 — `max(keys)`가 전체 서브트리 사전순 비교 | `service_area=TX`가 사전순 뒤라 **월과 무관하게 TX 유가가 NYC Gold에 들어간다.** EIA 시리즈는 원래 주(州)별이라 진짜 wrong-value 경로 |
| 7 | `silver_to_gold/job.py:148-149` `_csv_path` | 로컬 모드에서 **두 지역이 서로의 Gold CSV를 덮어쓴다.** `validate_gold_outputs`(`tasks.py:286-295`)는 마지막에 쓴 지역을 검증 |
| 8 | `monthly_taxi_trip_raw_to_silver/tasks.py:226-241` `existing_silver_partitions` (#165 가드) | 로컬·S3 양쪽 모두 **조용히 `[]` 반환** → #165 파티션 소실 가드가 공허하게 통과. 가드가 죽은 걸 아무도 모른다 |
| 9 | `monthly_bronze.py:43` S3 파생 브랜치 — `base.name`으로 키를 처음부터 재구성 | `base`가 `.../monthly_taxi_trip/service_area=NYC`면 `base.name`이 `"service_area=NYC"` → 키가 `silver/service_area=NYC/...`가 되어 **데이터셋 디렉터리가 사라진다** |
| 10 | `source_api_refresh_dag.py:74-78` `trigger_run_id` + `reset_dag_run=True(:90)` | run_id에 지역이 없어서, 두 지역이 같은 version 해시를 만들면 **한 지역이 다른 지역의 DagRun을 리셋한다** |
| 11 | `bronze_to_silver/.../job.py:281` `SparkParquetLoader(partition_by=["year_month"])` | `partitionOverwriteMode=dynamic`의 #165 보호가 `partition_by`에 든 축까지만 적용된다. 지역이 경로에만 있고 `partition_by`에 없으면 **지역 간 #165(다른 지역 파티션 소실)가 다시 열린다** |

### 구현 시 함께 고쳐야 하는 지점 — 요란하게 죽는 것

이쪽은 위험도가 낮다(배포하면 바로 안다). 다만 메시지가 오해를 유발하는 게
많아, 원인을 미리 알아두면 디버깅 시간이 줄어든다.

| 지점 | 증상 |
|---|---|
| `monthly_taxi_trip_silver_to_gold/tasks.py:114-116` `strptime(partition_key, "%Y-%m")` | 복합 키 변경의 **가장 직접적 casualty** — `strptime("NYC:2026-08", ...)`가 즉시 `ValueError`. `:` 로 먼저 쪼개야 한다 |
| `monthly_bronze.py:92-115` `validate_monthly_parquet_bronze` | `path.parent.parent.name == dataset_dir` 3계층 단정이 깨져 "월 파티션 계약과 다릅니다"로 죽음. 3→4계층 단정으로 고쳐야 함 |
| `available_year_months:58`, `latest_partition_file`(silver_to_gold `job.py:104,111` — bronze_to_silver의 동명 함수와 **별개**), EIA `bronze_partitions:92-110` | 한 레벨 glob이라 못 찾고 "파티션이 없습니다"로 죽음. 파티션이 한 단계 깊어졌을 뿐인데 없다고 말하므로 메시지가 오해를 부른다 |
| Gold Postgres 첫 운영 실행 | `_create_table_sql`이 `CREATE TABLE IF NOT EXISTS`라 기존 테이블에 컬럼이 **안 붙는다** → `INSERT`가 `UndefinedColumn`으로 죽음. 아래 마이그레이션 절 참고 |

한 가지 **유용한** 요란함: `lease_vehicle_inventory_bronze_to_silver/handler.py:24-26`의
`YEAR_MONTH_PATTERN.fullmatch`가 복합 키가 안 쪼개진 채 내려오면 잡아준다 —
`year_month=NYC:2026-08` 디렉터리가 조용히 만들어지는 것보다 훨씬 낫다.
**이 가드는 유지하고, `service_area` 형식 가드도 대칭으로 추가할 것.**

### EIA 3종은 파티션 축이 다르다 — 단일 규칙이 안 먹히는 예외

`main/aws_lambda/common/eia_fuel_price_layout.py:31`, 사유는 `:9-20` 주석:
**EIA Bronze는 `collected_date=`로 파티션된다, `year_month=`가 아니다.** 한 파일에
26년치 이력이 들어있어 "그 파일의 월"이라는 개념이 없기 때문이다. 따라서
`service_area=`를 넣는 모양이 다른 3종과 다르다 —
`<dataset>/service_area=<sa>/collected_date=<d>/<file>`.

덤으로 **지역이 URL이 아니라 파일명에도 박혀 있다** — `GAS_FILE_NAME =
"gasoline_weekly_ny.xls"`(`:34`). `bronze_s3_prefix`/`newest_bronze_s3_key`가
`key.endswith(f"/{file_name}")`로 필터하므로, 지역별 파일명을 extractor URL뿐
아니라 이 두 곳에도 같이 흘려야 한다.

주의할 실패 모드: `is_duplicate_of_newest`(`:134-148`)는 `base_dir`가 지역 단위로
스코프되면 정상 동작하지만, `base_dir`가 지역 무관인 채 `service_area=`가 그
아래로 들어가면 `newest_bronze_partition`이 아무것도 못 찾아 `None`을 반환 →
**dedup이 조용히 스스로 비활성화된다**(매번 새 파티션을 쓰지만 에러는 없음).

### Gold Postgres 마이그레이션 — 도구가 아예 없다

- `_create_table_sql`(`postgres_loader.py:45-56`)은 `CREATE TABLE IF NOT EXISTS`라
  **이미 배포된 테이블에는 no-op**이다. dataclass에 컬럼을 더해도 실제 컬럼은 안 생긴다.
- PK 변경도 `IF NOT EXISTS`로는 불가 — `ALTER TABLE … DROP CONSTRAINT … ADD
  PRIMARY KEY (service_area, year_month, version…)`가 필요하다.
- **레포 전체에 `ALTER TABLE`이 0건, Alembic도(Airflow 자체 메타DB 제외) `.sql`도
  마이그레이션 파일도 없다.** 즉 손으로 쓴 SQL을 배포 *전에* 수동 실행하는 것이
  유일한 경로이고, 기존 행에는 `DEFAULT 'NYC'` 백필이 필요하다.
- `docs/`에 Gold DDL을 다루는 문서가 하나도 없다(`CREATE TABLE`/`PRIMARY KEY`
  언급 0건). **런북 문서를 새로 써야 한다.**

반대로 **Bronze/Silver는 마이그레이션할 게 없다.** 디스크의 `year_month=` 데이터가
전부 리네임 전 죽은 데이터셋명(`hvfhv`, `hvfhv_driver_trip`) 아래라 지금 코드가
읽지 않는다 — 옛 레이아웃 경로를 읽다가 404 날 곳이 없다.

### 관찰가능성 — 공짜로 얻는 것과 지금 없는 것

**공짜**: Airflow가 이미 `partition_key`를 콜백 컨텍스트에 넣어준다
(`airflow/sdk/execution_time/task_runner.py:337`, 콜백에 같은 컨텍스트가 전달됨).
Slack 템플릿에 `{{ partition_key }}` 한 줄씩 추가하면 지역 귀속이 끝난다.
**배선 비용 0.**

**지금 없는 것**: 6개 Slack 템플릿 전부에 지역 정보가 없다
(`slack_failure_callback.py:26,37,48,58,68` + Block Kit 변형). F 설계가 "지역마다
DAG를 안 만든다"이므로 **N개 지역이 한 DAG로 들어오는데, 온콜이 어느 지역이
죽었는지 알 방법이 없다.** `slack_quality_warning.py:16`도 `dataset`/`year_month`만
받는다.

EMR 잡 이름도 문제다 — `monthly_taxi_trip_raw_to_silver_dag.py:136`은
`ds_nodash`(날짜만)라 **같은 날 모든 지역이 동일한 잡 이름**이 되어 콘솔에서
구분이 안 된다. (Gold 쪽 `:102`의 `run_id`는 #746에서 의도적으로 고친 것이니
그 방식을 따르는 게 맞다.)

`docs/AIRFLOW_OPS.md` §5가 알림 내용을 "DAG · Task · Run ID · 시도 횟수 · 로그"로
못박고 있어 **같이 갱신해야 한다.**

### 작업 규모

| 항목 | 규모 |
|---|---|
| Param 배선 hop | 데이터셋당 **7 hop / 5파일**(DAG params → task context → Lambda event → handler → loader, ×2 for bronze/silver). 전체 **~25-30개 지점, ~15파일** |
| 테스트 | `main/airflow` 405개, `main/spark` 71개 기준. **하드 블로커 3개** ↓ |

하드 블로커:

1. **`test_main_dag_params.py:54-65`** — `set(dag.params) == expected` 완전일치를
   **8개 DAG에 parametrize**. `service_area` Param을 추가하면 8케이스가 전부
   깨지므로 `DAG_PARAMS` 전 항목을 같이 고쳐야 한다. (이 테스트가 #743 변경을
   병합에서 유실시킨 바로 그 계약이다 — 같은 함정을 두 번 밟지 말 것.)
2. **해결됨 — `max_active_runs == 3` 계약** — 지역 파티션을 받는 main DAG 8개와
   관련 계약 테스트가 공용 상한 3을 강제한다. `test_dag_concurrency.py`도
   `service_area`로 격리된 서로 다른 지역이라는 근거를 명시한다.
3. **`test_monthly_taxi_trip_silver_to_gold_dag.py:89`** —
   `IdentityMapper.to_downstream("2026-05") == "2026-05"`. IdentityMapper는
   항등이라 복합 키로도 통과하지만 **더 이상 아무것도 검증하지 않는다.** 복합 키
   케이스를 추가해야 하고, 하드코딩된 `partition_key="2026-05"` 7곳
   (`:149,226,241,264,433,452,548`)도 같이 손봐야 한다.

그밖에 `test_source_api_refresh_dag.py:286`이 trigger conf를 정확한 3키 dict로
단정하므로, conf에 `service_area`를 더하면 함께 깨진다.

### 4번(택시존 스키마)이 안 풀리는 이유 — B와의 관계

흥미로운 부분: **B(스키마 드리프트 3단계) 조사에서 `PULocationID`/
`DOLocationID`/`pickup_zone`/`dropoff_zone`을 이미 "선택 컬럼"으로 분류해뒀다**
(하류 변환 로직에서 안 쓰임). 그래서 새 지역의 트리거 데이터에 이 컬럼들이
**아예 없는** 경우는 B의 "선택 컬럼 소실 → 경고만 하고 진행"이 이미 커버한다.

**그런데 완전히 풀리는 건 아니다.** 새 지역이 zone 컬럼이 "없는" 게 아니라
**위경도처럼 다른 방식으로 위치를 표현**하면, 이건 "컬럼이 없다"가 아니라
"같은 이름의 컬럼이 다른 의미"인 문제라 B의 필수/선택 분류 모델로는 못 잡는다.
이 경우는 지역별 스키마 어댑터(원본 위치 표현 → 공통 Silver 스키마 변환기)가
따로 필요하고, 이번 설계 범위 밖이다. **발표에서는 "1~3번은 이 설계로 풀리고,
4번은 B가 부분적으로만 완화하며 완전히는 안 풀린다"고 정직하게 말할 것.**

### 발표 멘트 초안

> "지역이 늘어난다는 걸 대비하려면 Gold가 지역별로 독립적으로 준비 여부를
> 감지해야 합니다. 저희는 이걸 새로운 트리거 구조를 만드는 대신, 지금 쓰는
> 비동기 트리거의 파티션 키에 지역을 얹는 것으로 풀 수 있다는 걸 설계했습니다
> — DAG나 Asset 구조를 새로 안 만들어도, 지역마다 서로를 기다리지 않고
> 트리거됩니다. 저장 계층도 확인해봤는데, Silver 파일 구조는 손댈 게
> 없었습니다 — 지역이 늘어난다는 건 파일이 커지는 게 아니라 개수가 늘어나는
> 거라서요. 대신 정직하게 말씀드리면, 지금 설정으로는 지역들이 *독립적으로*
> 트리거되긴 하지만 *동시에* 실행되지는 않습니다. 그 지점과 택시존 스키마
> 문제는 남겨진 과제로 밝히고 있습니다."

**멘트 주의**: "독립적으로 처리된다"는 표현을 쓸 때 *논리적 독립*(서로의 준비를
기다리지 않음)까지만 주장하고 *물리적 병렬*로 들리게 하지 말 것. 심사위원이
"동시에 도나요?"라고 물으면 아래 Q&A대로 답한다.

### 예상 질문 & 답변

- **Q. 지금 당장 다지역이 되나?**
  A. 아니다. 설계만 나온 상태고 코드 반영은 안 됐다. `#674`도 이 설계를
  반영해 갱신했다.
- **Q. `PartitionedAssetTimetable` 자체를 안 바꿔도 되는 게 맞나?**
  A. 맞다. Airflow의 asset 파티션은 파티션 키를 그냥 불투명한 문자열로
  다룬다. `"NYC:2026-08"`과 `"TX:2026-08"`은 이미 서로 다른 파티션으로
  취급되므로, Timetable/Mapper 코드는 그대로 두고 파티션 키를 만드는
  쪽(각 Silver DAG)과 읽는 쪽(Gold DAG)만 복합 키를 이해하게 바꾸면 된다.
- **Q. 그럼 왜 진작 안 했나?**
  A. `#674`를 처음 발행할 때는 확장 시점이 미정이라 설계 자체가 과잉이라고
  판단했었다. 이번에 구체적으로 설계해보니 비용이 생각보다 크지 않다는 걸
  알게 됐고, 그래서 이슈를 갱신했다.
- **Q. 리전이 10개면 10개가 동시에 도나?**
  A. **아니다. 세 지역까지 동시에 돌고 나머지는 큐에서 기다린다.** EMR와 하위
  DAG 대기는 이미 `deferrable=True`라 worker slot을 장시간 점유하지 않는다.
  네 지역 이상이 필요하면 queued 지연과 EC2 자원을 실측해 상한과 Executor를
  다시 결정한다.
- **Q. Silver 파일 구조는 안 바꿔도 되나? 단일 parquet에 쓰는 게 나중에 걸리지 않나?**
  A. 검토했고 안 걸린다. 멀티리전은 파일 하나를 키우는 게 아니라 개수를
  늘리므로, 단일 파일 쓰기 비용은 리전 수와 무관하다. Loader는 경로를 문자열로
  받고 파일명 패턴만 검사하므로 `service_area=` 계층에 영향받지 않는다 — 실제로
  코드를 대조해서 diff가 0이라는 걸 확인했다. 예외는 `latest_partition_files`의
  glob 깊이 한 곳뿐이다. 단일 파일 쓰기가 아파지는 건 리전이 늘 때가 아니라
  한 리전의 한 달이 커질 때(실 HVFHV 월 2000만 행 규모)이고, 그건 별도 항목으로
  분리해뒀다.
- **Q. 왜 `region`이 아니라 `service_area`인가?**
  A. `region`은 이미 AWS 리전으로 쓰이고 있어서다 — 같은 오퍼레이터 호출 안에
  `region="NYC"`와 `region_name="ap-northeast-2"`가 나란히 놓이면 사고가 난다.
  이름 하나로 막을 수 있는 혼동은 이름으로 막는 게 맞다고 판단했다.
- **Q. 설계는 깔끔한데, 실제로 고칠 게 몇 군데인가?**
  A. 정직하게 많다. Param 배선만 **~25-30개 지점 / ~15파일**이고, 그와 별개로
  "조용히 틀린 값을 만드는" 지점 11개를 코드 대조로 찾아 목록화했다. 그중 3개는
  **일부만 고치면 안 고친 것보다 나쁘다**(Gold Postgres의 버전·PK·검증 3함수).
  설계가 "파티션 키에 한 축을 더한다"로 간단해 보이는 것과 실제 구현 범위는
  다르다는 걸 미리 파악해둔 게 이번 조사의 성과다.
- **Q. 지역을 늘리면 어느 지역이 실패했는지 알 수 있나?**
  A. 지금 Slack 템플릿 6개엔 지역 정보가 없어서 모른다. 다만 Airflow가 이미
  `partition_key`를 콜백 컨텍스트에 넣어주고 있어서, 템플릿에 한 줄씩 추가하는
  것만으로 해결된다 — 배선 비용이 0인 지점이라 F 구현의 첫 항목으로 잡아뒀다.

---

## 7. 확인된 사실 vs 확인이 더 필요한 것

### 확인된 사실 (코드/이슈로 근거 있음)

- A의 두 dedup 메커니즘(`source_api_refresh`의 ETag/Last-Modified,
  `is_duplicate_of_newest`의 콘텐츠 해시)은 이미 코드에 있고 5개 raw 수집
  지점 전부를 커버한다.
- C의 staging 설계는 #742에서 monthly_taxi_trip에 적용됐었고, #912에서
  최종 경로 직접 쓰기 + `_SUCCESS` 공개 방식으로 대체됐다.
- B/D/E의 데이터셋별 분류·지표 표는 이번 세션에서 실제 코드(스키마 정의,
  변환 로직)를 추적해서 만든 것이며, 임의로 지어낸 게 아니다.
- Gold는 추천 KPI를 계산하지 않고 월별 수익 시뮬레이션과 재고를 버전별로 누적한다.
- F가 근거로 삼은 `#674`의 4가지 NYC 종속 지점(EIA 시리즈 하드코딩, 파티션
  지역 축 없음, Timetable의 지역 트리거 표현 불가, 택시존 스키마 종속)은
  이슈에 이미 문서화돼 있던 사실이다.
- B의 `PULocationID`/`DOLocationID`/`pickup_zone`/`dropoff_zone` 선택 컬럼
  분류는 F의 4번 문제를 부분적으로 완화한다는 것도 실제 분류 결과에서
  나온 사실이다(지어낸 연결이 아니다).
- F의 5번(실행 동시성)은 구현됐다: 지역 파티션을 받는 `main/` DAG 8개는
  `max_active_runs=3`, Executor는 단일 노드 LocalExecutor다. EMR와 하위 DAG
  대기는 `deferrable=True`라 대기 중 worker slot을 반환한다.
- Silver 단일 파일 적재(`SingleParquetFileLoader`)가 리전 축 추가에 영향받지
  않는다는 것도 코드 대조로 확인한 사실이다 — Loader가 경로를 문자열로 받고,
  파일명 패턴 검증(`job.py:271-279`)이 디렉터리를 보지 않는다. 예외는
  `latest_partition_files`(`job.py:201-209`)의 한 레벨 glob 하나다.
- F의 "조용히 틀린 값" 11개 지점, "요란하게 죽는" 4개 지점, EIA `collected_date=`
  파티션 축 차이, 대시보드 3개 지점, 마이그레이션 도구 부재(`ALTER TABLE` 0건),
  테스트 하드 블로커 3개 — **전부 코드를 읽어 file:line으로 확인한 사실**이다.
- `region` 이름 충돌도 사실이다 — `region_name=os.getenv("AWS_DEFAULT_REGION")`
  2곳, `.env.example:13`의 `AWS_REGION`, 워크플로 6개 중 5개의 `aws-region:`.
- Slack 콜백 컨텍스트에 `partition_key`가 이미 들어온다는 것도 Airflow 3.3.0
  소스에서 확인했다(`airflow/sdk/execution_time/task_runner.py:337`).
- **`driver_id`는 지역 간 유니크하지 않다**(#805에서 확인). `build_driver_ids()`
  (`sub/generators/synthetic_company_snapshot/snapshot.py:61-66`)가
  `f"{DRIVER_ID_PREFIX}{index:0{width}d}"`로 `SD0000`~`SD1999`를 만드는데 지역
  성분이 prefix에도 순번에도 없다. 그래서 Gold 자연 키와 대시보드 조인 키 양쪽에
  `service_area`를 넣기로 결정했다(`docs/decision_making/0823.md` 3번) — 외부가
  주는 ID의 유니크성에 정합성을 의존시키지 않는다는 판단.

### 확인이 더 필요한 것 / 발표에서 정직하게 인정할 것

- B, D, E는 **코드가 전혀 없다.** "설계를 마쳤다"까지만 말하고 "구현했다"고
  말하지 않는다. F는 데이터 격리와 세 지역 동시 실행 상한까지 구현됐지만,
  실제 지역 등록과 지역별 DagRun fan-out은 별도 운영·구현 범위다.
- F는 세 지역까지 물리적으로 병렬 실행할 수 있다. 다만 `max_active_runs=3`은
  실행 상한일 뿐 지역 DagRun을 자동 생성하지 않는다는 점을 함께 밝힌다.
- F는 1~3번 문제만 설계로 풀리고, **4번(택시존 스키마)은 완전히 안 풀린다.**
  B의 선택 컬럼 처리가 "컬럼이 없는 경우"만 완화하고, "같은 이름인데 의미가
  다른 위치 표현"까지는 못 잡는다는 걸 질문받으면 그대로 인정할 것.
- F는 `#674`가 "확장 시점 미정이라 보류"로 결론냈던 걸 다시 연 것이다 —
  "시점이 정해져서"가 아니라 "구체적으로 설계해보니 비용이 생각보다 작았다"는
  게 재론 이유임을 분명히 할 것.
- **다만 "비용이 작다"는 파티션 키 설계 자체에 한정된 말이다.** 실제 구현 범위는
  Param 배선 ~25-30개 지점 + 조용히 틀리는 지점 11개 + 대시보드 + 마이그레이션
  런북 + 테스트 하드 블로커 3개다. 발표에서 "간단하다"로 들리게 말하면
  안 된다 — "설계는 기존 구조를 안 건드리는 방식으로 풀었고, 대신 손댈 지점을
  전수 조사해서 목록화했다"가 정확한 표현이다.
- **`test_dry_run_contract.py`가 이 브랜치에 없다.** develop의 `b43a138`에서
  dry-run 실행 경로가 제거된 것으로 보이는데, 이 문서와 과거 커밋에 남은
  "dry_run이 마지막 파라미터여야 한다"는 계약 언급은 이제 무효다. 왜 제거됐는지
  확인이 필요하다.
- C는 6개 중 1개만 구현됐다. 나머지 5개는 설계는 있지만 코드는 없다.
- D의 3단계 분류는 사후에 정리한 원칙이다 — "처음부터 이렇게 설계했다"고
  말하면 안 된다.
- E의 "평균 ± 2표준편차" 임계값은 실측 데이터 없이 정한 관례값이다. 실제
  데이터가 쌓이면 재보정이 필요할 수 있다.
- 로컬에 실제로 쌓인 과거 데이터는 거의 없다(`year_month=2026-01` 스테일
  개발 데이터 1개월). E의 "누적되면 자동으로 확장"이라는 설계가 왜 필요한지
  설명할 때, "지금 당장 검증하기 위해서"가 아니라 "앞으로 쌓일 것을 대비해서"
  라는 프레이밍을 유지할 것.

---

## 8. 다음에 할 일

### F 착수 순서 (권고)

지역 확장은 "조용히 틀린 값"이 많아 순서가 중요하다. 아래 순서는 *깨진 채로
초록불이 되는 구간을 최소화*하도록 잡았다.

1. [x] **`driver_id`가 지역 간 유니크한지 확인** — #805에서 **유니크하지 않음**으로
       결론. Gold 자연 키와 대시보드 조인 키에 `service_area`를 넣기로 결정
       (`docs/decision_making/0823.md` 3번)
2. [ ] **Slack 템플릿에 `{{ partition_key }}` 추가** — 배선 비용 0이고, 이후
       작업 중 무엇이 어느 지역에서 깨졌는지 보이게 해준다. 먼저 해두면 나머지
       작업의 디버깅이 싸진다
3. [ ] **생산자 쪽 파티션 키 발행**(`publish_month_partition`)과 **소비자 쪽
       파싱**(`resolve_target_year_month`)을 **같은 커밋에서** 변경 — 한쪽만
       바꾸면 Gold가 조용히 안 돈다
4. [ ] **dedup 상태 키에 `service_area` 추가**(`source_api_processed__`) +
       `_bronze_partition_exists` 경로 + `trigger_run_id` — 이걸 안 하면 지역이
       서로를 굶기므로 데이터 자체가 안 들어온다
5. [ ] **Gold Postgres 3함수 동시 수정**(`_next_version`, `_PRIMARY_KEYS`,
       `_validate_written_rows`) + 마이그레이션 SQL 런북 — *일부만 고치면 안 고친
       것보다 나쁘다*
6. [ ] 경로 계층 추가(EIA는 `collected_date=` 축이라 별도 취급) → 대시보드 →
       `max_active_runs` 상향

### 그 외

- [ ] B/C/D/E 중 어느 것을 실제로 구현할지 판단해 이슈 발행
- [ ] F의 4번(택시존 스키마) 문제는 이번 설계 범위 밖으로 남겨뒀다 — 실제
      착수 시 별도로 다시 그릴미 세션이 필요함
- [ ] F 구현은 영향 범위가 넓어(~25-30 배선 지점 / ~15파일) 이슈를 쪼개야 한다.
      `#674`의 DoD를 그 단위로 다시 나눌 것
- [ ] `docs/AIRFLOW_OPS.md` §5(알림 설계)를 지역 귀속 포함으로 갱신
- [ ] Gold DDL/마이그레이션 런북 문서 신설 — 지금 `docs/`에 Gold DDL을 다루는
      문서가 하나도 없다(`ALTER TABLE`도 레포 전체에 0건)
- [ ] `test_dry_run_contract.py`가 사라진 경위 확인 (develop `b43a138`)
- [ ] #740 staleness 워치독이 asset-triggered 경로에서 구조적으로 안 울리는
      문제 — **멀티리전과 무관하게 지금 죽어 있으므로 별도 이슈로**
- [x] 지역 파티션 main DAG 8개의 `max_active_runs=3` 상향. EMR와 하위 DAG
      대기는 `deferrable=True`이고 LocalExecutor `parallelism=32`는 유지한다.
- [ ] F 구현 시 `latest_partition_files`(`monthly_taxi_trip_bronze_to_silver/job.py:201-209`)의
      한 레벨 `year_month=` glob을 `service_area=` 계층까지 내려가도록 수정. 나머지
      Silver 적재 경로는 변경 불필요 (근거는 F 절의 "손댈 게 없는 것" 참고)
- [x] `coalesce(1)`→`repartition(1)` — #818로 해결됨. **리전 확장과는 무관한
      별도 축**이었음 (5GB `client.copy` 실패는 정정됨 — boto3 managed transfer가
      자동 멀티파트 처리하므로 대응 불필요)
- [ ] C는 그룹 1(driver_vehicle, lease_vehicle) 먼저, 그룹 2(EIA 3종)는 배선
      변경이 더 크므로 이후로 — 이슈 분리 시 참고
- [ ] B는 "선택 컬럼 소실 시 하류 null-fill" 부수조건을 별도 체크리스트로
      만들어 구현 시 빠뜨리지 않게 할 것
- [ ] E의 임계값(3개월/12개월 윈도우/2표준편차)은 발표 후 실제 구현 논의
      시 재검토
- [ ] 각 포인트의 발표 멘트를 실제 발표 시간에 맞게 축약
- [ ] D의 표, B의 필수/선택 컬럼 표, C의 그룹 분류 표를 슬라이드용 그림으로 재정리
