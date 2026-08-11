# 기사 마스터 테이블 생성 — Implementation Plan

이슈 #160. `driver_id`가 원본에 없어(`transformer.py:133`) 가짜 기사 1만 명을
2단계 계층적 시뮬레이션(기사 성향 → 일별 로그 → 필드 집계)으로 생성한다.

**범례**: `[실측]` = `analysis.md`의 2024년 실측치 근거 / `[목표]` = 사용자가 준
체크리스트 목표치 / `[가정]` = 실측·목표 둘 다 없어 임의로 정한 분포 (이 문서에서 확정)

`기사ID`(UUID4), `기사이름`(하드코딩 성/이름 풀 무작위 조합)은 자명해서 표에서 제외.

## 1. 기사 성향(trait) — 1만 명, 샘플링 1회

| 트레잇 | 분포 | 파라미터 | 근거 |
|---|---|---|---|
| `work_mean_i` (일 근무시간, h) | Gamma | shape=6, scale=1.2 → mode 6.0h, mean 7.2h | `[목표]` 최빈값 6~8h |
| `work_cv_i` | Uniform | (0.30, 0.40) | `[목표]` CV 0.3~0.4 |
| `active_days_count_i` | Categorical | {3:0.15, 4:0.20, 5:0.25, 6:0.25, 7:0.15} | `[가정]` 3~7 spread, 5~6에 약한 peak |
| `active_weekdays_i` | 요일 7개 중 비복원 가중 샘플 k=`active_days_count_i` | 가중치 [월.125 화.130 수.136 목.143 금.156 **토.167** 일.143] | `[실측]` 요일 비중 |
| `distance_pref_i` (개인 평균 트립거리, mile) | 실측 trip_miles 샘플에서 부트스트랩 추출 | `analysis.md` 6,000,000행 샘플 풀 | `[실측]` — 개인차를 실측 population 분산 그대로 물려받음 |
| `time_pref_i` (8개 시간대 선호 가중치) | Dirichlet | concentration = 8 × [.081 .048 .120 .131 .136 .157 **.171** .156] | `[실측]` 시간대 비중을 중심으로 개인별로 흔들기 |
| `avg_trip_duration_i` (분) | 실측 trip_time 샘플에서 부트스트랩 추출 | `analysis.md` 샘플 풀 (median 16.3분) | `[실측]` |
| `rest_frac_i` | Uniform | (0.05, 0.15) | `[가정]` — 체크리스트에 목표 없음 |
| `idle_frac_i` | Uniform | (0.15, 0.35) | `[가정]` — 체크리스트에 목표 없음 |
| `churn_flag_i` | Bernoulli | p=0.25 | `[가정]` 이탈률 25% |
| `가입일_i` | Uniform | [오늘-1095일, 오늘-14일] (최소 14일 tenure 보장) | `[가정]` |

## 2. 일별 로그 시뮬레이션 (트레잇 → 하루 값)

기간: `가입일_i` ~ min(탈퇴일 또는 오늘, 가입일_i+90일) — 최대 90일 캡.
`active_weekdays_i`에 해당하는 날짜만 근무일로 계산.

```
work_minutes_day  ~ Gamma(shape=1/work_cv_i², scale=work_mean_i*60*work_cv_i²)   # mean=work_mean_i*60분
rest_minutes_day  = work_minutes_day * clip(rest_frac_i + Normal(0,0.02), 0.02, 0.25)
remaining         = work_minutes_day - rest_minutes_day
idle_seconds_day  = remaining*60 * clip(idle_frac_i + Normal(0,0.05), 0.05, 0.5)
trip_minutes_day  = remaining - idle_seconds_day/60
trip_count_day    = round(trip_minutes_day / avg_trip_duration_i)
```

`trip_count_day`개의 트립마다:
- 거리(mile) ~ `Lognormal(mu=log(distance_pref_i), sigma=0.35)` → 버킷화 (임계값 **1.93mi / 4.75mi**, `[실측]` tertile)
- 시간대 ~ `Categorical(time_pref_i)`

## 3. 최종 스키마 필드 — 생성/집계 방법

출력 컬럼명은 전부 영어(스키마 필드명도 마찬가지 — 한글은 여기 문서에서 설명용으로만
씀). 코드 상 실제 dict/컬럼 키는 `aggregate.py`의 `aggregate_driver()` 참고.

| 필드(영어 컬럼명) | 원본 한글 스펙 | 생성 방법 | 근거 |
|---|---|---|---|
| `primary_distance_bands` | 주요활동거리 | 위 트립 로그의 거리 버킷별 점유율 계산 → **점유율 20% 이상인 버킷 전부** (없으면 최다 버킷 1개) | `[실측]`(임계값) + `[가정]`(20% 규칙) |
| `primary_time_blocks` | 주요활동시간 | 트립 로그의 시간대별 점유율 → 점유율 20% 이상인 시간대 전부 (없으면 최다 1개) | `[실측]`(가중치) + `[가정]`(20% 규칙) |
| `active_weekdays` | 활동요일 | `active_weekdays_i` 그대로 | `[실측]`+`[가정]` (§1) |
| `max_idle_seconds` / `min_idle_seconds` | 최대/최소공차시간(초) | `idle_seconds_day`의 max/min | `[가정]` |
| `max_trip_count` / `min_trip_count` | 최대/최소trip수 | `trip_count_day`의 max/min | 파생 (§2 공식) |
| `max_work_minutes` / `min_work_minutes` | 전체운행시간최대/최소(분) | `work_minutes_day`의 max/min | `[목표]` (§1 work_mean/cv) |
| `max_rest_minutes` / `min_rest_minutes` | 최대/최소휴식시간(분) | `rest_minutes_day`의 max/min | `[가정]` |
| `joined_at` | 가입일 | `가입일_i` | `[가정]` |
| `churned_at` | 탈퇴일 | `churn_flag_i`=1이면 `가입일_i + Uniform(30, (오늘-가입일_i).days)`일, 아니면 `null` | `[가정]` |

## 4. 코드 위치

`spark/pyproject.toml:16`에 이미 `scipy`가 "합성 데이터 분포" 주석과 함께 들어있어
이 작업을 예상하고 심어둔 의존성으로 보임 — 새 `scripts/` uv 프로젝트 대신
`spark/` 안에서 생성한다 (Spark 세션은 안 씀, numpy/scipy/pandas만 사용).

```
spark/jobs/driver_master/
  traits.py      # §1 트레잇 샘플링
  simulate.py    # §2 일별 로그
  aggregate.py   # §3 필드 집계
  job.py         # CLI 엔트리포인트
spark/tests/jobs/driver_master/
  test_traits.py, test_simulate.py, test_aggregate.py
```

출력: `data/bronze/driver_master.csv` (1만 행, `taxi_zone_lookup.csv`처럼 정적 레퍼런스).

## 5. 검증 (자체 self-check)

시뮬레이션 중간산물(일별 로그, 트립별 거리)을 QA용으로 잠시 보관해서 확인:

1. 전체 `work_minutes_day` 풀링 히스토그램 → 우편향, 최빈값 360~480분, 720분 꼬리
2. 기사별 `work_minutes_day`의 CV → 0.3~0.4 근처 분포
3. `활동요일` 길이 분포 → 3~7 spread
4. 기사별 `distance_pref_i` 히스토그램 → 넓게 퍼짐 (부트스트랩이라 실측 분산 그대로 유지될 것으로 예상)

기준 미달 시 §1의 Gamma shape/scale, Dirichlet concentration 등을 조정.
