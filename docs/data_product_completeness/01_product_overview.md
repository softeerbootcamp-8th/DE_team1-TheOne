# 01. 제품 개요 — 사용자 문제, 핵심 기능, 입력·출력, 결과물 형태

## 사용자와 문제

| 항목 | 내용 |
| --- | --- |
| **사용자** | 뉴욕 Uber·Lyft 기사를 대상으로 차량을 리스하는 업체의 고객 담당자 |
| **문제** | 수천 명의 기사 중 "상위 등급 차량으로 교체하면 순수익이 월 $500 이상 오르는 고객"을 판단 기준 없이 감에 의존해 골라냄 → 객단가 향상 기회 상실 |
| **해결** | 운행 데이터로 교체 대상 고객과 제안 차량을 계산해 우선순위 순으로 보여주는 대시보드 |

선정 조건은 두 가지입니다 — **기사 예상 월 순수익 증가액이 하한($500 기본) 이상**이면서 **리스 업체의 월 렌탈 객단가도 증가**하는 조합만 추천합니다(`main/dashboard/app.py` 의 `_aggregates`, 캡션 문구 참고).

## 핵심 기능

| # | 기능 | 구현 위치 |
| --- | --- | --- |
| 1 | 기사별 월 운행 집계 — 현재 차량의 예상 순수익(수익 − 연료비 − 렌트료), 시간대·지역 패턴 | `main/spark/jobs/silver_to_gold/transformer.py` (`build_driver_monthly_aggregation`) |
| 2 | 차량 교체 시뮬레이션 — 보유 차량 12종 × 기사 2,000명 조합의 예상 순수익·객단가 증가액 계산 | 동일 (`build_driver_monthly_profit`) + `recommendation_algorithm/` (v1 기사 순수익 우선, v2 회사 매출 우선 + threshold 스윕) |
| 3 | 추천 대상 선정 — 알고리즘 버전·순수익 증가 하한·지역·월 필터 | `main/dashboard/app.py` (`recommendation_scope`) |
| 4 | 결과 제공 — 지표 카드(추천 대상 수·인당 순수익 증가·평균 객단가 증가·추천 차종 수), 증가 분포·차종 이동·추천 사유 차트, 기사별 추천 테이블 | `main/dashboard/app.py`, `main/dashboard/charts.py` |
| 5 | 결과 신뢰성 확인 — 어떤 Silver 입력 버전으로 만들었는지 계보(expander) 노출 | `main/dashboard/app.py` (`_silver_source_expander`) |

## 입력

| 출처 | 데이터 | 규모 | 수집 주기 |
| --- | --- | --- | --- |
| 회사 가상 원천 DB (API) | 월별 택시 운행 기록 (TLC 실데이터 + taxi_id/driver_id 합성 배정) | 월 70–90만 행 | 일 1회 |
| 회사 가상 원천 DB (API) | 기사-택시 월별 스냅샷 | 약 2,000행 | 일 1회 |
| 회사 가상 원천 DB (API) | 리스 보유 차량 마스터(차종·등급·제원·주간 렌트료) | 12종 | 일 1회 |
| EIA | 뉴욕주 휘발유 주간 소매가 (XLS) | 월 1파티션 | 월 1회 |
| EIA | 뉴욕주 전기 요금 (XLSX) | 월 1파티션 | 월 1회 |

원천 DB 자체는 별도 파이프라인(`sub/`)이 TLC 실데이터에 결정적 배정 알고리즘으로 taxi_id/driver_id를 붙여 API로 공개합니다. 세부는 [README 원천 DB 파이프라인](../../README.md#데이터-파이프라인) 참고.

## 출력

Gold(RDS PostgreSQL)에 월·지역 파티션으로 적재되고, 대시보드가 읽습니다.

| 테이블 | 한 행 | 월 규모 | 주요 컬럼 |
| --- | --- | --- | --- |
| `driver_aggregation` | 기사 1명 | 약 2,000행 | 시간대 운행 비율, 상위 3개 운행 zone, 현재 차량·연비, 월 주행거리·연료비·렌트료·순수익 |
| `driver_car_suggestion` | 기사 × 추천 조합 | 약 2,000행 | 추천 제조사·모델·연식, 추천 사유, 예상 연료비·순수익, **순수익 증가액**, **객단가 증가액** |
| `silver_lineage` | 실행 1회 | 실행당 1행 | 어떤 Silver 버전 경로·코드 SHA·설정 해시로 만들었는지 |

적재는 `(service_area, year_month)` 단위로 버전이 누적되며, 같은 입력·설정의 재실행은 fingerprint 로 식별해 기존 버전을 재사용합니다(`main/spark/jobs/silver_to_gold/postgres_loader.py`).

## 결과물 형태

- **운영**: Streamlit 대시보드 — Nginx 리버스 프록시 뒤 `https://43-200-202-72.sslip.io/` 로 공개, RDS 에서 최신 Gold 버전만 조회(`DASHBOARD_DATA_SOURCE=rds`)
- **로컬**: `data/gold/` 의 CSV 를 읽는 동일 화면(`DASHBOARD_DATA_SOURCE=local`, 기본값)

![대시보드 결과 화면](../../assets/dashboard_reference.png)
