# 03. 구현 현황 — 완료 / 제약 / 후속 과제

## 구현 완료

| 영역 | 내용 |
| --- | --- |
| 수집 자동화 | 원천 API 3종 + EIA 2종을 Lambda로 일·월 수집, 버전 디렉터리 + `_SUCCESS` 마커로 재수집 안전 |
| 메달리온 3계층 | Bronze(원본)→Silver(정제·통합)→Gold(비즈니스 로직) 분리, 계층 간 스키마 계약(`schema/`) 고정 |
| 품질 검증 | Great Expectations 표 검증 + 데이터독스, Silver 스키마 대조·격리, Gold control total·비즈니스 불변식 |
| 적재 신뢰성 | Gold 단일 트랜잭션 + advisory lock + fingerprint 멱등성 재사용 + 입력 파일 내용 해시(#1088), 배포 Lambda 저장소 설정 누락 시 즉시 실패(#1083), NaN 등 비유한 값 차단(#1076, #1080) |
| 오케스트레이션 | 월간 DAG 체인, 상류 완료 센서(#1086), asset 파티션 의존, Slack 실패 알림 |
| 추천 알고리즘 | v1(기사 순수익 우선)·v2(회사 매출 우선, threshold 스윕) 병행 산출(`recommendation_algorithm/`) — 병행 설계 문서는 PR #1091 로 승격 진행 중 |
| 대시보드 | 알고리즘/하한/지역/월 필터, 지표 카드 4종, 분포·차종·사유 차트, 기사별 추천 테이블, Silver 입력 계보 노출 |
| 인프라·관측 | 역할별 EC2 분리 + Nginx 단일 진입점, Prometheus/Grafana + CloudWatch ([docs/MONITORING.md](../MONITORING.md)) |
| CI | 런타임별 pytest(`make test`), ruff, uv lock 검증 — 머지 게이트 |

## 실제 결과물 예시 (2026-01 로컬 실행, threshold $100)

`data/gold/monthly_report/year_month=2026-01/monthly_report.csv`:

| 지표 | 값 |
| --- | --- |
| 추천 대상 기사 | **117명** (분석 대상 2,000명 중) |
| 기사 1인당 예상 월 순수익 증가 | **$194** |
| 평균 예상 월 렌탈 객단가 증가 | $93 |
| 회사 총 예상 월 매출 증가 | **$10,881** |

기사별 추천 행(`driver_car_suggestion.csv`) 예시 — 왜 이 차량인지 사유까지 함께 제공:

| driver_id | 현재 | 추천 | 추천 사유 | 순수익 증가액 |
| --- | --- | --- | --- | --- |
| SD0303 | KIA Forte | Chevrolet Malibu 2025 | 연비, 차량등급 | +$95.9 |
| SD0317 | Chevrolet Malibu | KIA Forte 2024 | 더 저렴한 렌트료 | +$86.7 |

대시보드 화면은 [01 제품 개요](./01_product_overview.md) 하단 이미지와 [README 결과 이미지](../../README.md#결과-이미지) 참고.

## 제약

| 제약 | 내용 |
| --- | --- |
| 전력 요금 공개 지연 | EIA 전력 요금이 약 3개월 늦게 확정돼 최신 달 통합 연료비 분석도 그만큼 물러섬(`eia_fuel_price_silver` DAG의 lag 로직). 잠정값(Preliminary)은 재생성 시 값이 바뀔 수 있고 `ev_price_status` 로 표기 |
| 운영 지역 | `service_area` 파라미터 구조는 지역 확장을 열어두었으나 실제 운영·데이터는 NYC 단일 |
| 합성 원천 의존 | HVFHV 원본에 taxi_id/driver_id 가 없어 결정적 배정으로 합성한 회사 DB를 원천으로 사용 — 실제 리스 업체 원장과는 다른 가정 포함 |
| 대시보드 조회 범위 | 지역·월별 최신 Gold 버전만 조회. 과거 버전 간 비교 UI는 없음(버전 이력은 RDS에 누적) |
| 월 단위 갱신 | 추천 결과는 월 파티션 단위라 당일 운행 변화는 반영되지 않음 |

## 후속 과제

| 과제 | 근거 |
| --- | --- |
| 원천 API 호출 재시도/backoff 부재 | `main/aws_lambda/common/monthly_dataset.py` 의 HTTP GET — 일시적 5xx/429 가 곧 DAG 실패로 이어짐 |
| Slack 스키마 드리프트 알림 미연결 | `shared/aws_lambda/common/slack_notifier.py` — 구현만 있고 호출부 없음. 연결하거나 제거 필요 |
| 원천 API 주소 하드코딩 | `10.0.10.81:8091` 리터럴이 DAG/scripts 4곳에 반복 — 설정화 필요 |
| Gold 로컬 CSV 모드와 RDS 모드 병행 유지보수 | `main/dashboard/datasource.py` 두 소스의 컬럼 계약 동기화 유지 |
