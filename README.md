# The One — 주행 데이터 기반 리스 차량 추천 파이프라인

> **뉴욕 Uber·Lyft 기사 대상 차량 리스 업체를 위한 데이터 기반 차량 교체 추천 시스템**
>
> 기사의 순수익과 리스 업체의 객단가가 **동시에** 오르는 고객과 제안 차량을 매월 산출합니다.

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [솔루션](#2-솔루션)
3. [기대 효과](#3-기대-효과)
4. [데이터 프로덕트](#4-데이터-프로덕트)
5. [데이터 파이프라인 설계 및 기술적 고려](#5-데이터-파이프라인-설계-및-기술적-고려)
6. [기술 스택](#6-기술-스택)
7. [팀원 소개](#7-팀원-소개)

---

## 1. 프로젝트 개요
### 대상 
- 뉴욕 Uber·Lyft 기사 대상 차량 리스 업체의 고객 담당자
### 문제점
- 객단가 향상을 통한 매출 상승 기회 상실 
  - 기사에게 더 높은 순수익을 주면서도 리스 업체의 객단가를 끌어올릴 수 있는 차량을 데이터 기반으로 추천하지 못함
### 해결방안
- 차량 교체 권장 고객 및 제안 차량 추천 대시보드
  - 대시보드 조건 : 차량 변경 시 '기사 순수익 월 600$ 이상 증가 & 리스 업체 렌탈 객단가 상승'

---

## 2. 솔루션

**실제 승차공유 운행 기록**에서 기사별 월 순수익을 계산하고,
모든 후보 차량으로 바꿨을 때의 손익을 시뮬레이션해 **교체를 권할 고객과 제안 차량**을 뽑습니다.

| 단계 | 설명 |
| --- | --- |
| **운행별 순수익** | 운행 기록 + 그달 연료 단가 + 현재 차량 제원 → 운행 한 건의 순수익 |
| **기사 월간 집계** | 시간대 8구간 비중, 상위 3개 운행 구역, 현재 차량, 월 순수익 |
| **교체 시뮬레이션** | 기사 × 후보 차량 전체에 대해 연비 차이·등급 상승·렌트료 차이를 반영 |
| **우선순위 산출** | `순수익 증가 ≥ $600 AND 매출 증가 ≥ 0` 을 만족하는 대상자를 증가 폭 순으로 정렬 |

### 결과물

| | 설명 |
| --- | --- |
| **차량 교체 추천 대시보드** | 이번 달 연락할 고객과 제안 차량을 순위로 제공 |
| **추천 근거 표시** | `연비` / `차량등급` / `렌트료` 중 무엇이 이득을 만들었는지 분해 |
| **월간 리포트** | 추천 대상자 수, 평균 순수익 증가액, **총 객단가 상승액** |

*(대시보드 스크린샷 삽입 예정)*

현재는 Streamlit 프로토타입([main/dashboard/app.py](./main/dashboard/app.py))으로 Gold 산출물을 검증 중이며,
화면이 확정되면 **Next.js 기반 웹 대시보드로 전환**할 예정입니다.
대시보드는 Gold 3종만 읽고 계산은 전부 파이프라인이 끝내 두므로, 프론트엔드를 바꿔도 스키마는 그대로입니다.

---

## 3. 기대 효과

| 관점 | Before | After |
| --- | --- | --- |
| **차량 추천** | 판단 기준 부재, 감에 의존 | 운행 기록 기반 순수익·객단가 **동시** 시뮬레이션 |
| **고객 우선순위** | 수백 명 중 누구부터인지 불명확 | 순수익 증가 폭 순 자동 정렬 |
| **추천 근거** | 설명 불가 | 연비·등급·렌트료 **기여도 분해** 제공 |
| **비즈니스 효과** | 업그레이드 기회 누락, 객단가 정체 | 기사 만족도 ↑ & **렌탈 객단가 ↑** |

---

## 4. 데이터 프로덕트

### 4-1. 원천과 산출물

<details>
<summary><b>공개 원천 7종</b></summary>

| 원천 | 수집 대상 | 수집 방식 | 갱신 주기 | 규모 |
| --- | --- | --- | --- | --- |
| **TLC** | HVFHV 운행 기록 | Parquet 다운로드 | 월 1회 | 월 **2,040만 행** |
| **FastTrackLease** | 렌탈 차종·주간 렌트료 | HTML 크롤링 + **이미지 OCR** | 주 1회 | 24종 |
| **FuelEconomy.gov** | 차량 제원 (연비·전비·연료종류) | CSV 다운로드 | 월 1회 | 50,242행 |
| **Uber** | Comfort 배차 자격 차량 | 내부 API | 주 1회 | 59,650행 |
| **Lyft** | Extra Comfort 자격 차량 | HTML 크롤링 | 주 1회 | 1,008행 |
| **EIA** | 뉴욕주 휘발유 주간 소매가 | XLS 다운로드 | 월 1회 | 월 1행 |
| **EIA** | 뉴욕주 전기 요금 | XLSX 다운로드 | 월 1회 | 월 1행 |

**외부 원천 수집은 전부 원천 DB 파이프라인이 맡습니다.**
메인 파이프라인은 스스로 크롤링하지 않고, 원천 DB 가 발행한 것만 소비합니다 —
수집·정제 책임과 분석·추천 책임을 섞지 않기 위해서입니다.

렌탈 카탈로그만 OCR 을 씁니다 — FastTrackLease 가 차종명과 가격을 **이미지로만** 노출하기 때문입니다.
(`pytesseract`, 모델명/제조사를 한 줄씩 잘라 인식 정확도를 확보)

</details>

<details>
<summary><b>원천 DB 가 만드는 데이터</b></summary>

| 데이터셋 | 한 행 | 규모 | 생성 방식 | 공개 |
| --- | --- | --- | --- | --- |
| `월별 택시 운행 기록` | 운행 1건 | 월 **2,040만 행** | TLC 실데이터에 `taxi_id` 를 시공간 제약하에 배정 | **API** |
| `기사-택시 마스터 데이터` | 기사 1명 | 2,000행 | 내부 원장에서 파생 | **API** |
| `리스 업체 보유 차량 데이터` | 차량 1대 | 2,000행 | 내부 원장에서 파생 | **API** |

TLC 원본에는 `driver_id` 도 `taxi_id` 도 없습니다.
기사 선호도·근무 한도·공차 이동시간 제약을 만족하는 **결정적(deterministic) 배정 알고리즘**으로 만들어 냅니다.

</details>

<details>
<summary><b>최종 산출물 3종</b></summary>

| 산출물 | 한 행 | 규모 | 갱신 주기 |
| --- | --- | --- | --- |
| `기사별 운행 순수익` | 기사 × 월 | 2,000행/월 | 월 1회 |
| `기사별 차량 교체시 예상 수익` | 기사 × 월 | 2,000행/월 | 월 1회 |
| `월간 리포트 주요 요약 정보` | 월 | 1행/월 | 월 1회 |

`월간 리포트` 에는 **계보 컬럼**(어느 시점 차량 대장·어느 달 연료비를 썼는지)을 함께 싣습니다.

</details>

### 4-2. 메인 데이터 파이프라인
> 가상 사내 시스템과 EIA에서 데이터를 수집해 메달리온 아키텍처로 정제
![메인 데이터 파이프라인 아키텍처](main_data_product_architecture.png)

| 계층 | 역할 | 실행 런타임 | 적재 위치|
| --- | --- | --- | --- |
| **Bronze** | 원본 그대로 적재 (Extract → Load) | Lambda | S3 |
| **Silver** | 원본별로 1:1 정제, 연료비만 통합 | Lambda or Spark(EMR) | S3|
| **Gold** | 조인,집계,시뮬레이션,추천 | Lambda or Spark | RDS|

### 4-3. 원천 DB 파이프라인
> 데이터 합성 과정을 가상 사내 시스템 파이프라인으로 분리했습니다.

![원천 DB 파이프라인 아키텍처](source_company_architecture.png)

| 계층 | 내용 |
| --- | --- |
| **RAW** | 실제 원천 데이터의 원본 저장 |
| **Curated** | 표준화, 정제된 데이터 저장 |
| **Synthesize** | 데이터 합성 과정 저장 |
| **Attribution** | 합성된 데이터와 실제 데이터를 매핑|
| **Published** | 해당 원천 시스템에서 제공하는 데이터|

### 4-4. 두 파이프라인의 경계

**`main` 은 `sub` 의 내부 파일을 직접 참조하지 않고 Published API를 이용하여 분리된 원천에서 받아오는 것처럼 구현합니다.**

---

## 5. 데이터 파이프라인 설계 및 기술적 고려

### 5-1. 기술적 고려사항

- [Spark 최적화 — Arrow 직접 메모리 초과와 그룹 키 재설계](./docs/SPARK_OPTIMIZATION.md)
- [데이터 모델링 — 계층별 스키마와 스키마 소유권 중앙화](./docs/DATA_MODEL.md)

### 5-2. 비즈니스 로직 고려 사항

- [순수익·객단가 계산식과 추천 기준선](./docs/METRICS.md)
- [TLC 에 없는 서비스 등급을 데이터에서 추정하기](./docs/METRICS.md#3-요금-배수는-어디서-오는가)
- [Gold 레이어 설계](./docs/GOLD_LAYER_DESIGN.md)

### 5-3. 운영 고려 사항

- [원천 DB 파이프라인을 따로 만든 이유 — 백필 테스트가 불가능했다](./docs/SOURCE_PIPELINE.md)
- [원천을 신뢰하지 않는 전제로 수집하기 — 매니페스트 데이터 계약](./docs/SOURCE_CONTRACT.md)
- [데이터 품질 — 검증 게이트 · 원자적 공개 · 스키마 드리프트](./docs/DATA_QUALITY.md)
- [Airflow 운영 — 태스크 설계와 장애 알림](./docs/AIRFLOW_OPS.md)
- [로컬 개발 환경 구성](./docs/GETTING_STARTED.md)
- [팀 협업 규칙 — Branch · Commit · PR · 리뷰 훅](./docs/TEAM_RULES.md)

### 5-4. 의사결정 기록
팀내 의견 공유를 통해 의사결정한 내용(기술, 기획 등)들은 [docs/decision_making](./docs/decision_making/) 에 날짜별로 기록했습니다.

## 6. 기술 스택

<div align="center">

### Data Processing
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1E4FFF?style=for-the-badge&logo=apacheiceberg&logoColor=white)

### Storage
![AWS S3](https://img.shields.io/badge/S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Amazon RDS](https://img.shields.io/badge/RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white)
![AWS Glue](https://img.shields.io/badge/Glue_Data_Catalog-8C4FFF?style=for-the-badge&logo=amazonaws&logoColor=white)

### Compute / Infrastructure
![AWS EMR](https://img.shields.io/badge/EMR-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/Lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white)
![AWS EC2](https://img.shields.io/badge/EC2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

### Orchestration / Quality
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6310?style=for-the-badge&logo=greatexpectations&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Slack](https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white)

### Visualization
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

</div>

---

## 7. 팀원 소개

<table align="center">
  <tr>
    <td align="center"><a href="https://github.com/kingrangE"><b>전길원</b></a></td>
    <td align="center"><a href="https://github.com/taeju-moon"><b>문태주</b></a></td>
    <td align="center"><a href="https://github.com/HongJunseong"><b>홍준성</b></a></td>
    <td align="center"><a href="https://github.com/inerasable0203"><b>최지욱</b></a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/kingrangE"><img src="https://github.com/kingrangE.png" width="150px" alt="전길원"/></a></td>
    <td align="center"><a href="https://github.com/taeju-moon"><img src="https://github.com/taeju-moon.png" width="150px" alt="문태주"/></a></td>
    <td align="center"><a href="https://github.com/HongJunseong"><img src="https://github.com/HongJunseong.png" width="150px" alt="홍준성"/></a></td>
    <td align="center"><a href="https://github.com/inerasable0203"><img src="https://github.com/inerasable0203.png" width="150px" alt="최지욱"/></a></td>
  </tr>
  <tr>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
  </tr>
</table>
