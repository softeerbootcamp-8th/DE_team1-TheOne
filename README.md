<div align="center">

![](logo.png)
<h4><b>택시 차량 리스 업체를 위한 <span style="color:#0c549f;">운행 데이터 기반 차량 교체 추천</span> 대시보드</b></h4>

<p>NEXTMOVE는 <span style="color:#0c549f;font-weight:bold;">운행 데이터를 기반으로 차량 교체를 제안할 고객을 확인</span>할 수 있는 대시보드입니다.<br/>
이때, 추천 대상은 <span style="color:#0c549f;font-weight:bold;">상위 등급 차량 교체시 고객의 순수익이 월 $600 이상 증가</span>하는 고객입니다.</p>

<a href="https://43-200-202-72.sslip.io/"><img src="https://img.shields.io/badge/대시보드_바로가기-000000?style=for-the-badge&logoColor=white" alt="대시보드 바로가기" /></a> <a href="docs/TEAM_RULES.md"><img src="https://img.shields.io/badge/팀규칙_바로가기-000000?style=for-the-badge&logoColor=white" alt="팀 규칙 바로가기" /></a>
</div>


## 목차

1. [프로젝트 개요](#프로젝트-개요)
2. [데이터 파이프라인](#데이터-파이프라인)
3. [문서화](#문서화)
4. [기술 스택](#기술-스택)
5. [팀원](#팀원)



## 프로젝트 개요 
### 대상 
- 뉴욕 Uber·Lyft 기사 대상 차량 리스 업체의 고객 담당자
### 문제점
- 객단가 향상을 통한 매출 상승 기회 상실 
  - 기사에게 더 높은 순수익을 주면서도 리스 업체의 객단가를 끌어올릴 수 있는 차량을 데이터 기반으로 추천하지 못함
### 해결방안
- 차량 교체 권장 고객 및 제안 차량 추천 대시보드
  - 대시보드 조건 : 차량 변경 시 '기사 순수익 월 600$ 이상 증가 & 리스 업체 렌탈 객단가 상승'
### 결과 이미지
![대시보드 이미지](assets/dashboard_reference.png)


### 기대 효과

| 관점 | AS-IS | TO-BE |
| --- | --- | --- |
| **차량 추천** | 판단 기준 부재, 감에 의존 | 운행 기록 기반 순수익을 고려한 데이터 기반 추천 |
| **고객 우선순위** | 수천명 중 누구에게 전화할지 불명확 | 순수익 증가폭 순 정렬 |
| **비즈니스 효과** | 수익 증가 기회 놓침 | 기사 만족도 향상, 객단가 증가 |

[목차로 이동](#목차)

## 데이터 파이프라인

### 1. INPUT

| 출처 | 수집 대상 | 수집 방식 | 수집 주기 | 규모 |
| --- | --- | --- | --- | --- |
| 회사 가상 원천 DB | 월별 택시 운행 기록 | API 요청 | 일 1회 | 월 **70-90만 행** |
| 회사 가상 원천 DB | 렌탈 차종, 등급, 제원, 주간 렌트료 등 | API 요청 | 주 1회 | 12종 |
| 회사 가상 원천 DB | 월별 기사-택시 테이블 | API 요청 | 월 1회 | 약 2000 행 |
| **EIA** | 뉴욕주 휘발유 주간 소매가 | XLS 다운로드 | 월 1회 | 월 1행 |
| **EIA** | 뉴욕주 전기 요금 | XLSX 다운로드 | 월 1회 | 월 1행 |

### 2. OUTPUT

| 데이터 | 주요 정보 | 계산 방식 |
|---|---|---|
| 기사 현재 예상 순수익 | 기사의 현재 운행 예상 수익 | 수익 - 주간 유류비 + 주간 렌트료 |
| 기사별 차량 교체시 예상 수익 | 기사-차량 조합별 예상 수익 | 현재 운행 예상 수익 산식을 각 차량에 적용 |
| 월간 리포트 요약 정보 | 골드 데이터 버전, 추천 대상 기사 수, 기사 평균 순수익 증가액/매출 증가액, 기사 매출 증가액 합계 | 위 두 테이블을 기반으로 변경 시 값 계산 |


### 3. 파이프라인
![메인 데이터 파이프라인 아키텍처](assets/main_data_product_architecture.png)
> 가상 사내 시스템과 EIA에서 데이터를 수집해 **메달리온 정제 구조** 설계

| 계층 | 역할 | 실행 런타임 | 적재 위치|
| --- | --- | --- | --- |
| **Bronze** | 원본 적재, 수집 품질 검증 | Lambda | S3 |
| **Silver** | 원본별 정제, 연료비 통합, 레코드 품질 검증 | Lambda or Spark(EMR) | S3|
| **Gold** | 비즈니스 로직 적용, 조인, 집계, 시뮬레이션, 추천 | Lambda or Spark(EMR) | RDS |

### 4. AWS Infra Architecture
![AWS 인프라 아키텍처](assets/AWS_architecture.png)
- [AWS 서비스별 역할 및 설명](/docs/AWS_INFRA.md)
- 주요 항목
  - **Lambda** : 역할에 따른 VPC 위치 구분
    1. VPC 내부 : 원천 소스 API 서버와 상호작용하는 것
        - 가정 : 리스 **회사 내의 개발자**가 개발한 **데이터 파이프라인**
    2. VPC 외부 : 그 외 외부 통신 IAM 최소 권한으로 보안 확보
  - **NGINX** : 리버스 프록시를 이용한 대시보드 접근 경로 단일화 / 내부 포트 은닉
    - Public의 Nginx가 요청을 받아 Private 차량 추천 대시보드로 리버스 프록시
    - 외부 사용자는 NginX를 통해서만 접근
  - **모니터링 대시보드** : 외부망 노출 X (내부 전용)

<details>
<summary>원천 DB 파이프라인</summary>

> HVFHV 데이터에 택시 ID/기사 ID가 없기에 합성을 진행하는 **가상의 회사 DB**입니다.


#### 1. INPUT
| 출처 | 수집 대상 | 수집 방식 | 수집 주기 | 규모 |
| --- | --- | --- | --- | --- |
| TLC | HVFHV(Uber/Lyft 운행 기록) | parquet 다운로드 | 일 1회 | 월 **2000만행** |
| FastTrackLease(렌탈사 사이트) | 렌탈 차종 및 주간 렌트료 | 웹 크롤링 | 월 1회 | 차량 12종 |
| Fuel Economy(미국 정부 사이트) | 차량 제원(연비 등) | API 요청 | 월 1회 | 약 50000행 |
| Uber Eligible List(Uber 사이트) | Uber 차량별 서비스 등급 | 웹 크롤링 | 월 1회 | 월 1행 |
| Lyft Eligible List(Lyft 사이트) | Lyft 차량별 서비스 등급 | 웹 크롤링 | 월 1회 | 월 1행 |

#### 2. OUTPUT
| 데이터 | 한 행 | 규모 | 생성 방식 | 공개 |
| --- | --- | --- | --- | --- |
| `월별 택시 운행 기록` | 운행 1건 | 월 **2,040만 행** | TLC 실데이터에 `taxi_id` 를 시공간 제약하에 배정 | **API** |
| `기사-택시 마스터 데이터` | 기사 1명 | 2,000행 | 내부 원장에서 파생 | **API** |
| `리스 업체 보유 차량 데이터` | 차량 1대 | 2,000행 | 내부 원장에서 파생 | **API** |

#### 3. 배정 알고리즘 (데이터 합성)
- 목표 : 현실적인 기사 데이터 배정
- 방식 : **결정적(deterministic) 배정 알고리즘**
  - 기사 선호도 / 근무 한도 / 공차 이동시간 제약을 만족하는 알고리즘

#### 4. 파이프라인
![원천 DB 파이프라인 아키텍처](assets/source_company_architecture.png)
> 원천 DB에서 생성한 데이터를 **API로 메인 데이터 파이프라인에서 수집**합니다.

| 계층 | 내용 |
| --- | --- |
| **RAW** | 실제 원천 데이터의 원본 저장 |
| **Curated** | 표준화, 정제된 데이터 저장 |
| **Synthesize** | 데이터 합성 과정 저장 |
| **Attribution** | 합성된 데이터와 실제 데이터를 매핑|
| **Published** | 해당 원천 시스템에서 제공하는 데이터 |

</details>

[목차로 이동](#목차)

## 문서화

### 성능 최적화
> Spark 성능 최적화

<details>
<summary><a href="/docs/SPARK_PARTITION_ETC_OPTIMIZATION.md">Silver → Gold Shuffle Partition 최적화</a></summary>

- 과도한 Scheduling Overhead가 예상되어 파티션 수 변경 실험을 진행했습니다.
  - 결과: Spark 기본값 200개 대신 실측 최적값 32개를 적용했습니다.
</details>

<details>
<summary><a href="/docs/SPARK_OPTIMIZATION_BROADCAST.md">Silver → Gold Broadcast Join 최적화</a></summary>

- 작은 차량·유가 dimension을 broadcast해 큰 운행 DataFrame의 join shuffle을 줄였습니다.
  - 결과: broadcast 완전 비활성화 대조군이 14.2% 느렸습니다.
</details>

<details>
<summary><a href="/docs/SPARK_OPTIMIZATION_LAZY_CHECKPOINT.md">Silver → Gold Lazy Checkpoint 최적화</a></summary>

- 반복 배정의 lineage 절단은 유지하고 checkpoint 즉시 실행만 지연했습니다.
  - 결과: Jobs·Stages 8.7% 감소, 실행시간 7.3% 단축을 확인했습니다.
</details>

<details>
<summary><a href="/docs/SPARK_OPTIMIZATION_STRATEGIC_CACHING.md">Silver → Gold 전략적 캐싱</a></summary>

- 반복 사용되는 중간 결과만 persist하고 수명주기에 맞춰 unpersist합니다.
  - 결과: 전체 Silver 입력 추가 캐시는 1.0% 느려 적용하지 않았습니다.
</details>

### 파이프라인 설계


### AWS 인프라
> AWS 인프라 운영 중 겪은 장애 해결

<details>
<summary><a href="/docs/troubleshooting/aws/CFN_INSTANCE_ID_PARAM_TYPE.md">[AWS] CloudFormation 배포가 exit 255 로만 죽음</a></summary>

- 인스턴스 ID 파라미터 타입이 `AWS::EC2::Instance::Id`라 배포 role에 없는 `ec2:DescribeInstances` 호출이 거부됐습니다.
  - 결과: `Type: String` + `AllowedPattern`으로 교체해 해결했습니다.
</details>

<details>
<summary><a href="/docs/troubleshooting/aws/GITHUB_OIDC_WRONG_ROLE.md">[AWS] GitHub Actions OIDC가 "Not authorized"로 계속 실패함</a></summary>

- GitHub 레포 Variable이 EC2용 IAM role을 가리키고 있어 OIDC assume이 거부됐습니다.
  - 결과: GitHub Actions 배포 전용 role ARN으로 교체해 해결했습니다.
</details>

<details>
<summary><a href="/docs/troubleshooting/aws/LETSENCRYPT_AMAZONAWS_DOMAIN.md">[AWS] Let's Encrypt가 AWS 기본 제공 도메인엔 인증서를 안 줌</a></summary>

- `*.amazonaws.com` 공유 도메인은 정책상 인증서 발급이 차단되어 있었습니다.
  - 결과: 무료 와일드카드 DNS `sslip.io`로 전환해 해결했습니다.
</details>

<details>
<summary><a href="/docs/troubleshooting/aws/RDS_PRIVATE_SUBNET.md">[AWS] RDS 생성 마법사에 서브넷(VPC) 선택 화면이 안 나옴</a></summary>

- 표준 PostgreSQL 생성 흐름에서는 서브넷 그룹을 고르는 화면 자체가 빠져 있었습니다.
  - 결과: 서브넷 그룹을 CLI로 먼저 만들고 지정해 인스턴스를 생성했습니다.
</details>

<details>
<summary><a href="/docs/troubleshooting/aws/S3_DELETE_PERMISSION_DAG.md">[AWS] S3 DeleteObject 권한 누락</a></summary>

- Airflow DAG가 직접 `DeleteObject`를 호출하는 주체라는 걸 놓쳐 권한이 빠져 있었습니다.
  - 결과: `theone-airflow-role`에 `s3:DeleteObject`를 추가해 해결했습니다.
</details>

### [의사결정 문서]([./docs/decision_making/README.md])
> 팀내 의견 공유를 통해 날짜별 의사결정한 내용(기술/기획 등) 정리


[목차로 이동](#목차)

## 기술 스택

<div align="center">

### Data Processing
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1E4FFF?style=for-the-badge&logo=apacheiceberg&logoColor=white)

### Storage
![AWS S3](https://img.shields.io/badge/S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Amazon RDS](https://img.shields.io/badge/RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white)

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

[목차로 이동](#목차)

## 팀원

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

[목차로 이동](#목차)