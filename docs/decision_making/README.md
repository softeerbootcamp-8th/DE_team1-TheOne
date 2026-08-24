# 프로젝트 진행하며 의사 결정한 목록
- 형식 : `주제:결론`
    - `결론 사유`는 `상세 정리 문서 링크`를 통해 확인 가능
### 8/4 ([상세 정리 문서 링크](/docs/decision_making/0804.md))
1. 차량 렌탈비 고정 여부 : `렌탈 최소 금액`으로 고정
2. RDB v.s. Athena v.s. 둘 다 안 쓰기 : `RDS 도입`
3. 순수익 정렬 기준 : `%로 정렬`하기
4. 차량 추천 연산 시간 선정 : 새벽 시간대에 배치로 연산
5. 골드 레이어 요소 선정 : 현재 기사의 순수익 / 차량 추천 목록 / 현재 추천 순수익 차이
6. 고객 범위 (대여 후 기간) : 대여 후 

### 8/6 ([상세 정리 문서 링크](/docs/decision_making/0806.md))
1. 차량 추천 개수 : 차량 타입(X,Comport,XL)별 `2개`씩
2. 예상 수입 값 추가 여부 : 예상 수입을 넣자
3. 기사 집계 기간 : `주단위 집계` / 데이터 보여줄 때 `집계 기간 명시`
4. 휘발유/전기 수집 : `각각 개별 API`를 이용하여 `일 단위 평균`으로 집계

### 8/10 ([상세 정리 문서 링크](/docs/decision_making/0810.md))
1. 차 등급 구분 : Premium만 추출하고 나머지는 Standard로 적용
2. 문제 재정의 : 기사에게 더 높은 순수익을 주면서도 리스 업체의 객단가를 끌어올릴 수 있는 차량을 데이터 기반으로 추천하지 못해 `객단가 향상 기회 상실`
3. Airflow 호스팅 위치 : EC2에 올리기로 결정
4. Airflow task 단위 : 기본적으로 논리적 단위로 묶은 것을 Task로 정의, 5분 이상 걸리는 작업은 Task 분리


### 8/11 ([상세 정리 문서 링크](/docs/decision_making/0811.md))
1. 지역 확장성 모델링 고려 : City,State도 함께 저장
2. Docker CI 시간 단축 : setup-buildx-action을 사용하여 캐시 이용
3. ISSUE/PR skill 제작 : Skill로 개발 외의 작업을 AI가 대신해주도록 제작

### 8/12 ([상세 정리 문서 링크](/docs/decision_making/0812.md))
1. Airflow 인프라(EC2 v.s. ECS Fargate) : EC2 사용
2. Iceberg 도입 여부 : 도입 찬성
3. Hive Style Partitioning 도입 : S3 기준 모든 데이터에 도입 / Iceberg 파티셔닝은 HVFHV에만

### 8/13 ([상세 정리 문서 링크](/docs/decision_making/0813.md))
1. Silver에서 단계를 분할하여 저장할 것인가? : 정제와 비즈니스 로직을 분리
2. Silver 책임 분리 정도 : 정제된 데이터를 재사용할 가능성이 있다거나 재사용한다면 분리

### 8/16 ([상세 정리 문서 링크](/docs/decision_making/0816.md))
1. 가짜 데이터 합성 데이터 파이프라인 포함 여부 : 파이프라인에서 분리하기

### 8/18 ([상세 정리 문서 링크](/docs/decision_making/0818.md))
1. 휘발유/전기 요금 수집 방향 : 무조건 월별/주별로 고정

### 8/19 ([상세 정리 문서 링크](/docs/decision_making/0819.md))
1. Airflow Task v.s. lambda : Lambda

### 8/23 ([상세 정리 문서 링크](/docs/decision_making/0823.md))
1. EMR Serverless 이미지 배포 : Job 제출 시점에 이미지 digest 해석

### 8/24 ([상세 정리 문서 링크](/docs/decision_making/0824.md))
1. 지역별 Airflow DagRun 동시 실행 상한 : `max_active_runs=3`
2. Gold 세 번째 물리 테이블 : `monthly_report` 대신 `lease_vehicle_inventory` 적재
3. Bronze·Silver S3 공개 : 최종 경로 직접 적재 후 검증 성공 시 `_SUCCESS` 기록
