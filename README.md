# 🚘 Car4You - 주행 데이터 기반 리스 차량 추천 시스템

1. [프로젝트 소개](#프로젝트-소개)
2. [아키텍처](#아키텍처)
3. 실행 방법
4. 문서화
5. [Team Rule](#team-rule)

## 프로젝트 소개

### 📌 프로젝트 개요
**Car4You**는 뉴욕에서 Uber·Lyft 기사에게 차량을 주 단위로 임대하는 리스 업체를 위한 **주행 데이터 기반 차량 추천 및 대시보드 시스템**입니다.

리스 업체의 고객 담당자(CSM)는 수백 명의 기사를 관리하지만, 어떤 고객이 차량을 변경했을 때 기사의 순수익과 리스 업체의 객단가가 동시에 개선되는지 판단하기 어렵습니다. 기사의 수익을 높이면서 더 비싼 차량을 맞춤 추천할 수 있다면 리스 업체 역시 기사 1인당 렌탈 객단가를 효과적으로 향상시킬 수 있습니다.

본 시스템은 실제 승차공유 운행 기록에서 고객별 주간 수익을 계산하고, 렌탈 시세 및 차량 제원 데이터와 교체 시 손익을 비교하여 **매주/매달 우선적으로 연락할 고객 20명과 맞춤 제안 차량**을 산출합니다.

---

### 🎯 핵심 요약

- **대상**: 뉴욕 Uber·Lyft 기사 대상 차량 리스 업체의 고객 담당자
- **문제점**: 기사에게 더 높은 순수익을 주면서도 리스 업체의 객단가를 끌어올릴 수 있는 차량을 데이터 기반으로 추천하지 못해 객단가 향상 기회 상실
- **해결 방안**: 차량 변경 시 **'기사 순수익 증가 & 리스 업체 렌탈 객단가 상승'** 조건을 충족하는 상위 n명과 제안 차량을 대시보드로 제공하여 담당자가 즉시 추천 영업에 활용
- **핵심 지표**: **상위 n명 차량 변경 시 렌탈 객단가 상승액**

---

### 🔄 Before & After

| 구분 | Before | After |
| --- | --- | --- |
| **차량 추천 방식** | 판단 기준 부재로 감에 의존한 수동/비효율 관리 | 주행 데이터 기반 객관적 순수익·객단가 상승 시뮬레이션 추천 |
| **고객 케어 우선순위** | 수백 명의 기사 중 누구에게 먼저 제안할지 불명확 | 주간 순수익 증가 폭이 가장 큰 **상위 20명 자동 추출** |
| **비즈니스 효과** | 차량 업그레이드 추천 기회 누락으로 객단가 정체 | 기사 순수익 극대화 만족도 증대 & **리스 업체 렌탈 객단가 향상** |

---

## 아키텍처
![System Architecture](architecture.png)

## Team Rule

### 리뷰 검토 Git 훅

팀원은 clone 후 한 번 `make setup-hooks`를 실행합니다. 커밋 전과 푸시 전 훅은 `review-engineering` 스킬을 실행했을 때 남긴 변경 해시 기록만 확인합니다. 스킬 자체를 Git 훅이 실행하지는 않습니다.

검토 후 `python3 .claude/hooks/review_gate.py --pass commit` 또는 `--pass pr`로 해당 기록을 남깁니다. `--no-verify`는 긴급 복구 외에는 사용하지 않으며, 사용했다면 즉시 후속 검토를 남깁니다.

공유 워크플로우 스킬의 기준본은 `.agents/skills`입니다. Claude 환경의 `.claude/skills`는 동일한 정책을 따르되, Claude 전용 훅과 그 경로는 유지합니다. 스킬 정책을 바꿀 때는 두 경로의 대응 파일을 함께 검토합니다.

### Ground Rule
1. 10:00 - 10:15에 점심밥 정하기
2. 생각나는 의견 있으면 숨기지 않고 말하기
3. 작업은 항상 Branch에서 진행하기 ([Branch 규칙](#branch-rule) 참고)
4. 개발 시 주석은 항상 본인이 작성하기
5. PR은 항상 24시간 이내로 다른 사람이 리뷰 후 Merge 해주기
    - PR은 리뷰하기 편하도록 작업 목록 Checklist로 작성하기
6. 모든 작업은 항상 개발 전, Issue 만들기
    - 작업 목적
    - 완료 조건
    - 담당자
    - 예상 완료일
7. 의존성 버전 변경은 기능 변경과 분리된 PR로 올리기
    - `pyproject.toml`과 `uv.lock`을 함께 커밋하기
    - `make check` 통과 후 다른 팀원 1명에게 승인받기

### Branch Rule
- 소문자만 사용
- 띄어쓰기 대신 `-` 사용
- Branch 형상
    ```
    main
    └─ develop
        ├─ feature/12-bike-dag
        ├─ fix/24-spark-xxx
        ├─ refactor/31-docker-xxxx
        └─ docs/35-readme
    ```

- main: 배포 가능한 안정 버전
    - 직접 Commit,Push 금지
    - 항상 PR 병합
    - Test 및 Build 성공한 코드만 병합
    - 일반 기능 개발 Branch 바로 병합 금지 (develop 통해서 병합)
    - 태그 붙이기
         - EX) `v1.0.0   최종 발표 버전`
    - 배포된 버전에서 발생한 긴급 오류만 hotfix 브랜치로 수정 병합
- develop: 기능이 통합되는 개발 버전
    - 모든 작업 Branch 여기서 생성/병합
    - 직접 Push 금지
- 작업 브랜치: 하나의 Issue 단위로 생성
    - EX) `feature/12-bike-station-dag`

    |타입|사용 시점|예시|
    |---|------|---|
    |feature|새로운 기능 개발|feature/12-bike-station-dag|
    |fix|개발 중 발견된 버그 수정|fix/21-spark-null-handling|
    |refactor|동작 변화 없는 코드 구조 개선|refactor/31-etl-pipeline-split|
    |docs|문서 수정|docs/35-readme|
    |test|테스트 추가 및 개선|test/40-dag-unit-test|
    |chore|설정 및 유지보수|chore/44-airflow-docker-setup|
    |hotfix|main 버전 긴급 오류 수정|hotfix/50-kafka-consumer-crash|

### Commit Rule
> `Conventional Commits` 기준 작성
- 형식
    ```bash
    type(scope): subject

    body

    footer
    ```
- 예시
    ```bash
    feat(dag): 따릉이 대여소 수집 DAG 추가

    Airflow DAG를 등록해 공공 API로부터
    대여소 현황 데이터를 매시간 수집하도록 구현

    Closes #12
    ```
- 커밋 타입
    | 타입 | 사용 시점 | 예시 |
    |---|---|---|
    | `feat` | 새로운 기능 추가 | `feat(dag): 따릉이 대여소 수집 DAG 추가` |
    | `fix` | 버그 수정 | `fix(spark): null 값으로 인한 job 실패 수정` |
    | `docs` | 문서 추가/변경 | `docs: 데이터 파이프라인 실행 방법 추가` |
    | `style` | 동작 변화 없는 코드 형식 수정 | `style: PEP8 기준 들여쓰기 정리` |
    | `refactor` | 기능 변화 없는 코드 구조 개선 | `refactor(etl): 적재 로직 함수 분리` |
    | `test` | 테스트 추가 또는 수정 | `test(dag): DAG 태스크 순서 테스트 추가` |
    | `chore` | 설정/패키지/기타 유지보수 | `chore: Airflow Docker 이미지 버전 업데이트` |
    | `build` | 빌드 시스템이나 의존성 변경 | `build: pyspark 의존성 추가` |
    | `ci` | CI/CD 설정 변경 | `ci: GitHub Actions DAG 검증 작업 추가` |
    | `perf` | 성능 개선 | `perf(spark): 파티셔닝으로 셔플 비용 절감` |
    | `revert` | 이전 커밋 되돌리기 | `revert: 카프카 컨슈머 변경 사항 되돌리기` |

### 팀원
|[<img src="https://github.com/kingrangE.png">](https://github.com/kingrangE)|[<img src="https://github.com/taeju-moon.png">](https://github.com/taeju-moon.png)|[<img src="https://github.com/HongJunseong.png">](https://github.com/HongJunseong)|[<img src="https://github.com/inerasable0203.png">](https://github.com/inerasable0203)|
|---------|-----|-----|----|
|전길원|문태주|홍준성|최지욱|
|DE|DE|DE|DE|
