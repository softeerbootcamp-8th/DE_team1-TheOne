# DE 1조 

1. [프로젝트 소개](#프로젝트-소개)
2. [아키텍처](#아키텍처)
3. 실행 방법
4. 문서화
5. [Team Rule](#team-rule)

## 프로젝트 소개
### 기능

## 아키텍처
![System Architecture](architecture.png)

## Team Rule
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
