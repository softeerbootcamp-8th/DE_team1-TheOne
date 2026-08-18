# 팀 규칙

이 저장소에서 작업할 때 지키는 규칙입니다.
로컬 환경 구성은 [GETTING_STARTED.md](GETTING_STARTED.md) 를 보세요.

- [Ground Rule](#ground-rule)
- [Branch Rule](#branch-rule)
- [Commit Rule](#commit-rule)
- [PR Rule](#pr-rule)
- [리뷰 검토 Git 훅](#리뷰-검토-git-훅)

---

## Ground Rule

1. 10:00 - 10:15에 점심밥 정하기
2. 생각나는 의견 있으면 숨기지 않고 말하기
3. 작업은 항상 Branch에서 진행하기 ([Branch Rule](#branch-rule) 참고)
4. 개발 시 주석은 항상 본인이 작성하기
5. PR은 항상 24시간 이내로 다른 사람이 리뷰 후 Merge 해주기
    - PR은 리뷰하기 편하도록 작업 목록 Checklist로 작성하기
6. 모든 작업은 항상 개발 전, Issue 만들기
    - 작업 목적 / 완료 조건 / 담당자 / 예상 완료일
7. 의존성 버전 변경은 기능 변경과 분리된 PR로 올리기
    - `pyproject.toml` 과 `uv.lock` 을 함께 커밋하기
    - `make check` 통과 후 다른 팀원 1명에게 승인받기

---

## Branch Rule

- 소문자만 사용, 띄어쓰기 대신 `-` 사용

```
main
└─ develop
    ├─ feature/12-bike-dag
    ├─ fix/24-spark-xxx
    ├─ refactor/31-docker-xxxx
    └─ docs/35-readme
```

- **main**: 배포 가능한 안정 버전
    - 직접 Commit·Push 금지, 항상 PR 병합
    - Test 및 Build 성공한 코드만 병합
    - 일반 기능 개발 Branch 바로 병합 금지 (develop 통해서 병합)
    - 태그 붙이기 (예: `v1.0.0 최종 발표 버전`)
    - 배포된 버전에서 발생한 긴급 오류만 hotfix 브랜치로 수정 병합
- **develop**: 기능이 통합되는 개발 버전
    - 모든 작업 Branch 여기서 생성/병합, 직접 Push 금지
- **작업 브랜치**: 하나의 Issue 단위로 생성

| 타입 | 사용 시점 | 예시 |
| --- | --- | --- |
| `feature` | 새로운 기능 개발 | `feature/12-bike-station-dag` |
| `fix` | 개발 중 발견된 버그 수정 | `fix/21-spark-null-handling` |
| `refactor` | 동작 변화 없는 코드 구조 개선 | `refactor/31-etl-pipeline-split` |
| `docs` | 문서 수정 | `docs/35-readme` |
| `test` | 테스트 추가 및 개선 | `test/40-dag-unit-test` |
| `chore` | 설정 및 유지보수 | `chore/44-airflow-docker-setup` |
| `hotfix` | main 버전 긴급 오류 수정 | `hotfix/50-kafka-consumer-crash` |

---

## Commit Rule

> [Conventional Commits](https://www.conventionalcommits.org/) 기준

```
type(scope): subject

body

footer
```

```
feat(dag): 따릉이 대여소 수집 DAG 추가

Airflow DAG를 등록해 공공 API로부터
대여소 현황 데이터를 매시간 수집하도록 구현

Closes #12
```

| 타입 | 사용 시점 | 예시 |
| --- | --- | --- |
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

---

## PR Rule

- 템플릿은 [.github/pull_request_template.md](../.github/pull_request_template.md) 를 따릅니다.
- `변경 사항 요약` 은 200자 이내로 씁니다 — 길어지면 리뷰어가 읽지 않습니다.
- 작업 목록은 Checklist로, 각 항목은 한 줄로 압축합니다.
- 이슈 번호를 `Closes #12` 로 연결합니다.
- 24시간 이내에 다른 팀원 1명이 리뷰 후 Merge 합니다.

CI가 PR마다 4가지를 자동 검증합니다 — 락파일 드리프트 · 테스트 · 이미지 빌드 · DAG import.
상세는 [.github/workflows/ci.yml](../.github/workflows/ci.yml) 과 README의 *데이터 품질* 절을 보세요.

---

## 리뷰 검토 Git 훅

팀원은 clone 후 한 번 `make setup-hooks` 를 실행합니다.

```bash
make setup-hooks
```

커밋 전·푸시 전 훅은 `review-engineering` 스킬을 실행했을 때 남긴 **변경 해시 기록만** 확인합니다.
스킬 자체를 Git 훅이 실행하지는 않습니다.

검토 후 아래로 기록을 남깁니다.

```bash
python3 .claude/hooks/review_gate.py --pass commit   # 커밋 전
python3 .claude/hooks/review_gate.py --pass pr       # 푸시 전
```

`--no-verify` 는 긴급 복구 외에는 사용하지 않으며, 사용했다면 즉시 후속 검토를 남깁니다.

공유 워크플로우 스킬의 기준본은 `.agents/skills` 입니다.
Claude 환경의 `.claude/skills` 는 동일한 정책을 따르되, Claude 전용 훅과 그 경로는 유지합니다.
스킬 정책을 바꿀 때는 두 경로의 대응 파일을 함께 검토합니다.
