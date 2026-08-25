# 운행·차량 Silver Asset 통합으로 Gold 중복 실행 방지

- 요약
    - 운행·차량 원천 3종의 Silver DAG가 각각 Asset을 발행해 같은 지역·월의 Silver → Gold가 중복 실행
    - 변경 또는 복구가 필요한 DAG를 한곳에서 실행하고 모두 끝날 때까지 대기
    - 마지막에 준비 완료 Asset을 한 번만 발행

> 후속 문제: [Gold 완료 Asset 재발행으로 후속 갱신 복구](./ASSET_EVENT_CONSUMPTION_STOPS_GOLD_REFRESH.md)

## 문제

- Silver → Gold는 월별 택시 운행, 기사·차량 정보, 회사 차량 제원, 연료비를 결합
- 앞의 3개 데이터는 같은 원천 서버에서 가져오지만 별도 DAG가 Bronze → Silver 처리
- 기존에는 각 Silver DAG가 끝날 때마다 Asset 발행

```text
회사 차량 제원 Silver 완료 ───────→ Silver → Gold 실행 1
월별 택시 운행 Silver 완료 ───────→ Silver → Gold 실행 2
```

- Silver 적재는 각각 한 번이지만 Spark 계산과 Gold DB 적재는 두 번 실행
- `max_active_runs=1`은 실행 순서만 직렬화하고 예약된 DagRun은 제거하지 못함
- 원천 서버가 `304 Not Modified`를 반환해도 Bronze·Silver 파일이 없으면 복구가 필요

## 접근

1. 원천 변경 여부: 조건부 `HEAD`로 `ETag`, `Last-Modified` 비교
2. 내부 완료 여부: Bronze·Silver 데이터와 `_SUCCESS` 확인
3. 입력 묶음 완료 여부: 이번에 실행한 Silver DAG가 모두 성공했는지 확인

## 해결

- 통합 조정 DAG가 원천 3종을 검사
- 변경되거나 저장 파일이 불완전한 DAG만 실행
- 실행한 DAG를 모두 기다린 뒤 같은 지역·월의 준비 완료 Asset을 한 번 발행

| 상태 | 동작 |
| --- | --- |
| 원천 미변경, Bronze·Silver 완료 | 실행 및 Asset 발행 생략 |
| 변경 또는 저장 파일 누락 | 필요한 Silver DAG 실행 |
| 실행 대상 모두 성공 | 준비 완료 Asset 1회 발행 |
| 하나라도 실패 | 준비 완료 Asset 미발행 |

- Asset 파티션: `지역:연월`
- 다른 지역·월의 완료 결과는 서로 결합하지 않음

## 검증

- 같은 달에 원천 여러 개가 바뀌어도 준비 완료 Asset 파티션 1개 기록
- `304` 응답이어도 Bronze·Silver 또는 `_SUCCESS`가 빠지면 재실행
- 최신 Bronze에 대응하는 Silver가 완료된 경우에만 실행 생략
- 하위 DAG 실패 시 준비 완료 Asset 미발행

- 관련 코드: [`source_api_refresh_dag.py`](../../../main/airflow/dags/source_api_refresh_dag.py), [`source_api_refresh/tasks.py`](../../../main/airflow/scripts/source_api_refresh/tasks.py)
