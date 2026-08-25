# Gold 완료 Asset 재발행으로 후속 갱신 복구

- 요약
    - Gold 완료 Asset을 한 번 발행하면 계속 사용할 수 있는 상태값으로 착각
    - 실제 Asset은 Gold DAG 실행 조건에 사용되면 소비되는 이벤트
    - Silver → Gold와 Gold 산출물 검증이 끝날 때마다 완료 Asset을 다시 발행

> 이전 문제: [운행·차량 Silver Asset 통합으로 Gold 중복 실행 방지](./SOURCE_API_REFRESH_COORDINATES_GOLD.md)

## 문제

- 최초 Gold DAG를 실행할 때는 운행·차량 Silver Asset과 연료비 Silver Asset이 모두 필요
- 이후 Gold DAG 실행을 위해서는 직전 Gold 완료 Asset과 새 Silver Asset이 필요
- 착각: Gold 완료 Asset을 한 번 발행하면 이후 갱신에서도 계속 사용할 수 있다고 판단
- 실제: Asset은 Gold DAG 실행을 예약할 때 소비되어 다음 실행 조건에 다시 사용할 수 없음
- 결과: 두 번째 Gold 실행 후 새 완료 Asset이 없어 다음 연료비 갱신에서 실행 중단

```text
1. 운행·차량 Silver + 연료비 Silver → Gold 실행 1 → Gold 완료 Asset 1
2. Gold 완료 Asset 1 + 운행·차량 Silver 갱신 → Gold 실행 2, Asset 1 소비
3. 연료비 Silver 갱신 → 결합할 Gold 완료 Asset이 없어 실행되지 않음
```

## 해결

- Gold 완료 Asset을 영구 상태가 아닌 다음 갱신용 1회성 이벤트로 처리
- Silver → Gold 실행 후 Gold 산출물 validation이 성공할 때마다 Asset 재발행
- Spark 계산, DB 적재 또는 Gold validation 실패 시 Asset 미발행

```text
운행·차량 Silver + 연료비 Silver → Gold 실행 1 → Gold 완료 Asset 1
Gold 완료 Asset 1 + 운행·차량 Silver 갱신 → Gold 실행 2 → Gold 완료 Asset 2
Gold 완료 Asset 2 + 연료비 Silver 갱신 → Gold 실행 3 → Gold 완료 Asset 3
```

실행 조건:

```text
(운행·차량 Silver Asset AND 연료비 Silver Asset)
OR
(직전 Gold 완료 Asset AND (운행·차량 Silver 갱신 OR 연료비 Silver 갱신))
```

## 검증

| Asset 조합 | Gold 실행 |
| --- | --- |
| 운행·차량 Silver 또는 연료비 Silver만 존재 | 실행하지 않음 |
| 운행·차량 Silver + 연료비 Silver | 실행 |
| Gold 완료 + 운행·차량 Silver 갱신 | 실행 |
| Gold 완료 + 연료비 Silver 갱신 | 실행 |
| Gold 완료만 존재 | 실행하지 않음 |

- Asset 파티션은 `지역:연월` 형식으로 기록
- 다른 지역·월의 Gold 완료 Asset과 Silver 데이터의 Asset은 결합하지 않음

- 관련 코드: [`assets.py`](../../../main/airflow/common/assets.py), [`monthly_taxi_trip_silver_to_gold/tasks.py`](../../../main/airflow/scripts/monthly_taxi_trip_silver_to_gold/tasks.py)
- 회귀 테스트: [`test_monthly_taxi_trip_silver_to_gold_dag.py`](../../../main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py)
