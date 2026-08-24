# 입력 준비 상태를 보장하는 파티션 Asset 실행 계약

## 1. 문서 목적

Gold는 같은 지역·월의 API Silver 묶음과 연료비 Silver가 준비됐을 때 실행해야 한다.
이 문서는 시간표 대신 파티션 Asset을 사용한 이유, 최초 실행과 이후 재실행 조건을
다르게 구성한 판단을 정리한다.

- Asset 계약: [`assets.py`](../../main/airflow/common/assets.py)
- Gold DAG: [`monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py)
- 계약 테스트: [`test_monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py)

## 2. Asset 조건에서 발견한 문제

시간표로 Gold를 실행하면 상류가 늦거나 실패해도 job이 시작된다. 그렇다고 API READY와
Fuel을 단순 `OR`로 묶으면 최초 실행에서 입력 하나만 준비돼도 Gold가 실행된다.

반대로 두 입력을 항상 `AND`로 묶으면 최초 Gold 성공 이후 API나 Fuel 한쪽만 갱신됐을
때 재계산되지 않는다. 최초 준비 조건과 증분 재실행 조건이 달랐다.

## 3. 적용 판단

Asset 이벤트를 지역·월 파티션으로 구분하고, 최초 실행 성공 여부를 별도 READY Asset으로
기록했다.

```text
{service_area}:{year_month}
```

예시는 `NYC:2026-08`이다. Airflow Asset에는 다차원 파티션이 없어 두 축을 하나의
문자열로 표현하고 `IdentityMapper`로 같은 키를 Gold DagRun에 전달한다.

## 4. 적용 조건과 이유

현재 Gold schedule 조건은 다음과 같다.

```python
GOLD_INPUTS = (
    (API_SILVER_REFRESH_READY & FUEL_PRICE_SILVER)
    | (GOLD_INPUTS_READY & (API_SILVER_REFRESH_READY | FUEL_PRICE_SILVER))
)
```

### 4.1 최초 실행

`API_SILVER_REFRESH_READY & FUEL_PRICE_SILVER`로 두 입력이 같은 파티션에 모두 있어야
실행한다.

### 4.2 이후 재실행

Gold의 `validate_inputs`가 성공하면 같은 파티션에 `GOLD_INPUTS_READY`를 발행한다.
이후에는 API 또는 Fuel 중 하나가 갱신돼도 재실행한다.

### 4.3 Asset 발행 시점

Asset은 writer 종료가 아니라 파일·경로·품질 검증과 `_SUCCESS` 발행 뒤에 기록한다.
Task 성공과 데이터 공개 가능 상태를 구분하기 위해서다.

## 5. 조건 대조

| 입력 Asset 상태 | Gold 실행 |
|---|---|
| 최초 API READY만 존재 | 실행하지 않음 |
| 최초 Fuel만 존재 | 실행하지 않음 |
| 최초 API READY + Fuel | 실행 |
| `GOLD_INPUTS_READY` + API 갱신 | 재실행 |
| `GOLD_INPUTS_READY` + Fuel 갱신 | 재실행 |
| `GOLD_INPUTS_READY`만 존재 | 실행하지 않음 |

테스트는 `AssetEvaluator`로 각 조합을 평가하고, `IdentityMapper`가 지역·월 키를 그대로
전달하는지 확인한다.

## 6. 실패 조건

- 생산자가 `2026-08`, 소비자가 `NYC:2026-08`을 사용하면 오류 없이 트리거가 멈춘다.
- Asset 실행에서 Param 기본값을 우선하면 TX 이벤트가 NYC 처리로 바뀔 수 있다.
- 개별 API Silver를 Gold에 직접 연결하면 완료 시각마다 중복 실행될 수 있다.
- `max_active_runs=1`은 중복 DagRun을 제거하지 않고 순서대로 실행할 뿐이다.
- validation 전에 Asset을 발행하면 미완료 파일이 하류로 공개된다.

## 7. 재검증 절차

1. 같은 지역·월의 API READY와 Fuel 조합으로 최초 실행 조건을 평가한다.
2. `GOLD_INPUTS_READY` 이후 한쪽만 갱신해 재실행 조건을 평가한다.
3. 서로 다른 지역 또는 월의 Asset이 결합되지 않는지 확인한다.
4. Asset DagRun에서는 파티션 키가 Param보다 우선하는지 확인한다.
5. 검증 실패와 marker 누락 시 Asset이 발행되지 않는지 확인한다.
6. 새 생산자를 추가할 때 READY Asset의 의미가 바뀌지 않는지 검토한다.

## 8. 결론

Gold의 Asset schedule은 단순한 `OR` 또는 `AND`가 아니다. 최초에는 전체 입력을 기다리고,
검증 완료 상태가 남은 뒤에는 한쪽 변경만으로 재실행해야 한다. 파티션 키와 READY Asset을
함께 사용해 같은 지역·월의 준비 상태를 명시적으로 표현했다.
