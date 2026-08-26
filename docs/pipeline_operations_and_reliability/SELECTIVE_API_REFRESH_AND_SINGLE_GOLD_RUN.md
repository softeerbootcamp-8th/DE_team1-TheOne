# 변경된 API 데이터만 처리하고 최종 계산은 한 번만 실행

## 요약

- API 변경 여부와 내부 결과를 함께 확인해 필요한 하위 DAG만 실행한다.
- 시간 설정 대신 Asset 준비 완료를 기준으로 같은 지역·월의 Gold를 한 번만 실행한다.

## 문제

Gold는 월간 운행, 기사·차량 상태, 보유 차량, 연료비 Silver를 함께 사용한다.
각 작업이 끝날 때마다 Gold를 시작하면 같은 지역·월을 중복 계산한다. 반면 시간 설정으로
실행되는 DAG는 입력이 지연되면 실패하고, 이를 피하려 실행 시간을 늦추면 불필요하게 기다린다.

앞의 세 데이터는 같은 API 서버에서 오지만 서로 다른 DAG로 처리된다. API가 바뀌지
않았더라도 이전 실행 실패나 파일 삭제로 내부 Bronze·Silver가 없을 수 있으므로,
외부 변경 여부만 보고 실행을 생략할 수도 없다.

## 결정

- API 변경 여부는 `ETag`와 `Last-Modified`를 `HEAD` 요청으로 확인한다.
- 데이터가 그대로여도 Bronze·Silver 결과가 없으면 복구 대상으로 본다.
- DAG가 필요한 작업을 모두 기다린 뒤 준비 완료 Asset을 한 번만 발행한다.
- Gold는 같은 `운영 지역:대상 월`의 API 데이터 3종 Silver와 연료비 Silver가 준비될 때 시작한다.


시간 설정으로 실행되는 DAG는 입력이 실행 시각 전에 준비된다고 가정할 뿐, 실제 준비
상태를 알 수 없다.
Asset은 검증을 통과한 데이터와 파티션을 실행 조건으로 사용하므로, 늦게 끝난 입력은 완료
즉시 반영하고 변경이 없는 날에는 Spark 계산과 적재를 반복하지 않는다.

## 동작

| API 상태 | 내부 처리 상태 | 실행 결과 |
| --- | --- | --- |
| 변경 없음 | 수집·정제 완료 | 실행 생략 |
| 변경 없음 | Bronze 또는 Silver 누락 | 해당 하위 DAG 실행 |
| 변경됨 | 무관 | 변경된 하위 DAG 실행 |

1. 선택한 하위 DAG를 모두 기다린다.
2. 하나라도 실패하면 준비 완료 Asset을 발행하지 않는다.
3. 모두 성공하면 지역·월별 준비 완료 Asset을 한 번 발행한다.
4. 연료비까지 준비되면 Gold를 실행한다.
5. Gold 검증 성공 후 완료 Asset을 다시 발행해 다음 갱신을 받는다.


## 검증

| 실행 상황 | 결과 |
| --- | --- |
| 모두 미변경이고 내부 결과 정상 | 실행하지 않음 |
| 하나만 변경 또는 누락 | 해당 DAG만 실행 후 Asset 1회 발행 |
| 여러 데이터가 변경 | 선택한 DAG가 모두 성공한 뒤 1회 발행 |
| 하나라도 실패 | Asset과 Gold 실행 없음 |
| 지역 또는 월이 다름 | 파티션별로 분리 |

완료 Asset이 소비된 뒤에도 다음 API 또는 연료비 갱신이 Gold를 다시 시작하는지 회귀
테스트로 확인한다. 검증에 실패한 Gold는 다음 갱신용 완료 Asset을 발행하지 않는다.

## 관련 자료

- [`main/airflow/scripts/source_api_refresh/tasks.py`](../../main/airflow/scripts/source_api_refresh/tasks.py)
- [`main/airflow/dags/source_api_refresh_dag.py`](../../main/airflow/dags/source_api_refresh_dag.py)
- [`main/airflow/common/assets.py`](../../main/airflow/common/assets.py)
- [`main/airflow/tests/test_source_api_refresh_dag.py`](../../main/airflow/tests/test_source_api_refresh_dag.py)
- [`main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py)
- [여러 완료 이벤트로 Gold가 중복 실행된 원인](../troubleshooting/pipeline/SOURCE_API_REFRESH_DUPLICATE_GOLD.md)
- [완료 이벤트 소비 후 후속 갱신이 멈춘 원인](../troubleshooting/pipeline/ASSET_EVENT_CONSUMPTION_STOPS_GOLD_REFRESH.md)
