# 같은 지역-월의 입력이 준비됐을 때만 추천 계산 진행
- 요약
  - 문제 : Gold 작업에서 상황별로 아래의 문제 발생 가능
    - **입력이 바뀌지 않아도 추천 계산 반복**
    - **다른 입력 준비 전 계산 시작**
    - **일부 늦은 수정으로 기존 값 갱신 지연됨**
  - 접근 : Airflow Asset에 복합키 사용(운영 지역:대상 월)
    - 최초 계산/이후 갱신의 실행 조건 구분
  - 검증 : 각 문제 상황을 시나리오로 작성 후, 자동화 테스트 진행
## 문제
- 배경 
  - Gold 계산에 **정제 데이터 여러 개 필요**
    - 정제 데이터 예시: 기사 차량 정보, 차량 제원/재고, 연료비 등
  - 각 정제 데이터는 
    - **갱신 시점이 다르다.**
    - 계산할 때 **같은 지역과 대상 월에 이용**되어야 한다.
- 문제
  > {상황(if 가정)}:{문제} 
  1. 정해진 시간 실행 : **입력이 바뀌지 않아도 추천 계산 반복**
  2. 상위 작업 하나 완료만 대기 : **다른 입력 준비 전 계산 시작**
  3. 모든 입력 재발행 대기 : **일부 늦은 수정으로 기존 값 갱신 지연됨**
  
## 접근
> 실행 기준 변경 : 무슨 작업 끝났는가 -> 어디의 언제 데이터가 준비됐는가
- Airflow의 Asset에 복합 키 사용
  ```text
  운영 지역:대상 월
  예: NYC:2026-08
  ```
  - 키가 잘못된 경우 실패 처리

## 해결
> 최초와 갱신을 구분하여 해결 그 외 기타 개발 사항
1. **최초 추천 계산** : 아래가 모두 준비되면 시작
  - 필요한 데이터
  - 연료비 데이터
2. **갱신 계산** : 하나만 갱신되어도 다시 계산
  - 필요한 데이터 중 하나 이상
  - 연료비 데이터
  - 여러 원천이 함께 갱신된 경우에는 개별 Silver 완료마다 Gold를 예약하지 않고,
    조정 파이프라인이 대상 작업의 완료를 모아 준비 완료 Asset을 한 번만 발행한다.
    이 과정에서 발생했던 Asset 의미 혼동과 중복 실행 문제는
    [운행·차량 Silver Asset 통합으로 Gold 중복 실행 방지](../troubleshooting/pipeline/SOURCE_API_REFRESH_COORDINATES_GOLD.md)에 정리했다.
  - Gold 완료 Asset도 계속 남는 상태값이 아니라 다음 실행 조건에 사용되는 이벤트다.
    다음 갱신이 중단되지 않도록 Gold 적재와 검증이 끝날 때마다 다시 발행하며, 자세한 과정은
    [Gold 완료 Asset 재발행으로 후속 갱신 복구](../troubleshooting/pipeline/ASSET_EVENT_CONSUMPTION_STOPS_GOLD_REFRESH.md)에 정리했다.
3. **원천 감시** : ETag와 수정 시각으로 변경 확인
  - 원천 그대로여도 아래에 해당하면 다시 처리
    1. 해당 지역/월의 수집 데이터 없음
    2. 최신 원본 데이터에 대응하는 정제 데이터가 없음
4. **추천 계산(Silver to Gold) 전에는 실제 저장소 재확인** 
  - 같은 지역/월의 완료된 정제 데이터 확정 후 계산 작업에 전달

## 검증
> 테스트 생성 후 검증
- 아래 시나리오별 테스트를 만들어 확인
  - 잘못된 키 실패 확인
    - 지역이 없는 과거 키 입력
    - 잘못된 지역/월 입력
  - 최초와 갱신 구분되는지 확인
    - 최초 : 모든 입력이 주어질 때 실행
    - 갱신 : 하나만 변경되어도 실행
  - 수집/정제 데이터 없을 때 재실행 확인 (원천 변경 여부와 관계없이)
  - _SUCCESS 없는 데이터 거부 확인
  - 같은 지역-월 이벤트만 결합하는지 확인
  - 그 외 다수.


## 검토 사항
- 현재 파티션 키 : **지역과 월 두 값 문자열 하나**로 표현
  - 추후 국가나 차량 사업자처럼 범위 차원이 늘어나면 **구조화된 다차원 파티션으로 변경 예정**

## 참고

- [`main/airflow/common/assets.py`](../../main/airflow/common/assets.py): 지역/월 파티션 키와 입력 조건
- [`main/airflow/scripts/source_api_refresh/tasks.py`](../../main/airflow/scripts/source_api_refresh/tasks.py): 원천 변경과 내부 데이터 누락 판정
- [`main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/dags/monthly_taxi_trip_raw_to_silver_dag.py): 파티션별 추천 계산 실행
- [`main/airflow/scripts/monthly_taxi_trip_silver_to_gold/tasks.py`](../../main/airflow/scripts/monthly_taxi_trip_raw_to_silver/tasks.py): 실행 직전 입력 확인
- [`main/airflow/tests/test_assets_partition_key.py`](../../main/airflow/tests/test_assets_partition_key.py): 복합 키 테스트
- [`main/airflow/tests/test_source_api_refresh_dag.py`](../../main/airflow/tests/test_source_api_refresh_dag.py): 변경 감지와 내부 누락 복구 테스트
- [`main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/tests/test_monthly_taxi_trip_silver_to_gold_dag.py): 최초/후속 실행과 입력 완결성 테스트
