# 추천 알고리즘 v1/v2 병행과 기사 순수익 증가 하한 스윕

- 요약
  - 문제 : 기사 순수익 우선 배정(v1) 하나만 운영해 회사 매출 관점 리포트가 없음 (#978)
  - 접근 : 회사 매출 우선·기사 순수익 증가 하한 필터 배정(v2)을 v1과 함께 운영, 기사 순수익 증가 하한 100~500 5개 스윕
  - 해결 : 기존 `driver_car_suggestion`에 알고리즘 버전·기사 순수익 증가 하한 컬럼을 추가해 재사용, 중복 저장 없이 리포트 확장

## 문제

리스 업체는 기사 순수익 증가를 최우선으로 차량을 배정하는 알고리즘(v1, `ProfitFirstAlgorithm`) 하나만 쓰고 있었다. v1 결과만으로는 회사 매출 관점의 리포트를 만들 수 없었다.

## 접근

회사 매출 증가를 1순위로 삼고, 기사 순수익 증가 하한을 넘는 배정만 후보로 남기는 알고리즘(v2, `RevenueFirstAlgorithm`)을 새로 만들었다. 기사 순수익 증가 하한은 하나로 고정하지 않고 100/200/300/400/500 다섯 값을 함께 스윕해, 리스 업체가 여러 기준에서의 결과를 비교할 수 있게 했다.

저장 방식은 두 가지를 놓고 비교했다.

1. v1 결과를 기사 순수익 증가 하한 5개 값 각각에 복사해서 5배로 저장
2. 기사 순수익 증가 하한을 축으로 추가해 알고리즘·기사 순수익 증가 하한 조합별로 한 번씩만 저장

1번은 완전히 같은 데이터를 5배로 중복 저장하는 낭비라 기각했다. 기사 순수익 증가 하한을 쓰지 않는 v1은 `threshold=-1`(sentinel)로 통일해서 한 번만 저장하기로 했다. 실제 기사 순수익 증가 하한 값은 항상 0 이상이라, `-1`은 "이 알고리즘에는 기사 순수익 증가 하한 축이 없다"는 뜻으로 다른 값과 명확히 구분된다.

## 해결

새 Gold 산출물을 만들지 않고 기존 `driver_car_suggestion`을 재사용했다. `recommendation_algorithm_version_id`(알고리즘 버전)·`threshold`(기사 순수익 증가 하한) 두 컬럼을 추가해 알고리즘·기사 순수익 증가 하한 조합별 행으로 관리하고, 두 컬럼 모두 기본키에 포함시켰다.

한 번의 Gold 실행에서 v1 결과 1세트와 v2의 기사 순수익 증가 하한별 결과(5세트)를 함께 계산해 적재한다. 기사 순수익 증가 하한 목록은 코드 기본값(100/200/300/400/500)이지만, Airflow Variable(`gold_recommendation_thresholds`)로 운영 중에도 조정할 수 있게 했다.

어떤 기사 순수익 증가 하한을 골라도 "차량 교체 때문에 기존보다 순수익이 줄어드는 배정"은 나오지 않아야 한다는 조건도 함께 걸었다. 기사 순수익 증가 하한은 항상 0 이상이고, 차량을 바꾸지 않는 "현재 차량 유지"는 재고 경쟁 없이 항상 후보에 남기 때문에, 이 조건은 알고리즘 필터만으로 구조적으로 보장된다.

## 검증

이 조건("교체로 손해 보는 배정 없음")을 회귀 테스트로 확인한다.

```bash
cd main/spark && PYTHONPATH=../.. uv run --frozen pytest tests/test_validate_gold_business_invariants.py
```

## 결론

알고리즘·기사 순수익 증가 하한을 기본키에 태그하는 방식은 저장 행 수는 늘지만, v1/v2와 여러 기사 순수익 증가 하한을 한 실행에서 함께 계산해 리포트를 확장하는 데 새 산출물이나 중복 저장이 필요 없다.

재실행 한 번당 쌓이는 행 수가 (알고리즘 수 × 기사 순수익 증가 하한 수)만큼 늘어나 Gold 실행 버전을 얼마나 보존·정리할지의 문제가 더 커진다. 이 부분은 별도 이슈(#971)에서 다룬다.

## 참고

- [`main/spark/jobs/silver_to_gold/recommendation_algorithm/profit_first.py`](../../main/spark/jobs/silver_to_gold/recommendation_algorithm/profit_first.py): v1
- [`main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py`](../../main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py): v2, `DEFAULT_THRESHOLDS`
- [`main/spark/jobs/silver_to_gold/job.py`](../../main/spark/jobs/silver_to_gold/job.py): 두 알고리즘을 함께 계산해 적재
- [`main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py`](../../main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py): `gold_recommendation_thresholds` Variable
- [`schema/gold/__init__.py`](../../schema/gold/__init__.py): `DriverCarSuggestion.recommendation_algorithm_version_id`/`threshold`
- [`main/spark/tests/test_validate_gold_business_invariants.py`](../../main/spark/tests/test_validate_gold_business_invariants.py): 손해 없는 배정 검증
- 원본 논의: `docs/decision_making/0825.md` 2번 항목, #978, #997
