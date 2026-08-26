# 행마다 최신 버전을 다시 계산해 대시보드 조회가 5.4초 걸리던 문제

- 요약
  - 대시보드가 보여주는 추천 결과 표에서 최신 버전을 고르는 조회가, 행마다 최신 버전을 서브쿼리로 다시 계산하고 있었음
  - 같은 조합(지역·연월·알고리즘 버전·기준값)의 최신 버전을 한 번만 계산하도록 윈도우 함수로 재작성
  - 실행 시간이 5423.773ms에서 812.079ms로, 읽은 페이지 수는 1,215,042에서 43,922로 줄음

## 문제

대시보드가 보여주는 표 중 하나인 '기사별 추천 차량 결과'는 운영 지역·연월·알고리즘 버전·기준값(threshold) 조합마다 여러 번 다시 계산된 결과가 쌓인다. 계산이 다시 돌 때마다 새 버전 번호가 붙기 때문에, 화면에는 조합별로 가장 최근 버전만 걸러서 보여줘야 한다.

이 조회가 대시보드를 열 때마다 5.4초 걸렸다. 실행 계획을 확인해보니(`EXPLAIN ANALYZE`) 같은 서브쿼리가 386,704번 반복 실행되고 있었다.

```
Seq Scan on driver_car_suggestion t  (actual time=1138.485..5412.365 rows=168324 loops=1)
  Filter: (version = (SubPlan 2))
  Rows Removed by Filter: 218380
  Buffers: shared hit=1215042
  SubPlan 2
    ->  Result  (actual time=0.013..0.013 rows=1 loops=386704)
          InitPlan 1
            ->  Limit  (actual time=0.013..0.013 rows=1 loops=386704)
                  ->  Index Only Scan Backward using idx_driver_car_suggestion_area_month_algorithm_threshold ... loops=386704
Execution Time: 5423.773 ms
```

## 접근과 해결

조회 조건을 보면 이유를 알 수 있었다. 한 행을 볼 때마다 "같은 조합(지역·연월·알고리즘 버전·기준값)에서 최신 버전이 몇인지"를 서브쿼리로 매번 다시 찾고 있었다.

```sql
SELECT t.* FROM driver_car_suggestion t
WHERE t.version = (
    SELECT MAX(version) FROM driver_car_suggestion
    WHERE service_area = t.service_area AND year_month = t.year_month
      AND recommendation_algorithm_version_id = t.recommendation_algorithm_version_id
      AND threshold = t.threshold
)
```

전체 386,704개 행마다 이 서브쿼리를 한 번씩 실행한 셈이라, 같은 조합의 최신 버전을 계속 반복해서 다시 구하고 있었다. 최신 버전 찾는 방식을 윈도우 함수로 바꿔서, 조합별로 최댓값을 한 번만 계산해 모든 행에 붙이도록 했다.

```sql
SELECT t.driver_id, ... FROM (
    SELECT t.*,
           MAX(t.version) OVER (
               PARTITION BY t.service_area, t.year_month,
                            t.recommendation_algorithm_version_id, t.threshold
           ) AS partition_latest_version
    FROM driver_car_suggestion t
) sub
WHERE version = partition_latest_version
```

## 검증

같은 조회를 다시 `EXPLAIN ANALYZE`로 떠서 비교했다.

| 항목 | 적용 전 | 적용 후 |
| --- | --- | --- |
| 실행 시간 | 5423.773 ms | 812.079 ms |
| 읽은 페이지 수(Buffers) | 1,215,042 | 43,922 |

```
Subquery Scan on sub  (actual time=8.316..803.193 rows=168324 loops=1)
  Filter: (sub.version = sub.partition_latest_version)
  Rows Removed by Filter: 218380
  Buffers: shared hit=43922
  ->  WindowAgg  (actual time=7.581..760.344 rows=386704 loops=1)
        Window: w1 AS (PARTITION BY t.service_area, t.year_month, t.recommendation_algorithm_version_id, t.threshold)
        ->  Index Scan using idx_driver_car_suggestion_area_month_algorithm_threshold on driver_car_suggestion t
              Index Searches: 1
Execution Time: 812.079 ms
```

실행 시간이 약 85% 줄었고, 읽은 페이지 수도 1,215,042에서 43,922로 크게 줄었다. 최종 결과 행 수(168,324행 통과, 218,380행 제외)는 적용 전후 동일해 조회 결과 자체는 바뀌지 않았다.

## 참고

- 관련 코드: `main/dashboard/datasource.py`의 최신 버전 조회 로직
