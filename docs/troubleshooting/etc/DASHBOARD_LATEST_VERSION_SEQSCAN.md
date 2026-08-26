# 대시보드 driver_car_suggestion 조회가 5.4초 걸림

## 증상

Streamlit 대시보드 렌더링 시 `driver_car_suggestion` 최신 버전 조회가 5.4초 걸림.
`EXPLAIN ANALYZE` 결과:

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

## 원인

매 행마다 MAX(version)을 찾는 과정을 반복하고 있었음

```sql
SELECT t.* FROM driver_car_suggestion t
WHERE t.version = (
    SELECT MAX(version) FROM driver_car_suggestion
    WHERE service_area = t.service_area AND year_month = t.year_month
      AND recommendation_algorithm_version_id = t.recommendation_algorithm_version_id
      AND threshold = t.threshold
)
```


## 해결

`MAX(version)`을 한 번만 계산하도록 윈도우 함수로 재작성. (파티션마다 한번)

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

윈도우 함수로 MAX(version)을 찾는 과정을 딱 한번만 하도록 변경.

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