# 대시보드 조회가 갑자기 30초 가까이 걸림

## 증상

`service_area` 컬럼/PK 마이그레이션을 Gold RDS에 적용한 뒤, 개발 dashboard-server의
Streamlit 대시보드가 페이지를 새로고침할 때마다 한참 멈춘 것처럼 느려짐. 실제로
`driver_aggregation` 대상 쿼리를 `EXPLAIN ANALYZE`로 떠보면 실행 시간이
약 30초 나옴.

```
Seq Scan on driver_aggregation t  (actual time=2.593..29949.468 rows=2000.00 loops=1)
  Filter: (version = (SubPlan 1))
  SubPlan 1
    ->  Aggregate (actual time=2.494..2.494 rows=1.00 loops=12000) # 12000행
          ->  Index Only Scan using driver_aggregation_pkey ... loops=12000
Execution Time: 29949.861 ms
```

## 원인

`main/dashboard/datasource.py`의 `_latest_version_query()`가 연도월별 최신 버전
행을 상관 서브쿼리로 뽑는데, 이 시점엔 서브쿼리 조건이 `year_month`뿐이었음.

```sql
WHERE t.version = (SELECT MAX(version) FROM {table} WHERE year_month = t.year_month)
```

`service_area` 마이그레이션으로 PK가 `(year_month, version, ...)`에서
`(service_area, year_month, version, ...)`로 바뀌면서, `year_month` 단독 조건은 더
이상 PK 인덱스 선두 컬럼이 아니게 되어 인덱스를 타지 못함. (PK 복합 인덱스의 경우 앞에서 부터 b+tree를 탐)

## 해결


```sql
WHERE t.version = (SELECT MAX(version) FROM {table}
                    WHERE service_area = t.service_area AND year_month = t.year_month)
```

service_area도 검색 조건에 추가하면서, service_area, year_month 검색 조건을 compound pk `(service_area, year_month, version, ...)`에 순서대로 맞추면서 인덱스를 타게 만듦.
