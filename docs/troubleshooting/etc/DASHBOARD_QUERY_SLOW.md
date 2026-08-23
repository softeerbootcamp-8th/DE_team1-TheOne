# 대시보드 조회가 갑자기 30초 가까이 걸림

## 증상

`service_area` 컬럼/PK 마이그레이션을 Gold RDS에 적용한 뒤, 개발 dashboard-server의
Streamlit 대시보드가 페이지를 새로고침할 때마다 한참 멈춘 것처럼 느려짐. 실제로
`driver_aggregation`(12,000행) 대상 쿼리를 `EXPLAIN ANALYZE`로 떠보면 실행 시간이
약 30초 나옴.

```
Seq Scan on driver_aggregation t  (actual time=2.593..29949.468 rows=2000.00 loops=1)
  Filter: (version = (SubPlan 1))
  SubPlan 1
    ->  Aggregate (actual time=2.494..2.494 rows=1.00 loops=12000)
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
이상 PK 인덱스 선두 컬럼이 아니게 됨. 그 결과 서브쿼리 한 번(바깥 12,000행마다 반복)이
인덱스를 효율적으로 seek하지 못하고 사실상 전체를 훑는 꼴이 되어 O(n²)에 가깝게
느려짐.

## 해결

같은 파일이 이미 `#847`(다른 지역 데이터가 조용히 사라지는 정합성 버그) 수정으로
상관 조건에 `service_area`가 추가돼 있었음.

```sql
WHERE t.version = (SELECT MAX(version) FROM {table}
                    WHERE service_area = t.service_area AND year_month = t.year_month)
```

이 조건이 PK `(service_area, year_month, version, ...)`의 선두 두 컬럼과 정확히
일치해서 인덱스를 다시 탈 수 있게 되고, 부수 효과로 성능 문제도 함께 해결됨. 단,
develop에 이 수정이 머지돼 있어도 **dashboard-server 컨테이너를 그 이미지로
재배포하지 않으면** 예전 컨테이너는 여전히 느린 쿼리를 그대로 씀 — 코드가 고쳐졌다고
운영 중인 컨테이너까지 저절로 바뀌지 않음.
