# spark/jobs/silver_to_gold

Silver(정제된 데이터)를 집계해 Gold로 옮기는 job

```
<dataset>/
  transformer.py   # 집계 로직 (기사 주단위 집계, 차량 추천 목록 산출 등)
  job.py             # Silver 읽고 집계 후 Gold 저장
                       # Gold 목적지가 RDS면 PostgresLoader를 만들어서 사용
```