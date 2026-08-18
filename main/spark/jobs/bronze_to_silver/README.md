# spark/jobs/bronze_to_silver

Bronze를 정제/검증해 Silver로 옮기는 job 

데이터셋 하나당 폴더

```
<dataset>/
  transformer.py   # 정제/검증 로직만
  job.py             # spark-submit 엔트리포인트. SparkParquetExtractor(spark/common)로
                       # Bronze 읽고, Transformer 적용 후, SparkParquetLoader로 Silver 저장.
                       # Pipeline(extractor, transformer=..., loader=...).run() 한 줄로 조립
```

이 레이어에서는 정제만 — 집계는 `spark/jobs/silver_to_gold/`
