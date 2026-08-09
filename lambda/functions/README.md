# lambda/functions

데이터셋 하나당 폴더 하나:

```
<dataset>/
  extractor.py   # class <Dataset>Extractor(Extractor) — 상단 docstring에 출처 URL/수집
                 # 주기/응답 형태/특이사항을 적습니다. API URL, 헤더 등 설정값도 여기 모듈
                 # 상수로 둡니다.
  handler.py     # lambda_handler — Extractor + Loader(lambda/common) 를 Pipeline 으로 조립.
                 # 파티션 스키마(pyarrow schema)도 보통 여기 상수로 둡니다.
```