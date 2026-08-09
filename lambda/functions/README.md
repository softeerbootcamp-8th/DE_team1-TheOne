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

`common/` 은 데이터셋이 아니라 **핸들러들이 같이 쓰는 코드**입니다 (`logging_setup.py` 등).
`lambda/common/` 이 아니라 여기 있는 이유는 `airflow/dags/common/` 이 Airflow 프로세스에서
최상위 이름 `common` 을 먼저 점유해 `lambda/common` 하위 모듈을 import 할 수 없기 때문입니다.
핸들러에서는 `from ..common.logging_setup import ...` 처럼 상대 경로로 부르세요 —
이러면 Lambda 이미지(`functions.*`), Airflow in-process(`lambda.functions.*`), pytest 에서
모두 같은 경로로 풀립니다.