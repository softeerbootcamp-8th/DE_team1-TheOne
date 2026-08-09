# libs/pipeline_core

`Extractor` / `Transformer` / `Loader` / `Pipeline` 공통 인터페이스 
`lambda`, `spark` 가 로컬 경로 의존성으로 참조

- 3개 런타임(lambda/spark/airflow) 전부에서 재사용 가능하고 **외부 의존성이 0개**인 순수 로직  
    - `abc`/`typing`/`dataclasses` 같은 표준 라이브러리만 사용

- pyarrow, boto3, pyspark 등 특정 런타임 의존성을 쓰는 코드는 `lambda/common/` 또는 `spark/common/`에 작성

