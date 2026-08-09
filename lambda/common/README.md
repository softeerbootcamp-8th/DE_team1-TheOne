# lambda/common

Lambda 함수 2개 이상이 같이 쓰는 코드. pyarrow/boto3 등 Lambda 전용 의존성 사용 가능
(`libs/pipeline_core`와 다른 점은 그것뿐 순수 로직만 되는 게 아니라 Lambda 의존성을 써도 됨)

> **주의:** Airflow DAG 가 핸들러를 in-process 로 import 하는 동안에는 여기 있는 모듈을
> 쓸 수 없습니다. `airflow/dags/common/` 이 정규 패키지라 최상위 이름 `common` 을 먼저
> 차지하고, sys.path 순서를 바꿔도 이깁니다. 핸들러가 import 해야 하는 공용 코드는
> `lambda/functions/common/` 에 두고 상대 경로로 부르세요.

- ex, `LocalParquetLoader`, `S3ParquetLoader`
- ex, 여러 크롤러가 같이 쓰는 HTTP 클라이언트(재시도/헤더 공통화)
- 데이터셋 하나에만 쓰이는 것은 제외하기