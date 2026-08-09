# lambda/common

Lambda 함수 2개 이상이 같이 쓰는 코드. pyarrow/boto3 등 Lambda 전용 의존성 사용 가능
(`libs/pipeline_core`와 다른 점은 그것뿐 순수 로직만 되는 게 아니라 Lambda 의존성을 써도 됨)

- ex, `LocalParquetLoader`, `S3ParquetLoader`
- ex, 여러 크롤러가 같이 쓰는 HTTP 클라이언트(재시도/헤더 공통화)
- 데이터셋 하나에만 쓰이는 것은 제외하기