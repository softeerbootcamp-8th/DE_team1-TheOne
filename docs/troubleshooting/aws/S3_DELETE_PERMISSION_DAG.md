# S3 DeleteObject 권한 누락 — 놓친 이유는 "누가 S3를 만지는가"를 서비스 단위로만 셌기 때문

## 증상

Silver 검증(GX) 통과 후 최종 버전 경로로 승격하는 단계에서 실패.

```
AccessDenied: An error occurred (AccessDenied) when calling the DeleteObject
operation: User: arn:aws:sts::572660899671:assumed-role/theone-airflow-role/...
is not authorized to perform: s3:DeleteObject on resource:
"arn:aws:s3:::de-theone/silver/monthly_taxi_trip/year_month=2026-01/....staged.parquet"
because no identity-based policy allows the s3:DeleteObject action
```

GX 검증(15개 expectation, 0 warning)까지는 다 통과한 뒤였다.

## 원인

IAM 권한을 챙길 때 "이 파이프라인에서 S3를 만지는 주체가 몇 개인가"를 **Lambda
핸들러, Spark job 같은 개별 서비스 단위로만** 파악하고 있었다. 그래서 각 서비스가
필요로 하는 `PutObject`/`GetObject`는 다 챙겼는데, 정작 이번에 `DeleteObject`를
호출한 주체는 Lambda도 Spark도 아니라 **DAG 자체(Airflow Python 코드)**였다.

`main/airflow/common/monthly_bronze.py`의 `commit_staged_silver` 함수가
`validate_silver_task` 안에서 boto3로 직접 `copy` + `delete_object`를 호출해
staging 파일을 최종 경로로 승격시키는 구조다. 즉 **오케스트레이션 레이어(DAG)도
데이터 레이크에 직접 쓰기/삭제 작업을 하는 주체 중 하나**인데, "DAG는 서비스들을
호출만 하고 정작 S3 API는 안 부른다"고 암묵적으로 가정하고 있어서 이 권한을
빠뜨렸다. 게다가 이 role은 지금까지 S3에 쓰기(Put)/읽기(Get)만 했지 지우는
동작은 이번이 처음이라, `s3:DeleteObject` 자체가 정책에 없었다.

## 해결

`theone-airflow-role`에 `s3:DeleteObject`를 Silver 버킷 prefix 범위로 추가.

```json
{
  "Effect": "Allow",
  "Action": "s3:DeleteObject",
  "Resource": "arn:aws:s3:::de-theone/silver/*"
}
```
