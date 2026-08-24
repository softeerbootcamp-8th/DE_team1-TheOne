# S3 DeleteObject 권한 누락 — 놓친 이유는 "누가 S3를 만지는가"를 서비스 단위로만 셌기 때문

> Airflow DAG가 직접 `DeleteObject`를 호출하는 주체라는 걸 놓쳐 권한이 빠져 있었음.
> `theone-airflow-role`에 `s3:DeleteObject`를 추가해 해결.
>
> 과거 장애 기록 — #912 이후 staging copy/delete 승격은 제거되고, writer가
> 최종 경로의 기존 `_SUCCESS`만 무효화한 뒤 직접 적재하도록 바뀜.

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

## 해결

`theone-airflow-role`에 `s3:DeleteObject`를 Silver 버킷 prefix 범위로 추가.

```json
{
  "Effect": "Allow",
  "Action": "s3:DeleteObject",
  "Resource": "arn:aws:s3:::de-theone/silver/*"
}
```
