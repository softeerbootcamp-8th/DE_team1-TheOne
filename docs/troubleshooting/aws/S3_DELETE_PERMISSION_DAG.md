# 삭제 권한을 서비스 단위로만 챙겨 파이프라인 완료 단계에서 권한이 누락된 문제

- 요약
  - 정제 데이터를 최종 경로로 옮기는 마지막 단계가 권한 오류로 실패
  - 저장소 권한을 처리 프로그램 단위로만 챙기다 보니, 워크플로 자체가 보내는 삭제 요청을 놓침
  - 워크플로가 사용하는 권한 역할에 삭제 권한을 추가해 해결

## 문제

정제 데이터의 품질 검증을 통과한 뒤, 임시로 저장해둔 파일을 최종 경로로 옮기는 마지막 단계에서 파이프라인이 실패했다.

```
AccessDenied: An error occurred (AccessDenied) when calling the DeleteObject
operation: User: arn:aws:sts::572660899671:assumed-role/theone-airflow-role/...
is not authorized to perform: s3:DeleteObject on resource:
"arn:aws:s3:::de-theone/silver/monthly_taxi_trip/year_month=2026-01/....staged.parquet"
because no identity-based policy allows the s3:DeleteObject action
```

품질 검증(15개 항목, 경고 0건)까지는 모두 통과한 뒤였다.

## 접근

저장소(S3) 접근 권한을 챙길 때, "이 파이프라인에서 저장소를 만지는 주체가 몇 개인가"를 각 처리 프로그램(수집 함수, 집계 작업 등) 단위로만 파악하고 있었다. 그래서 각 프로그램이 필요로 하는 읽기·쓰기 권한은 다 챙겼는데, 정작 이번에 삭제 요청을 보낸 주체는 개별 처리 프로그램이 아니라 전체 흐름을 조율하는 워크플로 코드 자체였다.

## 해결

워크플로가 쓰는 권한 역할에 정제 데이터 저장 경로 범위로 삭제 권한을 추가했다.

```json
{
  "Effect": "Allow",
  "Action": "s3:DeleteObject",
  "Resource": "arn:aws:s3:::de-theone/silver/*"
}
```

## 참고

- 과거 장애 이력(#912) 이후, 임시 경로에 복사했다가 지우는 승격 방식은 없어지고 최종 경로의 기존 완료 표시만 무효화한 뒤 바로 적재하는 방식으로 바뀌었다. 이번 삭제 요청도 그 방식에서 나온 것이다.
