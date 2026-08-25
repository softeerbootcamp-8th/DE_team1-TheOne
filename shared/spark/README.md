# shared/spark

`main/spark`·`sub/spark` 공용 Spark 런타임. `Dockerfile`이 EMR Serverless 실행 이미지를 빌드하고, `common/`이 Spark job들이 같이 쓰는 세션·입출력 헬퍼를 담습니다.

## EMR Serverless

Application 하나를 전체 프로젝트가 공용으로 씁니다 (Lambda 이미지 하나로 여러 함수를 서빙하는 것과 같은 구조) — job마다 새 Application을 만들지 않고, 다른 job은 그냥 다른 Job Run(다른 spark-submit 경로)으로 제출합니다.

| 리소스 | 값 | 어디서 찾나 |
|---|---|---|
| Application | 이름 `theone-spark`, release `emr-7.13.0`, x86_64, pre-initialized capacity 없음 | AWS 계정 ID·Application ID는 GitHub Variable `EMR_APPLICATION_ID` 또는 EMR Studio 콘솔 |
| 실행 역할 | `theone-spark-emr-exec` (S3 `de-theone` 버킷 전체 read/write/delete) | IAM 콘솔에서 이름으로 검색 |
| 이미지 | ECR 리포지토리 `theone-spark` | `deploy-spark.yml`이 push 후 이 Application에 자동 반영 |
| 로그 | `s3://de-theone/logs/emr-serverless/` | |

ECR 리포지토리(`theone-spark`)에는 `emr-serverless.amazonaws.com`이 커스텀 이미지를 pull할 수 있게 하는 리포지토리 정책이 걸려 있어야 합니다 (`ecr:BatchGetImage`·`ecr:GetDownloadUrlForLayer`·`ecr:DescribeImages`).

Application은 `applicationLevelDigestResolution=false`를 사용합니다. `deploy-spark.yml`이 실행 중인 Application의 이미지를 갱신해도 기존 Job은 제출 당시 digest로 계속 실행하고, 이후 제출되는 Job부터 새 digest를 사용합니다. 이 모드는 pre-initialized capacity와 함께 쓸 수 없으므로 warm worker를 두지 않으며, 중지된 Application은 Job 제출 시 자동으로 시작합니다.

### Job Run 제출 (수동 실행 예시)

Job Run은 일회성입니다 — 실행할 때마다 새로 제출하며, 중지된 Application은 자동으로 시작합니다. `job.py`가 `main.spark...`/`shared...` 절대경로로 import하는데 spark-submit은 스크립트 자기 디렉터리만 `sys.path`에 넣어서, `PYTHONPATH=/home/hadoop`을 명시적으로 conf로 넘겨야 합니다 (로컬의 `PYTHONPATH=../.. uv run`과 같은 이유).

Application ID·실행 역할 ARN은 계정 정보라 커밋하지 않습니다 — GitHub Variables(`EMR_APPLICATION_ID` 등) 또는 팀 채널에서 확인하세요.

```bash
aws emr-serverless start-job-run \
  --application-id <EMR_APPLICATION_ID> \
  --execution-role-arn <theone-spark-emr-exec 역할 ARN> \
  --name monthly-taxi-trip-bronze-to-silver \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "/home/hadoop/main/spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/job.py",
      "entryPointArguments": ["--env", "prod", "--bucket", "de-theone", "--start_year_month", "2026-01", "--end_year_month", "2026-01"],
      "sparkSubmitParameters": "--conf spark.driver.cores=2 --conf spark.driver.memory=6g --conf spark.executor.cores=2 --conf spark.executor.memory=6g --conf spark.emr-serverless.driverEnv.PYTHONPATH=/home/hadoop --conf spark.executorEnv.PYTHONPATH=/home/hadoop"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": { "logUri": "s3://de-theone/logs/emr-serverless/" }
    }
  }' \
  --region ap-northeast-2

# 상태 확인
aws emr-serverless get-job-run \
  --application-id <EMR_APPLICATION_ID> \
  --job-run-id <위 응답의 jobRunId> \
  --region ap-northeast-2 \
  --query "jobRun.state"
```

다른 job(`silver_to_gold` 등)을 EMR에 올릴 땐 `entryPoint`·`entryPointArguments`만 그 job에 맞게 바꾸면 됩니다 — Application·실행 역할은 그대로 재사용.
