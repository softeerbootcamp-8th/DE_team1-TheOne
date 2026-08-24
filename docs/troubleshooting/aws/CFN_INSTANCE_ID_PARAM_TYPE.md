# CloudFormation 배포가 exit 255 로만 죽음 — 범인은 파라미터 타입이었음

> 인스턴스 ID 파라미터 타입이 `AWS::EC2::Instance::Id`라 배포 role에 없는
> `ec2:DescribeInstances` 호출이 거부됨. `Type: String` + `AllowedPattern`으로
> 교체해 해결.

## 증상

`deploy-monitoring.yml` 의 "CloudFormation 배포" 단계가 이유 없이 실패.

```
Waiting for changeset to be created..
Waiting for stack create/update to complete

aws: [ERROR]: Failed to create/update the stack. Run the following command
to fetch the list of events leading up to the failure
aws cloudformation describe-stack-events --stack-name theone-monitoring
Error: Process completed with exit code 255.
```

changeset 은 만들어졌고 그 다음 단계에서 죽는다. `aws cloudformation deploy` 는
이유를 안 찍고 exit 255 만 낸다.

## 원인

템플릿의 인스턴스 ID 파라미터 타입이 `AWS::EC2::Instance::Id` 였다. 이 AWS 고유
타입은 CloudFormation 이 **배포 자격증명으로** `ec2:DescribeInstances` 를 호출해
인스턴스 존재를 검증하는데, `theone-github-actions-monitoring-deploy` 에는
`ec2:*` 가 없어 거부됐다.

CloudTrail 로 확인한 실제 거부(`userAgent` 가 `cloudformation.amazonaws.com`):

```
2026-08-23T13:22:21Z  cloudformation  ExecuteChangeSet    | (성공)
2026-08-23T13:22:22Z  ec2             DescribeInstances   | Client.UnauthorizedOperation  ×3
```

```
User: arn:aws:sts::572660899671:assumed-role/theone-github-actions-monitoring-deploy/GitHubActions
is not authorized to perform: ec2:DescribeInstances
```

인스턴스 3대는 전부 `running` 상태였지만, 존재를 **확인할 권한이 없으면** 없는
것으로 처리되어 다음 에러가 났다.

> `Parameter validation failed: parameter value i-0e91f8352af3af3b6 for parameter
> name AirflowInstanceId does not exist.`

검증이 리소스 생성보다 앞이라 리소스는 하나도 안 만들어졌고, 스택은
`ROLLBACK_COMPLETE` 로 남았다.

## 해결

`Type: String` + `AllowedPattern` 으로 바꿨다. `ec2:DescribeInstances` 는
resource-level 권한을 지원하지 않아 특정 인스턴스로 좁힐 수 없어, 계정 전체
인스턴스 읽기를 여는 대신 형식 검사로 대체했다.

```yaml
  AirflowInstanceId:
    Type: String
    AllowedPattern: '^i-[0-9a-f]{8,17}$'
    ConstraintDescription: i- 로 시작하는 EC2 인스턴스 ID 여야 합니다
```

회귀 테스트:
`monitoring/tests/test_monitoring.py::test_stack_connects_three_ec2_instances_and_emr_metrics`.

실패한 스택은 `ROLLBACK_COMPLETE` 로 남아 재배포를 막는데 `DeleteStack` 권한도
없어서, `STACK_NAME` 을 `theone-monitoring-prod` 로 바꿔 새로 CREATE 했다.

```bash
aws cloudformation delete-stack --stack-name theone-monitoring  # 권한 생기면 정리
```
