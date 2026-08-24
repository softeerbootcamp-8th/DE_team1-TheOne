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

템플릿의 인스턴스 ID 파라미터가 `Type: AWS::EC2::Instance::Id` 였다.

이 AWS 고유 타입을 쓰면 CloudFormation 이 **배포 자격증명으로**
`ec2:DescribeInstances` 를 호출해 인스턴스 존재를 검증한다.
`theone-github-actions-monitoring-deploy` 에는 `ec2:*` 가 한 줄도 없어서 거부됐다.

CloudTrail 에서 확인한 실제 거부(`userAgent` 가 내 CLI 가 아니라
`cloudformation.amazonaws.com` 인 게 핵심 단서):

```
2026-08-23T13:22:21Z  cloudformation  ExecuteChangeSet    | (성공)
2026-08-23T13:22:22Z  ec2             DescribeInstances   | Client.UnauthorizedOperation  ×3
```

```
User: arn:aws:sts::572660899671:assumed-role/theone-github-actions-monitoring-deploy/GitHubActions
is not authorized to perform: ec2:DescribeInstances
```

인스턴스 3대는 전부 실재하고 `running` 이었다. 그런데 스택 이벤트에는
존재하지 않는다고 찍힌다 — 존재를 **확인할 권한이 없으면** 없는 것으로 처리한다.

> `Parameter validation failed: parameter value i-0e91f8352af3af3b6 for parameter
> name AirflowInstanceId does not exist.`

검증이 리소스 생성보다 앞이라 리소스는 하나도 안 만들어졌고
(`list-stack-resources` 가 빈 배열), 스택은 `ROLLBACK_COMPLETE` 로 남았다.

## 잘못 짚었던 곳들

1. **`AgentMetricPolicy` 의 role 이름** — `deploy-server.yml` 주석에
   `theone-source-api-server` 가 "인스턴스 프로파일" 로 적혀 있어서
   `AWS::IAM::Policy.Roles` 에 role 이 아닌 이름을 넣은 걸로 의심했다.
   실제로는 인스턴스 프로파일과 role 이 같은 이름으로 존재해서 문제없었다.

   ```bash
   aws iam get-instance-profile --instance-profile-name theone-source-api-server \
     --query 'InstanceProfile.Roles[].RoleName' --output text
   ```

2. **배포 role 의 `iam:PutRolePolicy` 누락** — 인라인 정책을 직접 떠보니
   3개 role ARN 범위로 이미 있었다.

   ```bash
   aws iam get-role-policy --role-name theone-github-actions-monitoring-deploy \
     --policy-name theone-monitoring-deploy-policy --query 'PolicyDocument'
   ```

3. **SNS topic / EventBridge rule 이름 충돌** — 둘 다 존재하지 않았다.

`describe-stack-events` 가 개인 IAM 유저에게는 막혀 있어서(`edu/` 경로 유저)
스택 이벤트를 못 읽었고, 대신 **CloudTrail 로 그 시각의 거부된 API 호출**을
훑어서 찾았다. 권한 문제를 쫓을 때 이게 스택 이벤트보다 확실하다.

```bash
aws cloudtrail lookup-events \
  --start-time 2026-08-23T13:20:00Z --end-time 2026-08-23T13:32:00Z \
  --query 'Events[].CloudTrailEvent' --output json > ct.json
# eventTime / eventName / errorCode / userAgent 를 시간순으로 훑는다
```

## 해결

`Type: String` + `AllowedPattern` 으로 바꿨다. `ec2:DescribeInstances` 는
resource-level 권한을 지원하지 않아 특정 인스턴스로 좁힐 수 없고, 그 하나를
위해 계정 전체 인스턴스 읽기를 열기보다 형식 검사로 충분하다고 판단했다.

```yaml
  AirflowInstanceId:
    Type: String
    AllowedPattern: '^i-[0-9a-f]{8,17}$'
    ConstraintDescription: i- 로 시작하는 EC2 인스턴스 ID 여야 합니다
```

되돌리는 것을 막는 회귀 테스트는
`monitoring/tests/test_monitoring.py::test_stack_connects_three_ec2_instances_and_emr_metrics`.

## 같이 고친 것

실패 이벤트 출력 스텝이 `contains(ResourceStatus, 'FAILED')` 로 필터하고 있었다.
이번처럼 파라미터 검증에서 죽으면 리소스 단위 `*_FAILED` 이벤트가 없고 스택이
`CREATE_IN_PROGRESS` → `ROLLBACK_IN_PROGRESS` 로 바로 가므로, 그 스텝이 있었어도
**빈 표만 나왔다.** 이유가 붙은 이벤트를 모두 뽑도록 바꿨다.

```
--query "StackEvents[?ResourceStatusReason!=null].[Timestamp,LogicalResourceId,ResourceStatus,ResourceStatusReason]"
```

## 막힌 스택은 지우지 못해서 이름을 바꿨다

CREATE 가 실패한 스택은 `ROLLBACK_COMPLETE` 로 남고, 이 상태에서는 다음 배포가
`Stack ... is in ROLLBACK_COMPLETE state and can not be updated` 로 죽는다.
그런데 지울 권한이 어디에도 없었다.

- 배포 role `theone-github-actions-monitoring-deploy` — 정책에
  `cloudformation:DeleteStack` 이 아예 없다
- 팀 개인 IAM 유저(`user/edu/...`) — 역시 거부된다. 콘솔도 같은 자격증명을 쓰므로
  콘솔에서 눌러도 안 된다

그래서 `STACK_NAME` 을 `theone-monitoring-prod` 로 바꿔 새로 CREATE 하게 했다.
배포 role 정책의 `cloudformation:*` 액션은 `Resource: "*"` 라서 IAM 변경 없이 된다.
옛 `theone-monitoring` 은 리소스 0개짜리 빈 껍데기로 남는다(과금 없음).
`DeleteStack` 권한을 확보하면 그때 지우면 된다.

```bash
aws cloudformation delete-stack --stack-name theone-monitoring
```
