# 인스턴스 존재 확인 권한이 없어 배포가 원인 불명으로 실패한 문제

- 요약
  - 모니터링 서버를 자동으로 세팅하는 배포 스크립트가 원인 표시 없이 계속 실패
  - 배포 설정 항목 하나가 배포 계정에는 없는 조회 권한을 몰래 요구하고 있었음
  - 조회 없이 형식만 검사하도록 바꿔 해결

## 문제

모니터링 서버 3대를 자동으로 세팅해주는 배포 스크립트(CloudFormation)가 실행할 때마다 실패했다. 로그에는 원인 없이 종료 코드만 찍혔다.

```
Waiting for changeset to be created..
Waiting for stack create/update to complete

aws: [ERROR]: Failed to create/update the stack. Run the following command
to fetch the list of events leading up to the failure
aws cloudformation describe-stack-events --stack-name theone-monitoring
Error: Process completed with exit code 255.
```

배포 계획(changeset)까지는 만들어졌는데 그 다음 단계에서 죽었고, 로그만으로는 원인을 알 수 없었다.

## 접근

AWS 활동 기록(CloudTrail)을 뒤져 그 시점에 실제로 무엇이 거부됐는지 확인했다.

```
2026-08-23T13:22:21Z  cloudformation  ExecuteChangeSet    | (성공)
2026-08-23T13:22:22Z  ec2             DescribeInstances   | Client.UnauthorizedOperation  ×3
```

배포 설정에는 EC2 인스턴스 ID 값을 넣는 항목이 있었는데, 이 항목의 타입이 "값으로 넣은 인스턴스가 실제로 존재하는지 AWS가 자동으로 확인하는" 특수 타입이었다. 이 확인 과정에서 배포 계정이 EC2 정보를 조회할 권한(`ec2:DescribeInstances`)을 갖고 있지 않아 거부됐다.

```
User: arn:aws:sts::572660899671:assumed-role/theone-github-actions-monitoring-deploy/GitHubActions
is not authorized to perform: ec2:DescribeInstances
```

인스턴스 3대는 실제로는 모두 정상 실행 중이었지만, 존재를 확인할 권한이 없으니 AWS 입장에서는 "존재하지 않는 값"으로 처리해 다음 에러를 냈다.

> `Parameter validation failed: parameter value i-0e91f8352af3af3b6 for parameter name AirflowInstanceId does not exist.`

이 확인 과정이 실제 리소스를 만들기 전에 일어나기 때문에, 리소스는 하나도 만들어지지 않고 스택은 실패 상태로만 남았다.

## 해결

조회 권한을 새로 열어주는 대신, 인스턴스 ID 항목을 "AWS가 존재를 확인하는 특수 타입" 대신 형식만 검사하는 일반 문자열 타입으로 바꿨다. 이 조회 API는 특정 인스턴스로 권한을 좁혀줄 수 없는 구조라, 권한을 열면 계정의 모든 EC2 인스턴스 정보를 읽을 수 있게 된다. 배포 하나를 위해 그렇게까지 권한을 넓히는 대신, 값이 `i-`로 시작하는 형식인지만 검사하도록 바꿨다.

```yaml
  AirflowInstanceId:
    Type: String
    AllowedPattern: '^i-[0-9a-f]{8,17}$'
    ConstraintDescription: i- 로 시작하는 EC2 인스턴스 ID 여야 합니다
```

## 검증

수정한 설정으로 스택이 정상적으로 만들어지는지 확인하는 테스트를 추가했다. 실패했던 이전 스택은 실패 상태(`ROLLBACK_COMPLETE`)로 남아 같은 이름으로는 재배포가 막혔는데, 그 스택을 지울 권한도 없어서 새 이름으로 다시 만들어 배포가 통과하는 것을 확인했다.

## 한계

실패했던 이전 스택은 아직 삭제하지 못한 채 남아 있다. 스택 삭제 권한이 생기면 정리할 예정이다.

```bash
aws cloudformation delete-stack --stack-name theone-monitoring  # 권한 생기면 정리
```

## 참고

- 회귀 테스트: `monitoring/tests/test_monitoring.py::test_stack_connects_three_ec2_instances_and_emr_metrics`
