# 인프라 모니터링

`theone-infrastructure-prod` 대시보드 한 곳에서 다음을 확인합니다.

- Airflow, Source API, Dashboard EC2의 CPU·메모리·루트 디스크 사용률
- EMR Serverless의 실행 상태와 Driver/Executor CPU·메모리 사용률
- EMR Serverless 작업의 `FAILED`, `CANCELLED` 이벤트

EC2 메모리와 디스크는 CloudWatch Agent가 `TheOne/EC2` 네임스페이스로 전송합니다.
EMR Serverless에는 Agent를 설치하지 않고 `AWS/EMRServerless` 기본 지표를 사용합니다.

## 자동 배포

`develop`에 `monitoring/**` 변경이 합쳐지거나 `Deploy monitoring` 워크플로를 수동
실행하면 다음 작업이 순서대로 진행됩니다.

1. CloudFormation으로 대시보드, SNS Topic, EventBridge rule, EC2 역할 권한을 반영
2. Agent 설정을 Parameter Store에 저장
3. SSM으로 EC2 3대에 Agent를 설치하고 설정만 재시작

워크플로가 끝나면 실행 요약의 `CloudWatch 대시보드` 링크만 열면 됩니다. 매번 위젯을
직접 추가할 필요가 없습니다.

## 최초 1회 준비

GitHub Actions OIDC 배포 역할을 만든 뒤 Repository Variable
`AWS_ROLE_ARN_MONITORING`에 ARN을 저장해야 합니다. 이 역할에는 아래 작업 권한이
필요합니다.

- `cloudformation` stack 생성·조회·갱신
- `cloudwatch` dashboard 생성·삭제
- `events` rule/target 관리와 `sns` topic/policy 관리
- `iam` inline role policy 관리
- `ssm` parameter 및 Run Command 관리

기존 Repository Variables인 `AWS_REGION`, `EMR_APPLICATION_ID`,
`AIRFLOW_INSTANCE_ID`, `SERVER_INSTANCE_ID`, `DASHBOARD_INSTANCE_ID`도 사용합니다.

이메일 알림은 CloudFormation 출력의 `AlertTopicArn`에 이메일 구독을 한 번 추가하고
수신 메일에서 승인합니다. Slack 알림은 같은 Topic을 AWS Chatbot에 연결하면 됩니다.

Agent는 메모리와 `/` 디스크만 60초 간격으로 수집하고 InstanceId 단위 시계열만
남깁니다. CPU는 추가 과금 없는 EC2 기본 지표를 사용합니다.
