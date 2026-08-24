# EMR Serverless 모니터링

`theone-infrastructure-prod` CloudWatch 대시보드에서 다음을 확인합니다.

- EMR Serverless 작업 실행 상태 (Running / Pending / Failed / Success)
- Driver·Executor 의 CPU·메모리 사용률
- 작업이 `FAILED` 또는 `CANCELLED` 되면 SNS 알림

`AWS/EMRServerless` 는 EMR 이 직접 발행하는 지표라 에이전트도 추가 권한도 필요 없습니다.

## EC2 호스트 자원은 여기 없습니다

CPU·메모리·디스크는 **Prometheus + Grafana** 가 담당합니다. CloudWatch Agent 로
메모리·디스크를 보내려면 인스턴스 role 에 `cloudwatch:PutMetricData` 가 필요한데 이
계정에서는 IAM 변경이 불가합니다. `node_exporter` 는 호스트 안에서 자기 자신을 읽어
HTTP 로 노출하므로 AWS 권한이 개입하지 않습니다.

## 자동 배포

`develop` 에 `monitoring/**` 변경이 합쳐지거나 `Deploy monitoring` 을 수동 실행하면
`.github/workflows/deploy-monitoring.yml` 이 CLI 3개를 돌립니다.

| 대상 | 명령 | 멱등성 |
|---|---|---|
| 대시보드 | `cloudwatch put-dashboard` | 같은 이름이면 내용 교체 |
| 알림 Topic | `sns create-topic` + `set-topic-attributes` | 같은 이름이면 기존 ARN 반환 |
| 실패 rule | `events put-rule` + `put-targets` | 같은 이름·Id 면 덮어씀 |

세 명령 모두 멱등이라 몇 번 돌려도 같은 결과가 됩니다.

### CloudFormation 을 쓰지 않는 이유

리소스가 3개뿐인데 스택 상태 관리가 따라오고, 실제로 세 번 연속 배포를 막았습니다
(#886, #890). 자세한 경위는
[`docs/troubleshooting/aws/CFN_INSTANCE_ID_PARAM_TYPE.md`](../docs/troubleshooting/aws/CFN_INSTANCE_ID_PARAM_TYPE.md).

- 파라미터 타입 검증이 배포 자격증명으로 `ec2:DescribeInstances` 를 호출 → 거부
- 실패한 CREATE 가 `ROLLBACK_COMPLETE` 로 남고 `DeleteStack` 권한이 없어 좌초
- `AWS::CloudWatch::Dashboard` 핸들러가 기존 대시보드에 `AlreadyExists` 로 거부
  (`put-dashboard` API 자체는 upsert 라 이 문제가 없습니다)

좌초된 스택 `theone-monitoring`, `theone-monitoring-prod` 는 리소스가 0개라 방치해도
과금이 없습니다. `DeleteStack` 권한이 생기면 지우면 됩니다.

## 파일

| 파일 | 내용 |
|---|---|
| `dashboard.json` | 대시보드 본문. `${AWS_REGION}`, `${EMR_APPLICATION_ID}` 를 `envsubst` 로 치환 |
| `alert-topic-policy.json` | EventBridge 가 Topic 에 발행하도록 허용하는 정책 |
| `emr-failure-event-pattern.json` | `FAILED`/`CANCELLED` 만 걸러내는 이벤트 패턴 |

`WorkerType` 값은 **`Spark_Executor` / `Spark_Driver`** 입니다. `SPARK_EXECUTORS` 처럼
대문자·복수형으로 쓰면 `SEARCH()` 가 아무것도 매칭하지 못하고 **에러 없이 빈 위젯**이
그려집니다. 실측으로 확인한 값이고 테스트가 고정합니다.

## 최초 1회 준비

Repository Variable `AWS_ROLE_ARN_MONITORING` 에 OIDC 배포 역할 ARN 이 필요합니다.
이 역할에 필요한 권한은 다음뿐입니다 (IAM·CloudFormation 권한 불필요).

- `cloudwatch:PutDashboard` (대시보드 1개 ARN 범위)
- `sns:CreateTopic`, `sns:SetTopicAttributes`, `sns:GetTopicAttributes`
- `events:PutRule`, `events:PutTargets`

`AWS_REGION`, `EMR_APPLICATION_ID` Variable 도 사용합니다.

알림을 받으려면 이메일 구독을 1회 추가하고 수신 메일에서 승인합니다. 배포 요약이
구독 수가 0이면 이 명령을 안내합니다.

```bash
aws sns subscribe --topic-arn <AlertTopicArn> \
  --protocol email --notification-endpoint <메일주소>
```

Slack 은 같은 Topic 을 AWS Chatbot 에 연결하면 됩니다.
