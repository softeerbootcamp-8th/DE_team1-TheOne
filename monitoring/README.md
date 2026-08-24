# EMR Serverless 모니터링

`theone-infrastructure-prod` CloudWatch 대시보드에서 다음을 확인합니다.

- EMR Serverless 작업 실행 상태 (Running / Pending / Failed / Success)
- Driver·Executor 의 CPU·메모리 사용률
- 작업이 `FAILED` 또는 `CANCELLED` 되면 SNS 알림

`AWS/EMRServerless` 는 EMR 이 직접 발행하는 지표라 에이전트도 추가 권한도 필요 없습니다.

## EC2 호스트 자원은 Prometheus + Grafana 가 봅니다

CPU·메모리·디스크는 `theone-monitoring` 인스턴스의 Prometheus 가 각 호스트의
`node_exporter`(:9100)를 긁어 Grafana 로 보여줍니다. 설정은 [`stack/`](stack/) 에 있고
`Deploy monitoring` 의 `stack` job 이 SSM 으로 배포합니다.

| 항목 | 값 |
|---|---|
| 인스턴스 | `theone-monitoring` / `i-0233a9b38d817105c` / `10.0.10.240` / t4g.small AL2023 arm64 |
| 감시 대상 | airflow(10.0.10.28), source-server(10.0.10.81), dashboard-server(10.0.10.8), gateway(10.0.0.113) |
| 보존 | 15일 |
| 접속 | `ssh -N -L 3000:localhost:3000 monitoring` → http://localhost:3000 |

`node_exporter` 설치 방식이 호스트마다 다릅니다 — Docker 가 있는 3대는 컨테이너로,
`theone-gateway` 는 Docker 가 없어 바이너리 + systemd 유닛으로 띄웁니다. 그 인스턴스는
NAT 라우팅을 담당하므로 `net.ipv4.ip_forward` 를 건드리지 않는 범위에서만 설치했습니다.

### 왜 CloudWatch Agent 가 아닌가

예전 주석은 "인스턴스 role 에 `cloudwatch:PutMetricData` 를 붙일 IAM 변경이 불가"
라고 적었지만 **그렇지 않습니다** — MFA 세션이면 `iam:CreateRole`·`PutRolePolicy` 가
통하고, 실제로 이 작업에서 역할을 만들었습니다(#898).

실제 이유는 **인스턴스 1대를 감수하고 얻는 것을 택한 것**입니다.

| | CloudWatch Agent | Prometheus + Grafana |
|---|---|---|
| 새 인스턴스 | 0개 | **1대 (상시 과금)** |
| 대시보드 자유도 | CloudWatch 위젯 | PromQL·기성 대시보드(1860 등) |
| 컨테이너 지표 확장 | 별도 작업 | cAdvisor 추가로 가능 |
| 지표 비용 | 커스텀 지표 단가 | 없음 |

`node_exporter` 는 호스트 안에서 자기 자신을 읽어 HTTP 로 노출하므로 AWS 권한이
개입하지 않습니다. 그 성질은 어느 쪽을 택하든 유효합니다.

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

### 위젯을 추가할 때 — `SEARCH()` 는 차원 조합을 고정해야 합니다

`SEARCH()` 의 차원 조건은 **부분 일치**입니다. `ApplicationId="..."` 만 걸면 그 차원을
포함한 *모든* 조합이 잡히는데, EMR Serverless 는 `Worker*` 지표를 **`JobId` 별로**
발행합니다. 작업이 쌓일수록 위젯의 시계열이 계속 늘고, 500개를 넘는 순간 CloudWatch 가
**"허용된 최대 지표 수를 초과함"** 을 띄우고 그리기를 멈춥니다. 그 직전에는
`StatusCode 'Paginated'` 경고와 함께 일부 데이터만 그려집니다.

실제로 memory·CPU 위젯이 각각 510개까지 늘어 깨졌습니다. 앞의 중괄호가 차원 조합
자체를 고정해 이걸 막습니다.

```
SEARCH('{AWS/EMRServerless,ApplicationId,ApplicationName} MetricName="MemoryAllocated" ...')
        └─ 이 조합인 지표만. JobId 차원 지표는 애초에 후보에 들어오지 않습니다
```

`Worker*Used` 계열은 **앱 수준에 아예 없습니다.** 작업 하나하나의 워커 사용률은
EMR Serverless 콘솔의 job run 상세에서 봅니다. 대시보드는 앱 수준 지표만 씁니다.

| 보고 싶은 것 | 앱 수준 지표 |
|---|---|
| 용량 상한에 붙었나 | `MemoryAllocated` / `MaxMemoryAllowed`, `CPUAllocated` / `MaxCPUAllowed` |
| 워커가 몇 개 떠 있나 | `RunningWorkerCount`, `PendingCreationWorkerCount` |
| 작업 상태 | `RunningJobs`, `PendingJobs`, `FailedJobs`, `SuccessJobs` |

테스트가 스키마 고정과 지표명을 모두 잡습니다.

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
