# 모니터링 스택 (Prometheus · Grafana)

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
| `stack/grafana/provisioning/alerting/` | Grafana Slack 알림 (수신처 · 경로 · 규칙) |

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

### 워커 사용량은 Metrics Insights 로 봅니다

`Worker*Used`(실제 사용량)는 **앱 수준에 아예 없고 `JobId` 별로만** 발행됩니다. 그렇다고
포기할 필요는 없습니다 — Metrics Insights 는 **서버에서 집계**해 `GROUP BY` 결과만
돌려주므로, 스캔한 지표가 위젯 개수에 잡히지 않습니다.

```sql
SELECT SUM(WorkerMemoryUsed) FROM "AWS/EMRServerless"
WHERE ApplicationId = '...' GROUP BY WorkerType
```

작업이 2000개든 2만개든 위젯에는 `Spark_Driver`·`Spark_Executor` 두 줄만 돌아옵니다.

**위젯 하나에 Metrics Insights 쿼리는 1개까지입니다.** `GetMetricData` 가 호출당 1개만
받고, 위젯 하나가 한 번의 호출로 그려지기 때문입니다. 그래서 `used / allocated` 비율을
한 위젯에 담을 수 없어 used·allocated 를 나란한 두 위젯으로 둡니다. 두 개를 넣으면
배포는 통과하고 위젯만 깨지므로 테스트가 막습니다.

### 사용량 위젯을 읽는 법

**언제 얼마나 썼는지**를 절대량으로 봅니다. 붉은 선이 애플리케이션 용량 상한입니다.

```
Spark_Executor  ─────  executor 워커가 실제로 쓴 양
Spark_Driver    ─────  driver 워커가 실제로 쓴 양
용량 상한       ─────  maximumCapacity (48 GB / 12 vCPU / 2000 GB disk)
```

- **상한선에 붙으면** 자원이 모자란 것 — `maximumCapacity` 를 올려야 합니다
- **상한선과 멀면** 여유가 있는 것
- **driver·executor 를 나눠 그립니다.** 자원의 대부분은 executor 가 씁니다
  (실측 23.85 GB vs 4.44 GB). 합치면 어느 쪽을 조정할지 안 보입니다
- **합계선도 함께 그립니다.** 상한선은 둘을 합친 전체 한도라 개별 선만 보면
  남은 여유를 알 수 없습니다. executor 23.80 만 보고 24GB 로 읽으면 driver 2.07 이
  빠집니다 (실제 25.87 / 32)

합계는 `SUM()` 을 GROUP BY 결과에 걸어 만듭니다. Metrics Insights 쿼리를 더 쓰지
않으므로 위젯당 1개 제한을 지킵니다.

### 색

**순서가 곧 색입니다.** 색을 지정하지 않은 계열은 위젯 안 *순번*으로 기본 팔레트를
받습니다. 다른 계열이 같은 색을 지정해 뒀는지는 **보지 않습니다.**

```
순번  1        2        3        4
색    #1f77b4  #ff7f0e  #2ca02c  #d62728
      파랑     주황     초록     빨강
```

`GROUP BY` 결과에는 색을 지정할 수 없으므로 **맨 앞에 둬야** 파랑·주황을 가져갑니다.

```
metrics 순서   u (GROUP BY, 2계열) -> tot -> cap
실제 색        파랑, 주황            초록   빨강
```

합계·상한을 범례 앞으로 옮겼다가 워커 선이 3·4번으로 밀려 초록·빨강을 받았고,
합계·상한에 지정한 색과 똑같아져 네 선이 두 쌍으로 겹쳐 보였습니다.

| 선 | 색 | 지정 방식 |
|---|---|---|
| Spark_Executor | 파랑 `#1f77b4` | 자동 (순번 1) |
| Spark_Driver | 주황 `#ff7f0e` | 자동 (순번 2) |
| 합계 | 초록 `#2ca02c` | 지정 |
| 용량 상한 | 빨강 `#d62728` | 지정 |

테스트가 순번을 흉내내 실제 색을 계산하고, 한 위젯에 같은 색이 둘 이상이면 실패합니다.

상한선은 값을 박지 않고 `MaxMemoryAllowed`/`MaxCPUAllowed` 지표로 그립니다.
`maximumCapacity` 를 바꾸면 선이 따라옵니다.

### 앱 수준 `MemoryAllocated`/`CPUAllocated` 는 쓰지 않습니다

"할당량" 처럼 보이지만 **워커가 도는 중에도 0 으로 끊깁니다.** 작업별 사용량과 나란히
두면 "사용량 > 할당량" 이 나타나 보는 사람이 혼란스러워집니다.

```
활성 표본 330개 중  메모리 68건(21%), CPU 94건(28%) 에서 사용량 > 할당량
교차 시점의 할당량 값: 전부 0.0
```

상한값(`Max*Allowed`)은 상수라 이런 일이 없습니다.

### period 는 60 이어야 합니다

Metrics Insights 의 `SUM` 은 **기간 안의 모든 샘플을 더합니다.** 지표가 1분 간격이라
`period: 300` 을 쓰면 샘플 5개가 합쳐져 값이 5배가 됩니다.

```
period=60    executor 메모리  23.8 GB   ← 맞음 (앱 할당 28.0 GB 와 정합)
period=300   executor 메모리 118.9 GB   ← 5배로 부풂
```

그래프는 멀쩡해 보이고 숫자만 틀리는 실패라 테스트가 고정합니다.

| 보고 싶은 것 | 방법 |
|---|---|
| 워커 사용률·할당률 | Metrics Insights `WorkerMemoryUsed`/`WorkerCpuUsed` ÷ 상한 |
| 워커가 몇 개 떠 있나 | 앱 수준 — `RunningWorkerCount`, `PendingCreationWorkerCount` |
| 작업 상태 | 앱 수준 — `RunningJobs`, `PendingJobs`, `FailedJobs`, `SuccessJobs` |
| 작업 하나의 상세 | EMR Serverless 콘솔의 job run 상세, Spark UI |

테스트가 스키마 고정, 지표명, Metrics Insights 경유 여부, 위젯당 쿼리 수, period 를
모두 잡습니다.

## Lambda 알림 (CloudWatch 데이터소스)

Lambda 실패를 **Grafana 알림 경로로** 받습니다. 호스트 알림과 같은 Slack 수신처를 쓰므로
SNS→Slack 다리나 함수별 CloudWatch 알람을 따로 만들지 않습니다.

| 규칙 | 지표 | 심각도 | 왜 |
|---|---|---|---|
| Lambda 실패 | `AWS/Lambda Errors` | critical | Airflow 밖에서 실패하면 아무도 모릅니다 |
| Lambda 동시성 제한 | `AWS/Lambda Throttles` | warning | Airflow 에는 그냥 "태스크 실패" 로만 보입니다 |

### 규칙이 세 단계입니다 — A → B(reduce) → C(threshold)

Grafana 알림은 **축약된 값**만 임계와 비교할 수 있습니다. Prometheus 규칙은
`instant: true` 라 값이 계열당 하나씩이지만, **CloudWatch 는 항상 시계열**입니다.

그대로 비교하면 규칙은 등록되고 평가만 실패합니다.

```
invalid format of evaluation results ... only reduced data can be alerted on
```

UI 에는 `Health: error` 로 보이고 **알림은 영원히 안 옵니다.** 상태가 `Normal` 이라
얼핏 정상으로 보이는 것이 더 나쁩니다.

reducer 는 `sum` 입니다 — 15분 창에서 실패가 한 번이라도 있었으면 울려야 합니다.
`settings.mode` 는 `dropNN` 이어야 합니다. CloudWatch 는 호출이 없던 구간을 null 로
돌려주는데, 그대로 더하면 합이 NaN 이 되어 NoData 로 빠집니다.

### 호스트 규칙과 `noDataState` 가 반대입니다

```
호스트   noDataState: Alerting   지표가 끊기면 그 자체가 문제
Lambda   noDataState: OK         실패 지표가 없음 = 실패가 없었음
```

`Alerting` 으로 두면 그날 안 도는 함수들이 매분 울려 알림이 무시당하게 됩니다.

### 화면은 `theone / Lambda` 대시보드

```
Grafana → Dashboards → theone → Lambda
```

| 패널 | 지표 | 읽는 법 |
|---|---|---|
| 실패 | `Errors` | 0 이 아니면 그 함수가 실패 |
| 동시성 제한 | `Throttles` | Airflow 에는 그냥 "태스크 실패" 로만 보임 |
| 호출 | `Invocations` | 월간 파이프라인이라 평소 0, 실행일에만 솟음 |
| 실행 시간 | `Duration` | 함수 timeout 에 가까워지면 늘려야 함 |

### 함수 이름을 박지 않습니다

`FunctionName: "*"` 라 새 함수를 배포해도 규칙을 안 고쳐도 되고, 고치는 걸 잊어
감시에서 빠지는 일이 없습니다.

**`matchExact` 는 `true` 여야 합니다.** `false` 면 `FunctionName` 을 포함한 *모든* 차원
조합이 잡혀서, AWS 가 함께 발행하는 `Resource` 차원 변형까지 딸려옵니다.

```
matchExact=false   시리즈 28개   함수마다 두 개씩 (같은 실패로 알림이 두 번)
matchExact=true    시리즈 14개   함수당 하나
```

### 자격증명을 두지 않습니다

`authType: default` 는 AWS SDK 기본 체인이라 **EC2 인스턴스 프로파일(IMDS)** 에서 받습니다.
파일에도 Secret 에도 키가 남지 않고 만료 갱신도 SDK 가 합니다.

**인스턴스 role 에 CloudWatch 읽기 권한이 필요합니다.** 없으면 데이터소스는 등록되고
패널만 비어 조용히 실패합니다 — 실제로 확인한 오류는 이렇습니다.

```
AccessDenied: User: .../theone-monitoring-role/i-... is not authorized to perform:
cloudwatch:ListMetrics because no identity-based policy allows the action
```

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


## Grafana Slack 알림

호스트 지표가 임계를 넘으면 Airflow 와 **같은 채널**로 알립니다. 웹훅도 같은 Secret
(`SLACK_WEBHOOK_URL`) 이라 로테이션할 때 고칠 곳이 하나입니다.

### 임계값 — 실측 평시값 기준

| 규칙 | 임계 | 지속 | 심각도 | 평시 실측 |
|---|---|---|---|---|
| 노드 응답 없음 | `up == 0` | 2분 | critical | 4대 모두 1 |
| 디스크 | 80% | 10분 | warning | 14~40% |
| 디스크 | 90% | 5분 | critical | 〃 |
| 메모리 | 90% | 10분 | warning | 41~59% |
| CPU | 90% | 15분 | warning | 0.3~39% (airflow 39%) |

CPU 만 15분으로 길게 잡았습니다 — Airflow 는 잡이 돌면 정상적으로 튑니다.

swap 규칙은 두지 않습니다. 인스턴스에 swap 이 없어서 실측이 `NaN` 이고, 규칙을 두면
영원히 NoData 로 울립니다.

### 웹훅은 파일에 없습니다

```
GitHub Secret SLACK_WEBHOOK_URL
  → 배포가 ~/monitoring/.env 에 기록
  → compose 가 컨테이너 환경변수로 전달
  → 프로비저닝이 $__env{SLACK_WEBHOOK_URL} 로 읽음
```

`$__env{}` 가 아니면 Grafana 는 그 문자열을 **그대로 저장**하고 전송이 URL 파싱에서
실패합니다. 알림이 조용히 안 가는 형태라 로그를 봐야만 압니다. 테스트가 고정합니다.

### `noDataState` 는 전부 `Alerting`

지표가 안 오는 것 자체가 문제입니다. `OK` 로 두면 node_exporter 가 죽었을 때 조용해져,
**감시가 멈춘 것을 감시가 알려주지 않는** 상태가 됩니다.

### 데이터소스 uid 를 고정한 이유

규칙이 `uid` 로 데이터소스를 지목합니다. 비워 두면 Grafana 가 난수를 만들고, 컨테이너를
다시 만들 때 값이 바뀌어 **규칙은 남고 평가만 실패**합니다. `theone-prometheus` 로
고정했습니다.

### 남아 있는 기본 수신처

Grafana 가 만드는 `grafana-default-email` 은 SMTP 미설정으로 실패합니다. 알림 경로가
`slack` 을 가리키므로 실제로 쓰이지 않지만, 로그에 SMTP 오류가 보이면 이것입니다.


## EMR Serverless — CloudWatch 대시보드에서 이관

예전에는 CloudWatch 대시보드(`dashboard.json`)와 EventBridge → SNS → 이메일이었습니다.
둘 다 걷어내고 Grafana 로 모았습니다.

**옮긴 이유는 알림입니다.** CloudWatch 대시보드에는 알림이 없어서, CPU 가 상한에 붙어
있어도 사람이 열어보기 전까지 아무도 몰랐습니다.

```
Grafana → Dashboards → theone → EMR Serverless
```

| 패널 | 내용 |
|---|---|
| 작업 상태 | Running · Pending · Failed · Success |
| 메모리 사용 (GB) | 워커 실사용량 + 용량 상한선 |
| CPU 사용 (vCPU) | 워커 실사용량 + 용량 상한선 |
| 워커 수 | Running · 생성 대기 |

알림 2건:

| 규칙 | 지표 | 대체한 것 |
|---|---|---|
| EMR 작업 실패 | `FailedJobs` | EventBridge → SNS → 이메일 |
| EMR 용량 상한 도달 | `CPUAllocated >= MaxCPUAllowed`, 15분 | **새로 생긴 것** |

### 데이터소스가 UI 에서 부르는 것도 권한이 필요합니다

패널을 클릭해 편집기가 열리면 데이터소스가 리소스 API 를 부릅니다. 하나라도 403 이면
**"Something went wrong. Please check the console log."** 로 화면이 죽습니다.

| 엔드포인트 | 필요한 권한 | 없으면 |
|---|---|---|
| `metrics`, `dimension-keys` | `cloudwatch:ListMetrics` | 질의 자체가 안 됨 |
| `accounts` | `oam:ListSinks` | **편집기가 죽음** |
| `regions` | `ec2:DescribeRegions` | 내장 목록으로 대체 (로그만 남음) |

`oam:*` 은 교차 계정 관측(Observability Access Manager) 기능을 쓰는지 확인하는 호출입니다.
우리는 안 쓰지만 Grafana 가 항상 물어봅니다.

### 질의할 때 두 가지

**`ApplicationName` 을 함께 줘야 합니다.** `ApplicationId` 만 주면 `matchExact` 가 맞지
않아 **0계열**이 됩니다 — 실제 차원 조합이 `{ApplicationId, ApplicationName}` 이라서요.
패널은 에러 없이 비어 있어 눈으로만 봐서는 원인을 알 수 없습니다.

**한 패널에 Metrics Insights 는 하나뿐입니다.** `GetMetricData` 가 호출당 하나만 받고
Grafana 도 같은 제한입니다. 두 개를 넣으면 배포는 통과하고 패널만 깨집니다.

```
Maximum number of queries (1) exceeded
```

그래서 워커 실사용량(MI)과 용량 상한(일반 질의)을 한 패널에 두고, 사용량과 할당량을
동시에 그리지는 못합니다.
