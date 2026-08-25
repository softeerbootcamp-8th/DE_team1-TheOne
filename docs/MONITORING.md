# 모니터링 구축
- 요약
    - 파이프라인은 도는데 **자원이 언제 부족한지 아무도 몰랐음**
    - 감시 대상이 두 종류라 도구를 나눔: EC2 호스트 4대는 **Prometheus + Grafana**, EMR Serverless 는 **CloudWatch**
    - 임계값은 추측이 아니라 **실측 평시값**에서 잡음 (디스크 평시 14~40% → 80% 경고)
    - 대시보드에서 겪은 함정 4개를 실측으로 고침 (지표 한도 **510/500 초과**, 값 **5배 부풀림**, 선 색 충돌, 끊기는 게이지)
- 목차
    1. [문제](#문제)
    2. [감시 대상을 둘로 나눈 이유](#감시-대상을-둘로-나눈-이유)
    3. [EC2 호스트 — Prometheus + Grafana](#ec2-호스트--prometheus--grafana)
    4. [EMR Serverless — CloudWatch](#emr-serverless--cloudwatch)
    5. [겪은 함정](#겪은-함정)
    6. [알림](#알림)


## 문제

- 파이프라인 실패는 Slack 으로 왔지만, **자원 상태는 보이지 않음**
    - Airflow 가 배포마다 4.5GB 이미지를 받아 디스크(29GB)가 이틀이면 참
    - EMR 작업이 느려도 CPU 가 모자란 건지 I/O 대기인지 구분 불가
- 발견 경로가 "팀원이 Airflow 가 꺼졌다고 말함" 뿐이었음

## 감시 대상을 둘로 나눈 이유

| 대상 | 도구 | 이유 |
|---|---|---|
| EC2 호스트 4대 | Prometheus + Grafana | 인스턴스 role 에 `cloudwatch:PutMetricData` 가 없어 **CloudWatch Agent 가 지표를 못 보냄** |
| EMR Serverless | CloudWatch | AWS 가 이미 `AWS/EMRServerless` 로 발행 중. 수집기를 따로 붙일 이유가 없음 |

- 모니터링 전용 EC2 1대(`theone-monitoring`)를 감수하고 호스트·컨테이너 가시성을 택함
- 두 UI 모두 `127.0.0.1` 바인딩 + SSH 터널. Grafana 는 익명 접근이라 포트를 열면 곧바로 무인증 공개가 됨

## EC2 호스트 — Prometheus + Grafana

- Prometheus 가 15초마다 4대의 `node_exporter`(9100)를 긁음, 보존 15일
- 대상은 사설 IP 고정(`static_configs`). `ec2_sd_config` 는 인스턴스 role 에 `ec2:DescribeInstances` 가 필요해서 4대 고정인 지금은 static 이 단순

**실측 (평시)**

| 호스트 | 디스크 | 메모리 | CPU |
|---|---:|---:|---:|
| theone-airflow | 14.0% | 40.9% | 39.0% |
| theone-gateway | 23.1% | 45.7% | 0.3% |
| theone-dashboard-server | 34.0% | 59.3% | 0.6% |
| theone-source-server | 40.2% | 55.8% | 0.4% |

- Prometheus 가 들고 있는 시계열 **2,540개**
- Grafana 는 저장하지 않음 — 데이터소스가 `http://prometheus:9090`. **Prometheus 를 지우면 데이터가 없어지고, Grafana 를 지우면 그래프만 없어짐**

## EMR Serverless — CloudWatch

- 애플리케이션 `maximumCapacity` = **12 vCPU / 48 GB / 2000 GB** (인상 후)
- 워커 실사용량은 **Metrics Insights** 로 봄

```sql
SELECT SUM(WorkerMemoryUsed) FROM "AWS/EMRServerless"
WHERE ApplicationId = '...' GROUP BY WorkerType
```

**실측 (인상 전, 8 vCPU / 32 GB)**

| | 값 | 상한 |
|---|---:|---:|
| 워커 수 | 4개 (2 vCPU / 7GB 씩) | — |
| CPU 합 | **8 vCPU** | 8 |
| 메모리 합 | 26.05 GB | 32 |
| 디스크 | 80 GB | 200 |

- **CPU 가 정확히 상한에 붙어 병목**이었음. 5번째 워커에 10 vCPU 가 필요한데 8이 천장
- 12 vCPU / 48 GB 로 올려 워커 4 → 6 (executor **3 → 5개**)
- 자원의 대부분은 executor 가 씀 (메모리 23.85 GB vs driver 4.44 GB)

## 겪은 함정

### 1. 위젯 지표 한도 초과 (510 / 500)

- 증상: `허용된 최대 지표 수를 초과함` 으로 위젯이 그리기를 멈춤
- 원인: `SEARCH()` 의 차원 조건은 **부분 일치**. `ApplicationId` 만 걸면 `JobId` 차원 지표까지 잡힘

| 차원 조합 | 지표 수 |
|---|---:|
| `ApplicationId, ApplicationName, CapacityAllocationType, JobId, JobName, WorkerType` | **2,040** |
| `ApplicationId, ApplicationName, CapacityAllocationType, WorkerType` | 24 |
| `ApplicationId, ApplicationName` | 24 |

- 해결: 중괄호로 차원 조합 자체를 고정 → SEARCH 하나가 매칭하는 지표 **1개**

```
SEARCH('{AWS/EMRServerless,ApplicationId,ApplicationName} MetricName="..." ...')
```

- 작업이 2만 개가 돼도 개수가 안 늘어남

### 2. period 가 값을 5배로 부풀림

- Metrics Insights 의 `SUM` 은 **기간 안의 모든 샘플을 더함**. 지표가 1분 간격인데 `period: 300` 이면 5개가 합쳐짐

| period | executor 메모리 |
|---:|---:|
| 60 | **23.8 GB** (앱 할당 28.0 GB 와 정합) |
| 300 | 118.9 GB (앱 상한 32 GB 를 넘는 값) |

- **그래프는 멀쩡해 보이고 숫자만 틀리는** 실패라 테스트로 고정

### 3. 자동 색 배정이 지정색과 충돌

- `GROUP BY` 결과에는 색을 지정할 수 없고, CloudWatch 가 **위젯 안 순번**으로 팔레트를 배정
- 합계·상한을 범례 앞으로 옮겼더니 워커 선이 3·4번으로 밀려 **지정색과 같은 초록·빨강**을 받음

```
순번  1        2        3        4
색    #1f77b4  #ff7f0e  #2ca02c  #d62728
```

- 해결: 색을 지정할 수 없는 계열(`GROUP BY`)을 **맨 앞**에 둠

### 4. 앱 수준 `Allocated` 게이지는 동반 지표로 못 씀

- 워커가 도는 중에도 **0 으로 끊김** → "사용량 > 할당량" 이 활성 표본의 21~28% 에서 나타남

| | 교차 건수 | 교차 시점의 할당량 |
|---|---:|---|
| 메모리 | 68 / 330 (21%) | 전부 0.0 |
| CPU | 94 / 330 (28%) | 0.0, 2.0 |

- 해결: 상한값(`Max*Allowed`)을 기준선으로 씀 — 상수라 끊기지 않음

## 알림

- Grafana 통합 알림을 **파일로 프로비저닝**. UI 에서 만들면 컨테이너를 다시 만들 때 사라짐
- 웹훅은 Airflow 와 **같은 Secret**(`SLACK_WEBHOOK_URL`) → 채널도 같고 로테이션할 곳도 하나

| 규칙 | 임계 | 지속 | 평시 실측 |
|---|---|---|---|
| 노드 응답 없음 | `up == 0` | 2분 | 4대 모두 1 |
| 디스크 | 80% / 90% | 10분 / 5분 | 14~40% |
| 메모리 | 90% | 10분 | 41~59% |
| CPU | 90% | **15분** | 0.3~39% |

- CPU 만 15분으로 길게 잡음 — Airflow 는 잡이 돌면 정상적으로 튐
- swap 은 인스턴스에 없어(실측 `NaN`) 규칙을 두지 않음. 두면 영원히 NoData 로 울림
- `noDataState` 는 전부 `Alerting` — `OK` 로 두면 `node_exporter` 가 죽었을 때 조용해져 **감시가 멈춘 것을 감시가 알려주지 않음**

---

운영 절차와 파일별 설명은 [`monitoring/README.md`](../monitoring/README.md) 에 있습니다.
