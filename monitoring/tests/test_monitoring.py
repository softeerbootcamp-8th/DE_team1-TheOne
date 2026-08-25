import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]
MONITORING = ROOT / "monitoring"
WORKFLOW = ROOT / ".github/workflows/deploy-monitoring.yml"

# SEARCH 는 차원을 "부분 일치"로 봅니다. 이름만 걸면 그 이름을 가진 모든 차원 조합이
# 잡히는데, EMR Serverless 는 Worker* 지표를 JobId 별로 발행합니다. 그래서 작업이
# 쌓일수록 위젯이 무한히 커지고, 500개를 넘으면 CloudWatch 가 "허용된 최대 지표 수를
# 초과함" 을 띄우고 그리기를 포기합니다. 중괄호로 차원 조합 자체를 고정해야 막힙니다.
APP_SCHEMA = "{AWS/EMRServerless,ApplicationId,ApplicationName}"

# 위 스키마로 실제 발행되는 지표명 (`aws cloudwatch list-metrics` 로 확인).
# 여기 없는 이름을 쓰면 위젯이 조용히 비거나(#890) JobId 차원으로 새어 폭증합니다.
APP_LEVEL_METRICS = {
    "CPUAllocated", "CancelledJobs", "CancellingJobs", "FailedJobs", "FailedSessions",
    "IdleWorkerCount", "MaxCPUAllowed", "MaxMemoryAllowed", "MaxStorageAllowed",
    "MemoryAllocated", "PendingCreationWorkerCount", "PendingJobs", "RunningJobs",
    "RunningWorkerCount", "ScheduledJobs", "StartedSessions", "StartingSessions",
    "StorageAllocated", "SubmittedJobs", "SubmittedSessions", "SuccessJobs",
    "TerminatedSessions", "TerminatingSessions", "TotalWorkerCount",
}

# JobId 차원으로만 발행되어 대시보드에 두면 작업 수만큼 시계열이 늘어나는 지표.
PER_JOB_METRICS = {
    "WorkerCpuAllocated", "WorkerCpuUsed", "WorkerMemoryAllocated", "WorkerMemoryUsed",
    "WorkerEphemeralStorageAllocated", "WorkerEphemeralStorageUsed",
    "WorkerStorageReadBytes", "WorkerStorageWriteBytes",
}

PLACEHOLDERS = {"AWS_REGION", "EMR_APPLICATION_ID", "TOPIC_ARN", "AWS_ACCOUNT_ID"}


def _render(path, **values):
    text = path.read_text()
    for key, value in values.items():
        text = text.replace("${" + key + "}", value)
    return text


def _dashboard():
    rendered = _render(
        MONITORING / "dashboard.json",
        AWS_REGION="ap-northeast-2",
        EMR_APPLICATION_ID="00g85el8u6tujt2p",
    )
    assert "${" not in rendered, "치환되지 않은 플레이스홀더가 남았습니다"
    return json.loads(rendered)


def test_dashboard_shows_job_states_usage_and_workers():
    titles = {
        widget["properties"].get("title") for widget in _dashboard()["widgets"]
    }
    assert {
        "EMR Serverless job states",
        "EMR memory used (GB)",
        "EMR CPU used (vCPU)",
        "EMR worker count",
    }.issubset(titles)


def test_usage_widgets_split_driver_and_executor():
    """합치면 튜닝 대상이 안 보입니다. 자원의 대부분은 executor 가 씁니다
    (실측 executor 23.85GB vs driver 4.44GB) — 합계만 보면 driver 를 줄여야 하는지
    executor 를 줄여야 하는지 알 수 없습니다.
    """
    for title, expressions in _widget_expressions():
        if "used (" not in title:
            continue
        selects = [e for e in expressions if e.startswith("SELECT ")]
        assert selects, f"{title}: Metrics Insights 쿼리가 없습니다"
        assert all("GROUP BY WorkerType" in e for e in selects), (
            f"{title}: driver·executor 를 나누지 않습니다"
        )


def test_usage_widgets_show_the_total_of_both_worker_types():
    """driver 와 executor 는 서로 다른 워커 집단이라 자원을 나눠 씁니다.

    상한선은 둘을 합친 애플리케이션 전체 한도라, 개별 선만 있으면 눈으로 더해야
    남은 여유를 알 수 있습니다. executor 23.80 만 보고 24GB 라고 읽으면 driver 2.07 이
    빠집니다(실제 25.87).

    `SUM()` 을 GROUP BY 결과에 걸면 Metrics Insights 쿼리를 더 쓰지 않고 합계선이
    나옵니다 — 위젯당 쿼리 1개 제한을 지키면서.
    """
    for title, expressions in _widget_expressions():
        if "used (" not in title:
            continue
        assert "SUM(u)" in expressions, f"{title}: 합계선이 없습니다"


# CloudWatch 가 색을 지정하지 않은 계열에 주는 기본 팔레트. 위젯 안 "순번"으로 배정되며,
# 다른 계열이 같은 색을 지정해 뒀는지는 보지 않습니다.
DEFAULT_PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def _rendered_colors(widget):
    """위젯이 실제로 그릴 색을 순번대로 계산합니다.

    `GROUP BY WorkerType` 은 driver·executor 두 계열이 되므로 순번을 둘 소비합니다.
    """
    colors, index = [], 0
    for row in widget["properties"]["metrics"]:
        for item in row:
            if not isinstance(item, dict):
                continue
            count = 2 if "GROUP BY WorkerType" in item.get("expression", "") else 1
            explicit = item.get("color")
            for offset in range(count):
                slot = index + offset
                colors.append((
                    item["id"],
                    (explicit or DEFAULT_PALETTE[slot % len(DEFAULT_PALETTE)]).lower(),
                ))
            index += count
    return colors


def test_no_two_lines_in_a_widget_get_the_same_color():
    """자동 색은 위젯 안 순번으로 정해지고, 다른 계열의 지정색을 피해 가지 않습니다.

    합계·상한을 범례 앞으로 옮겼더니 워커 선이 3·4번 순번으로 밀려 초록·빨강을 받았고,
    합계·상한에 지정한 초록·빨강과 똑같아져 네 선이 두 쌍으로 겹쳐 보였습니다.
    `GROUP BY` 결과에는 색을 지정할 수 없으므로 순서로만 막을 수 있습니다.
    """
    for widget in _dashboard()["widgets"]:
        if widget["type"] != "metric":
            continue
        colors = _rendered_colors(widget)
        seen = {}
        for series_id, color in colors:
            assert color not in seen, (
                f"{widget['properties'].get('title')}: {series_id} 와 {seen[color]} 가 "
                f"모두 {color} 입니다"
            )
            seen[color] = series_id


def test_grouped_series_come_first_so_they_take_the_base_palette():
    """색을 지정할 수 없는 계열이 앞에 와야 합니다.

    뒤로 밀리면 팔레트 뒤쪽 색을 받는데, 그 색을 다른 계열이 이미 지정해 뒀을 수 있습니다.
    """
    for widget in _dashboard()["widgets"]:
        if widget["type"] != "metric":
            continue
        rows = widget["properties"]["metrics"]
        grouped = [n for n, row in enumerate(rows)
                   if "GROUP BY WorkerType" in row[0].get("expression", "")]
        if not grouped:
            continue
        assert grouped == [0], (
            f"{widget['properties'].get('title')}: GROUP BY 계열이 첫 번째가 아닙니다"
        )


def test_usage_widgets_draw_the_ceiling_from_a_metric():
    """"20GB 사용" 은 상한을 모르면 해석이 안 됩니다. 상한선이 있어야 남은 여유가 보입니다.

    값을 박지 않고 `Max*Allowed` 지표로 그립니다. maximumCapacity 를 올렸을 때
    선이 따라 올라가야 하고, 박아두면 조용히 틀린 기준을 보여줍니다.
    """
    for title, expressions in _widget_expressions():
        if "used (" not in title:
            continue
        joined = " ".join(expressions)
        assert "MaxMemoryAllowed" in joined or "MaxCPUAllowed" in joined, (
            f"{title}: 상한선이 없습니다"
        )


def test_usage_widgets_avoid_the_app_level_allocated_gauge():
    """앱 수준 `MemoryAllocated`/`CPUAllocated` 는 워커가 도는 중에도 0 으로 끊깁니다.

    작업별 사용량과 나란히 두면 "사용량 > 할당량" 이 활성 표본의 21~28% 에서 나타나
    보는 사람을 혼란스럽게 합니다(실측 330 표본). 상한값은 상수라 그런 일이 없습니다.
    """
    for title, expressions in _widget_expressions():
        if "used (" not in title:
            continue
        for name in ("MemoryAllocated", "CPUAllocated"):
            for expression in expressions:
                assert f'MetricName="{name}"' not in expression, (
                    f"{title}: {name} 는 0 으로 끊겨 동반 지표로 쓸 수 없습니다"
                )


def test_metrics_insights_widgets_aggregate_per_minute():
    """Metrics Insights 의 SUM 은 기간 안의 모든 샘플을 더합니다.

    지표가 1분 간격이라 period 를 300 으로 두면 샘플 5개가 합쳐져 값이 5배가 됩니다
    (실측: executor 메모리 23.8GB 가 118.9GB 로 표시). 그래프는 멀쩡해 보이고 숫자만
    틀리는, 알아채기 어려운 실패라 고정합니다.
    """
    for widget in _dashboard()["widgets"]:
        if widget["type"] != "metric":
            continue
        expressions = [
            item.get("expression", "")
            for row in widget["properties"]["metrics"]
            for item in row
            if isinstance(item, dict)
        ]
        if not any(e.startswith("SELECT ") for e in expressions):
            continue
        period = widget["properties"].get("period")
        assert period == 60, (
            f"{widget['properties']['title']}: period 가 {period} 입니다 — 60 이어야 합니다"
        )


def _search_expressions():
    for widget in _dashboard()["widgets"]:
        if widget["type"] != "metric":
            continue
        for row in widget["properties"]["metrics"]:
            for item in row:
                expression = item.get("expression", "") if isinstance(item, dict) else ""
                if "SEARCH(" in expression:
                    yield expression


def test_every_search_pins_the_dimension_schema():
    """차원 이름만 걸면 SEARCH 가 JobId 차원 지표까지 함께 잡습니다.

    Worker* 지표는 작업마다 새 시계열이 생기므로, 이렇게 두면 위젯이 계속 커지다가
    500개를 넘는 순간 "허용된 최대 지표 수를 초과함" 으로 그리기를 멈춥니다. 실제로
    memory·CPU 위젯이 각각 510개까지 늘어 깨졌습니다. 중괄호 스키마 고정이 유일한
    구조적 방어라서, 새 위젯이 이걸 빠뜨리지 못하게 막습니다.
    """
    expressions = list(_search_expressions())
    assert expressions, "SEARCH 가 하나도 없습니다"
    for expression in expressions:
        assert f"SEARCH('{APP_SCHEMA} " in expression, f"스키마 미고정: {expression}"


def _widget_expressions():
    for widget in _dashboard()["widgets"]:
        if widget["type"] != "metric":
            continue
        expressions = [
            item.get("expression", "")
            for row in widget["properties"]["metrics"]
            for item in row
            if isinstance(item, dict)
        ]
        yield widget["properties"]["title"], expressions


def test_per_job_metrics_are_only_read_through_metrics_insights():
    """작업별 지표를 SEARCH 로 끌어오면 작업 수만큼 시계열이 늘어 한도를 넘습니다.

    Metrics Insights 는 서버에서 집계해 GROUP BY 결과만 돌려주므로 스캔한 지표가
    위젯 개수에 잡히지 않습니다. Worker*Used 는 앱 수준에 아예 없어서 사용량을 보려면
    이 경로뿐입니다. 그래서 '쓰지 마라' 가 아니라 '이 경로로만 써라' 로 고정합니다.
    """
    for title, expressions in _widget_expressions():
        for expression in expressions:
            used = {m for m in PER_JOB_METRICS if m in expression}
            if not used:
                continue
            assert expression.startswith("SELECT "), (
                f"{title}: {used} 를 SEARCH 로 읽고 있습니다 — Metrics Insights 를 쓰세요"
            )


def test_each_widget_has_at_most_one_metrics_insights_query():
    """GetMetricData 는 호출당 Metrics Insights 쿼리를 1개만 받습니다. 위젯 하나가

    한 번의 호출로 그려지므로, 두 개를 넣으면 배포는 통과하고 위젯만 깨집니다.
    used/allocated 비율을 한 위젯에 못 담는 이유가 이것입니다.
    """
    for title, expressions in _widget_expressions():
        count = sum(1 for e in expressions if e.startswith("SELECT "))
        assert count <= 1, f"{title}: Metrics Insights 쿼리가 {count}개입니다"


def test_dashboard_metric_names_are_published_at_app_level():
    found = set(re.findall(r'MetricName=\\"([^\\"]+)\\"', json.dumps(_dashboard())))
    assert found, "MetricName 을 하나도 찾지 못했습니다"
    assert found <= APP_LEVEL_METRICS, f"앱 수준에 없는 지표: {found - APP_LEVEL_METRICS}"


def test_dashboard_carries_no_ec2_host_metrics():
    """EC2 자원은 Grafana 담당. Agent 권한이 없어 여기 두면 빈 위젯이 됩니다."""
    body = (MONITORING / "dashboard.json").read_text()
    for namespace in ("TheOne/EC2", "CWAgent", "AWS/EC2"):
        assert namespace not in body
    assert "mem_used_percent" not in body
    assert "disk_used_percent" not in body


def test_dashboard_aggregates_without_pinning_job_identifiers():
    """JobId/JobName 을 고정하면 작업마다 시계열이 늘어 위젯이 한계에 걸립니다."""
    body = (MONITORING / "dashboard.json").read_text()
    assert "JobId=" not in body
    assert "JobName=" not in body


def test_alert_routes_emr_failures_to_the_topic():
    pattern = json.loads(
        _render(
            MONITORING / "emr-failure-event-pattern.json",
            EMR_APPLICATION_ID="00g85el8u6tujt2p",
        )
    )
    assert pattern["source"] == ["aws.emr-serverless"]
    assert set(pattern["detail"]["state"]) == {"FAILED", "CANCELLED"}

    policy = json.loads(
        _render(
            MONITORING / "alert-topic-policy.json",
            TOPIC_ARN="arn:aws:sns:ap-northeast-2:572660899671:theone-pipeline-alerts",
            AWS_ACCOUNT_ID="572660899671",
        )
    )
    statement = policy["Statement"][0]
    assert statement["Principal"]["Service"] == "events.amazonaws.com"
    assert statement["Action"] == "sns:Publish"
    # SourceAccount 조건이 없으면 다른 계정의 EventBridge 가 이 Topic 에 발행할 수 있습니다.
    assert statement["Condition"]["StringEquals"]["AWS:SourceAccount"] == "572660899671"


def test_deploy_uses_idempotent_cli_without_cloudformation():
    workflow = WORKFLOW.read_text()
    # CloudFormation 은 DeleteStack·AlreadyExists·파라미터 검증으로 세 번 배포를 막았습니다.
    # 주석으로 그 이유를 남기는 건 괜찮고, 명령을 다시 부르는 것만 막습니다.
    assert "aws cloudformation" not in workflow
    assert not (MONITORING / "cloudformation.yml").exists()
    for command in (
        "cloudwatch put-dashboard",
        "sns create-topic",
        "sns set-topic-attributes",
        "events put-rule",
        "events put-targets",
    ):
        assert command in workflow


def test_deploy_no_longer_installs_the_cloudwatch_agent():
    """Agent 는 인스턴스 role 에 PutMetricData 가 없어 지표를 보낼 수 없습니다."""
    workflow = WORKFLOW.read_text()
    for removed in (
        "AmazonCloudWatchAgent",
        "AmazonCloudWatch-ManageAgent",
        "AWS-ConfigureAWSPackage",
        "ssm put-parameter",
    ):
        assert removed not in workflow
    assert not (MONITORING / "cloudwatch-agent.json").exists()


def test_deploy_fails_loudly_on_unsubstituted_placeholders():
    """치환이 빠지면 CloudWatch 는 에러 없이 빈 위젯을 그립니다. 배포가 막아야 합니다."""
    workflow = WORKFLOW.read_text()
    assert "envsubst" in workflow
    assert "치환되지 않은 플레이스홀더" in workflow
    assert "set -euo pipefail" in workflow


def test_deploy_checks_the_results_that_do_not_raise():
    """put-dashboard 와 put-targets 는 실패해도 예외가 아니라 값으로 알려줍니다.

    검사하지 않으면 배포는 초록불인데 위젯이 깨지거나 알림이 오지 않습니다.
    """
    workflow = WORKFLOW.read_text()
    assert "DashboardValidationMessages" in workflow
    assert '!= "[]"' in workflow
    assert "FailedEntryCount" in workflow
    assert 'if [ "$failed" != "0" ]' in workflow


def test_workflow_is_valid_yaml_and_passes_required_variables():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["deploy"]["steps"]
    rendered = WORKFLOW.read_text()
    for variable in ("AWS_REGION", "EMR_APPLICATION_ID", "AWS_ROLE_ARN_MONITORING"):
        assert f"vars.{variable}" in rendered
    # envsubst 를 쓰는 스텝은 그 변수를 env 로 받아야 합니다.
    for step in steps:
        if "envsubst" in step.get("run", ""):
            assert "EMR_APPLICATION_ID" in step.get("env", {})


STACK = MONITORING / "stack"
TARGET_IPS = {
    "10.0.10.28": "theone-airflow",
    "10.0.10.81": "theone-source-server",
    "10.0.10.8": "theone-dashboard-server",
    "10.0.0.113": "theone-gateway",
}


def _prometheus_config():
    return yaml.safe_load((STACK / "prometheus.yml").read_text())


def _stack_compose():
    return yaml.safe_load((STACK / "docker-compose.yml").read_text())


def test_prometheus_scrapes_every_ec2_host():
    """대상을 빠뜨리면 그 인스턴스는 조용히 감시 밖에 남습니다."""
    targets = {
        t
        for job in _prometheus_config()["scrape_configs"]
        for sc in job["static_configs"]
        for t in sc["targets"]
    }
    assert {f"{ip}:9100" for ip in TARGET_IPS} == targets


def test_every_target_carries_a_readable_name():
    """IP 만 있으면 대시보드에서 어느 인스턴스인지 알 수 없습니다."""
    for job in _prometheus_config()["scrape_configs"]:
        for sc in job["static_configs"]:
            assert sc["labels"]["instance_name"] in TARGET_IPS.values()


def test_prometheus_retention_is_bounded():
    """보존 기간을 정하지 않으면 디스크가 찰 때까지 늘어납니다 (#698 과 같은 사고)."""
    command = _stack_compose()["services"]["prometheus"]["command"]
    assert any("--storage.tsdb.retention.time=" in c for c in command)


def test_ui_ports_stay_on_loopback():
    """3000·9090 을 0.0.0.0 에 열면 익명 접근 설정과 겹쳐 무인증 공개가 됩니다."""
    services = _stack_compose()["services"]
    for name in ("prometheus", "grafana"):
        for mapping in services[name]["ports"]:
            assert str(mapping).startswith("127.0.0.1:"), (name, mapping)


def test_grafana_datasource_is_provisioned_not_clicked():
    """UI 로 추가하면 재생성 때 사라지고 재현이 안 됩니다."""
    path = STACK / "grafana/provisioning/datasources/prometheus.yml"
    source = yaml.safe_load(path.read_text())["datasources"][0]
    assert source["type"] == "prometheus"
    assert source["url"] == "http://prometheus:9090"


def test_stack_deploy_uses_ssm_not_ssh():
    """SSH 로 바꾸면 개인키가 Secrets 에 들어가고 러너 IP 대역을 열어야 합니다.

    예전에는 `secrets.` 자체를 금지했습니다. 지키려던 것은 "개인키를 Secrets 에 두지
    말 것" 인데 단정이 그보다 넓어서, Slack 웹훅처럼 개인키가 아닌 값도 함께 막혔습니다.
    실제로 지켜야 하는 것만 남깁니다 — 접속 수단은 SSM 이고 키 자재는 두지 않습니다.
    """
    workflow = WORKFLOW.read_text()
    assert "AWS-RunShellScript" in workflow
    assert "vars.MONITORING_INSTANCE_ID" in workflow
    for forbidden in ("EC2_SSH_PRIVATE_KEY", "ssh-action", "PRIVATE_KEY", "id_rsa"):
        assert forbidden not in workflow, f"{forbidden} 이 들어왔습니다 — SSM 을 유지하세요"


def test_stack_deploy_checks_the_ssm_result():
    """기다리지 않으면 EC2 반영이 실패해도 워크플로가 초록불로 끝납니다."""
    workflow = WORKFLOW.read_text()
    assert "ssm wait command-executed" in workflow
    assert "StandardErrorContent" in workflow


# --- Grafana Slack 알림 -------------------------------------------------------
ALERTING = STACK / "grafana/provisioning/alerting"


def _alerting(name):
    return yaml.safe_load((ALERTING / name).read_text())


def test_웹훅은_저장소_파일에_없다():
    """파일에 박으면 저장소 이력에 영구히 남고 로테이션이 불가능합니다."""
    for path in ALERTING.glob("*.yaml"):
        body = path.read_text()
        assert "hooks.slack.com/services/T" not in body, path.name


def test_수신처가_환경변수로_웹훅을_읽는다():
    """`$__env{}` 가 아니면 Grafana 는 문자열 그대로 저장하고, 전송이 URL 파싱에서
    실패합니다. 알림이 조용히 안 가는 형태라 로그를 봐야만 압니다.
    """
    receivers = _alerting("contact-points.yaml")["contactPoints"][0]["receivers"]
    slack = next(r for r in receivers if r["type"] == "slack")

    assert slack["settings"]["url"] == "$__env{SLACK_WEBHOOK_URL}"


def test_compose_가_웹훅을_필수값으로_넘긴다():
    """비어 있으면 컨테이너가 안 뜨는 쪽이 낫습니다 — 뜨고 알림만 안 오는 것보다."""
    compose = (STACK / "docker-compose.yml").read_text()

    assert "SLACK_WEBHOOK_URL: ${SLACK_WEBHOOK_URL:?" in compose


def test_배포가_웹훅을_env_파일로_내려준다():
    workflow = WORKFLOW.read_text()

    assert "SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}" in workflow
    assert "echo SLACK_WEBHOOK_URL=" in workflow


def test_알림_경로가_슬랙으로_간다():
    policy = _alerting("policies.yaml")["policies"][0]

    assert policy["receiver"] == "slack"
    # 호스트별로 묶어야 어느 인스턴스를 봐야 하는지가 메시지 단위로 갈립니다.
    assert "instance_name" in policy["group_by"]


def test_규칙이_고정된_데이터소스_uid를_쓴다():
    """uid 를 비우면 Grafana 가 난수를 만들고, 컨테이너를 다시 만들 때 값이 바뀌어
    규칙이 데이터소스를 잃습니다 — 규칙은 남고 평가만 실패합니다.
    """
    datasource = yaml.safe_load(
        (STACK / "grafana/provisioning/datasources/prometheus.yml").read_text()
    )["datasources"][0]
    uid = datasource["uid"]

    for group in _alerting("rules.yaml")["groups"]:
        for rule in group["rules"]:
            used = {
                q["datasourceUid"]
                for q in rule["data"]
                if q["datasourceUid"] != "__expr__"
            }
            assert used == {uid}, f"{rule['title']}: {used}"


def test_지표가_안_오는_것도_알린다():
    """`noDataState: OK` 로 두면 node_exporter 가 죽었을 때 조용해집니다 — 감시가
    멈춘 것을 감시가 알려주지 않는 상태가 됩니다.
    """
    for group in _alerting("rules.yaml")["groups"]:
        for rule in group["rules"]:
            assert rule["noDataState"] == "Alerting", rule["title"]


def test_없는_지표로_규칙을_만들지_않는다():
    """swap 은 인스턴스에 없어서(실측 NaN) 규칙을 두면 영원히 NoData 로 울립니다."""
    body = (ALERTING / "rules.yaml").read_text()

    assert "node_memory_Swap" not in body


# --- CloudWatch 데이터소스와 Lambda 알림 ---------------------------------------
DATASOURCES = STACK / "grafana/provisioning/datasources"


def test_cloudwatch_는_키_없이_인스턴스_role_을_쓴다():
    """`authType: default` 는 AWS SDK 기본 체인이라 IMDS 에서 자격증명을 받습니다.

    keys 를 쓰면 파일이나 Secret 에 자격증명이 남고 만료 갱신도 사람이 해야 합니다.
    """
    datasource = yaml.safe_load((DATASOURCES / "cloudwatch.yml").read_text())["datasources"][0]

    assert datasource["type"] == "cloudwatch"
    assert datasource["jsonData"]["authType"] == "default"
    assert "accessKey" not in yaml.safe_dump(datasource)
    assert "secretKey" not in yaml.safe_dump(datasource)


def test_lambda_규칙은_지표가_없을_때_울리지_않는다():
    """호스트 규칙과 반대입니다.

    호스트는 지표가 끊기면 그 자체가 문제라 Alerting 이지만, Lambda 는
    "실패 지표가 없음 = 실패가 없었음" 입니다. Alerting 으로 두면 그날 안 도는
    함수들이 매분 울려 알림이 무시당하게 됩니다.
    """
    for group in yaml.safe_load((ALERTING / "lambda-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            assert rule["noDataState"] == "OK", rule["title"]


def test_lambda_규칙은_함수를_고정하지_않는다():
    """함수 이름을 박으면 새 함수를 배포할 때마다 규칙을 고쳐야 하고,
    고치는 걸 잊으면 그 함수만 감시에서 빠집니다.
    """
    for group in yaml.safe_load((ALERTING / "lambda-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            for query in rule["data"]:
                model = query["model"]
                if model.get("namespace") != "AWS/Lambda":
                    continue
                assert model["dimensions"] == {"FunctionName": "*"}
                # matchExact 는 true 여야 합니다. false 면 FunctionName 을 포함한 모든
                # 차원 조합이 잡혀 AWS 가 함께 발행하는 Resource 차원 변형까지 딸려오고,
                # 함수마다 시계열이 두 개씩 생겨 같은 실패로 알림이 두 번 옵니다
                # (실측 14개 -> 28개).
                assert model["matchExact"] is True


def test_lambda_규칙이_고정된_데이터소스_uid를_쓴다():
    uid = yaml.safe_load((DATASOURCES / "cloudwatch.yml").read_text())["datasources"][0]["uid"]

    for group in yaml.safe_load((ALERTING / "lambda-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            used = {q["datasourceUid"] for q in rule["data"] if q["datasourceUid"] != "__expr__"}
            assert used == {uid}, f"{rule['title']}: {used}"


def test_배포가_cloudwatch_설정을_내려보낸다():
    workflow = WORKFLOW.read_text()

    for path in ("datasources/cloudwatch.yml", "alerting/lambda-rules.yaml"):
        assert path in workflow, f"{path} 가 배포 목록에 없습니다"


def test_배포가_grafana_를_재기동한다():
    """Grafana 는 프로비저닝을 **기동 시에만** 읽고 SIGHUP 재적재를 지원하지 않습니다.

    이미지도 compose 정의도 안 바뀌면 `up -d` 가 컨테이너를 그대로 둡니다. 그러면
    바인드 마운트된 파일만 새것이고 Grafana 안은 옛 설정이라, **배포는 초록불인데
    반영이 안 된 상태**가 됩니다. 실제로 데이터소스와 알림 규칙이 16시간 동안
    반영되지 않았습니다.
    """
    workflow = WORKFLOW.read_text()

    assert "docker compose restart grafana" in workflow


def test_배포가_prometheus_설정을_다시_읽게_한다():
    """Prometheus 는 SIGHUP 으로 재적재합니다 — 재기동보다 짧게 끊깁니다."""
    workflow = WORKFLOW.read_text()

    assert "SIGHUP prometheus" in workflow

DASHBOARDS = STACK / "grafana/provisioning/dashboards"


def test_lambda_대시보드가_파일로_있다():
    """UI 에서 만들면 컨테이너를 다시 만들 때 사라집니다 (데이터소스·알림과 같은 이유)."""
    provider = yaml.safe_load((DASHBOARDS / "dashboards.yaml").read_text())["providers"][0]
    assert provider["options"]["path"] == "/etc/grafana/provisioning/dashboards"
    # 파일이 정본이라 UI 수정을 막습니다 — 막지 않으면 파일과 화면이 갈립니다.
    assert provider["allowUiUpdates"] is False

    dashboard = json.loads((DASHBOARDS / "lambda.json").read_text())
    assert dashboard["uid"] == "theone-lambda"
    assert len(dashboard["panels"]) >= 4


def test_대시보드도_시리즈를_중복시키지_않는다():
    """알림과 같은 이유입니다 — false 면 함수마다 선이 두 개 그려집니다."""
    dashboard = json.loads((DASHBOARDS / "lambda.json").read_text())

    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            assert target["matchExact"] is True, panel["title"]
            assert target["dimensions"] == {"FunctionName": "*"}, panel["title"]


def test_배포가_대시보드를_내려보낸다():
    workflow = WORKFLOW.read_text()

    assert "dashboards/lambda.json" in workflow
    assert "dashboards/dashboards.yaml" in workflow
    assert "grafana/provisioning/dashboards" in workflow


def test_lambda_규칙은_축약_단계를_거친다():
    """Grafana 알림은 **축약된 값**만 임계와 비교할 수 있습니다.

    Prometheus 규칙은 `instant: true` 라 값이 계열당 하나씩이지만, CloudWatch 는 항상
    시계열입니다. 그대로 비교하면 규칙은 남고 평가만 error 가 됩니다 — UI 에서
    `Health: error` 로 보이고 **알림은 영원히 안 옵니다.**

        invalid format of evaluation results ... only reduced data can be alerted on
    """
    for group in yaml.safe_load((ALERTING / "lambda-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            kinds = [q["model"].get("type") for q in rule["data"]]
            assert "reduce" in kinds, f"{rule['title']}: 축약 단계가 없습니다"

            reduce_node = next(q for q in rule["data"] if q["model"].get("type") == "reduce")
            threshold = next(q for q in rule["data"] if q["model"].get("type") == "threshold")

            # 임계는 원본이 아니라 축약 결과를 봐야 합니다.
            assert threshold["model"]["expression"] == reduce_node["refId"]
            # CloudWatch 는 호출이 없던 구간을 null 로 돌려줍니다. 그대로 더하면 NaN 이
            # 되어 규칙이 NoData 로 빠집니다.
            assert reduce_node["model"]["settings"]["mode"] == "dropNN"
