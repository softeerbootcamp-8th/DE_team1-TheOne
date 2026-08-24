import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]
MONITORING = ROOT / "monitoring"
WORKFLOW = ROOT / ".github/workflows/deploy-monitoring.yml"

# CloudWatch 가 실제로 발행하는 값. 대문자·복수형(SPARK_EXECUTORS)으로 쓰면 SEARCH 가
# 아무것도 매칭하지 못하고 위젯이 조용히 빈 채로 그려집니다 (#890).
WORKER_TYPES = {"Spark_Executor", "Spark_Driver"}

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


def test_dashboard_shows_emr_job_states_and_worker_usage():
    titles = {
        widget["properties"].get("title") for widget in _dashboard()["widgets"]
    }
    assert {
        "EMR Serverless job states",
        "EMR worker memory usage (%)",
        "EMR worker CPU usage (%)",
    }.issubset(titles)


def test_dashboard_uses_the_worker_type_values_cloudwatch_publishes():
    body = (MONITORING / "dashboard.json").read_text()
    found = set(re.findall(r'WorkerType=\\"([^\\]+)\\"', body))
    assert found == WORKER_TYPES, f"실제 발행 값과 다릅니다: {found}"


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


def test_stack_deploy_uses_ssm_without_stored_secrets():
    """SSH 로 바꾸면 개인키가 Secrets 에 들어가고 러너 IP 대역을 열어야 합니다."""
    workflow = WORKFLOW.read_text()
    assert "AWS-RunShellScript" in workflow
    assert "vars.MONITORING_INSTANCE_ID" in workflow
    assert "secrets." not in workflow


def test_stack_deploy_checks_the_ssm_result():
    """기다리지 않으면 EC2 반영이 실패해도 워크플로가 초록불로 끝납니다."""
    workflow = WORKFLOW.read_text()
    assert "ssm wait command-executed" in workflow
    assert "StandardErrorContent" in workflow
