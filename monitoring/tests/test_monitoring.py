import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]


class CloudFormationLoader(yaml.SafeLoader):
    pass


def _construct_tag(loader, _tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


CloudFormationLoader.add_multi_constructor("!", _construct_tag)


def _stack():
    template = (ROOT / "monitoring/cloudformation.yml").read_text()
    return yaml.load(template, Loader=CloudFormationLoader)


def test_agent_collects_only_cost_conscious_host_metrics():
    config = json.loads((ROOT / "monitoring/cloudwatch-agent.json").read_text())

    assert config["agent"]["metrics_collection_interval"] == 60
    assert config["metrics"]["namespace"] == "TheOne/EC2"
    assert config["metrics"]["aggregation_dimensions"] == [["InstanceId"]]
    collected = config["metrics"]["metrics_collected"]
    assert set(collected) == {"mem", "disk"}
    assert collected["mem"]["drop_original_metrics"] == ["used_percent"]
    assert collected["disk"]["resources"] == ["/"]
    assert collected["disk"]["drop_original_metrics"] == ["used_percent"]


def test_stack_connects_three_ec2_instances_and_emr_metrics():
    stack = _stack()
    parameters = stack["Parameters"]
    assert {
        "AirflowInstanceId",
        "SourceInstanceId",
        "DashboardInstanceId",
        "EmrApplicationId",
    }.issubset(parameters)
    # AWS::EC2::Instance::Id 로 되돌리면 CloudFormation 이 배포 자격증명으로
    # ec2:DescribeInstances 를 호출해 배포가 다시 깨집니다.
    for parameter in (
        "AirflowInstanceId",
        "SourceInstanceId",
        "DashboardInstanceId",
    ):
        assert parameters[parameter]["Type"] == "String"
        assert parameters[parameter]["AllowedPattern"] == r"^i-[0-9a-f]{8,17}$"

    body = stack["Resources"]["UnifiedDashboard"]["Properties"]["DashboardBody"]
    rendered = re.sub(r"\$\{[^}]+}", "placeholder", body)
    dashboard = json.loads(rendered)
    titles = {widget["properties"].get("title") for widget in dashboard["widgets"]}
    assert {"EC2 CPU (%)", "EC2 memory (%)", "EC2 root disk (%)"}.issubset(titles)
    assert {"EMR worker memory usage (%)", "EMR worker CPU usage (%)"}.issubset(titles)
    assert "AWS/EMRServerless" in body
    assert "JobId=" not in body


def test_stack_grants_agent_permissions_and_routes_emr_failures():
    resources = _stack()["Resources"]
    policy = resources["AgentMetricPolicy"]["Properties"]
    assert len(policy["Roles"]) == 3
    statements = policy["PolicyDocument"]["Statement"]
    assert any(s["Action"] == "cloudwatch:PutMetricData" for s in statements)
    assert any(s["Action"] == "ssm:GetParameter" for s in statements)

    pattern = resources["EmrJobFailureRule"]["Properties"]["EventPattern"]
    assert pattern["source"] == ["aws.emr-serverless"]
    assert set(pattern["detail"]["state"]) == {"FAILED", "CANCELLED"}


def test_workflow_installs_then_configures_all_instances():
    workflow = (ROOT / ".github/workflows/deploy-monitoring.yml").read_text()
    for variable in (
        "AIRFLOW_INSTANCE_ID",
        "SERVER_INSTANCE_ID",
        "DASHBOARD_INSTANCE_ID",
        "EMR_APPLICATION_ID",
    ):
        assert variable in workflow
    assert "ssm put-parameter" in workflow
    assert workflow.index("AWS-ConfigureAWSPackage") < workflow.index(
        "AmazonCloudWatch-ManageAgent"
    )
