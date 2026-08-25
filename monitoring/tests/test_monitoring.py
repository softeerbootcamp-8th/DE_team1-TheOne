import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).parents[2]
MONITORING = ROOT / "monitoring"
WORKFLOW = ROOT / ".github/workflows/deploy-monitoring.yml"

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


# --- EMR Serverless (CloudWatch 대시보드에서 이관) -----------------------------


def test_emr_대시보드가_파일로_있다():
    """CloudWatch 대시보드를 걷어내고 Grafana 로 옮겼습니다.

    옮긴 이유는 알림입니다 — CloudWatch 대시보드에는 알림이 없어서 CPU 가 상한에
    붙어 있어도 사람이 열어보기 전까지 아무도 몰랐습니다.
    """
    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())

    assert dashboard["uid"] == "theone-emr"
    titles = {p["title"] for p in dashboard["panels"]}
    assert {"진행 중", "기간 결과 (합계)", "메모리 사용 (GB)",
            "CPU 사용 (vCPU)", "워커 수"} <= titles


def test_emr_질의는_ApplicationName_을_함께_준다():
    """`ApplicationId` 만 주면 `matchExact` 가 맞지 않아 **0계열**이 됩니다.

    실제 지표의 차원 조합이 `{ApplicationId, ApplicationName}` 이라서입니다. 패널은
    에러 없이 비어 있게 되므로 눈으로만 봐서는 원인을 알 수 없습니다(실측).
    """
    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())

    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            if target.get("metricQueryType") == 1:
                continue  # Metrics Insights 는 SQL 로 지목합니다
            assert "ApplicationName" in target["dimensions"], panel["title"]
            assert target["matchExact"] is True, panel["title"]


def test_한_패널에_Metrics_Insights_는_하나다():
    """`GetMetricData` 가 호출당 하나만 받습니다 — Grafana 도 같은 제한입니다.

    두 개를 넣으면 배포는 통과하고 패널만 `Maximum number of queries (1) exceeded`
    로 깨집니다.
    """
    for name in ("emr.json", "lambda.json"):
        dashboard = json.loads((DASHBOARDS / name).read_text())
        for panel in dashboard["panels"]:
            count = sum(1 for t in panel["targets"] if t.get("metricQueryType") == 1)
            assert count <= 1, f"{name} / {panel['title']}: {count}개"


def test_emr_작업_실패_알림이_있다():
    """EventBridge -> SNS -> 이메일 경로를 걷어냈으므로, 이 규칙이 없으면 작업 실패를
    알려주는 곳이 사라집니다.
    """
    rules = yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())
    titles = {r["title"] for g in rules["groups"] for r in g["rules"]}

    assert "EMR 작업 실패" in titles


def test_emr_규칙도_축약_단계를_거친다():
    """Lambda 규칙과 같은 이유입니다 — CloudWatch 는 항상 시계열입니다."""
    for group in yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            kinds = [q["model"].get("type") for q in rule["data"]]
            assert "reduce" in kinds, f"{rule['title']}: 축약 단계가 없습니다"


def test_배포가_emr_설정을_내려보낸다():
    workflow = WORKFLOW.read_text()

    assert "dashboards/emr.json" in workflow
    assert "alerting/emr-rules.yaml" in workflow


def test_CloudWatch_대시보드와_SNS_경로를_남기지_않는다():
    """옮긴 뒤 남겨두면 두 곳을 봐야 하고, 배포도 두 벌을 유지해야 합니다."""
    workflow = WORKFLOW.read_text()

    for gone in ("cloudwatch put-dashboard", "sns create-topic", "events put-rule"):
        assert gone not in workflow, f"{gone} 가 남아 있습니다"
    for gone in ("dashboard.json", "alert-topic-policy.json", "emr-failure-event-pattern.json"):
        assert not (MONITORING / gone).exists(), f"{gone} 가 남아 있습니다"


# --- 플레이스홀더 치환 ---------------------------------------------------------
PROVISIONING = STACK / "grafana/provisioning"
PLACEHOLDER = re.compile(r"\$\{([A-Z_]+)\}")


def _provisioning_files():
    return [f for f in PROVISIONING.rglob("*") if f.suffix in (".json", ".yaml", ".yml")]


def test_배포가_플레이스홀더를_치환한다():
    """프로비저닝 파일은 base64 로 **그대로** 복사됩니다.

    배포가 안 바꾸면 Grafana 가 `${EMR_APPLICATION_ID}` 를 문자열 그대로 질의에
    넣습니다. 대시보드는 정상으로 뜨고 **패널만 비어서**, 사람이 열어보기 전까지
    아무도 모릅니다 — 실제로 13곳이 치환되지 않은 채 배포됐습니다.
    """
    used = set()
    for path in _provisioning_files():
        used |= set(PLACEHOLDER.findall(path.read_text(encoding="utf-8")))

    workflow = WORKFLOW.read_text()
    for name in sorted(used):
        assert f'"{name}"' in workflow or f"{name}:" in workflow, (
            f"{name} 을 쓰는데 배포가 값을 넘기지 않습니다"
        )


def test_치환하지_못하면_배포가_멈춘다():
    """남은 채로 보내면 배포는 초록불이고 패널만 빕니다 — 가장 드러나지 않는 실패입니다."""
    workflow = WORKFLOW.read_text()

    assert "치환되지 않은 플레이스홀더" in workflow
    assert "raise SystemExit" in workflow


def test_치환_대상은_배포가_아는_이름뿐이다():
    """배포가 모르는 이름을 새로 쓰면 위 검사가 잡습니다. 목록을 여기에 고정해
    무엇이 필요한지 한눈에 보이게 합니다.
    """
    used = set()
    for path in _provisioning_files():
        used |= set(PLACEHOLDER.findall(path.read_text(encoding="utf-8")))

    assert used == {"EMR_APPLICATION_ID"}, f"새 플레이스홀더: {used}"


def test_용량_알림의_창이_for_보다_짧다():
    """창과 `for` 가 겹치면 알림이 늦습니다.

    `reduce(max)` 는 창 안의 최댓값을 보므로 값이 이미 내려가도 조건이 참으로 남고,
    `for` 가 그 "번진 값" 위에서 다시 셉니다. 실측에서 12:27~12:45 에 19분 포화였는데
    알림은 13:01 에 왔습니다 — 상황이 끝나고 16분 뒤입니다.
    """
    for group in yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            if "용량" not in rule["title"]:
                continue
            for_seconds = int(rule["for"].rstrip("m")) * 60
            for query in rule["data"]:
                window = query["relativeTimeRange"]["from"]
                assert window < for_seconds, (
                    f"{rule['title']}: 창 {window}초 >= for {for_seconds}초"
                )
                if query["model"].get("type") == "reduce":
                    # max 는 지나간 스파이크를 계속 참으로 만듭니다.
                    assert query["model"]["reducer"] == "last", rule["title"]


def test_슬랙_템플릿이_없는_라벨을_비워두지_않는다():
    """`instance_name` 은 호스트 알림에만 있습니다. 조건 없이 쓰면 EMR·Lambda 알림이
    `**` 만 찍고 나갑니다 — 실제로 그렇게 나갔습니다.
    """
    text = (ALERTING / "contact-points.yaml").read_text()

    assert "{{ with .Labels.instance_name }}" in text


def test_용량_알림의_근거가_대시보드에_보인다():
    """알림은 `CPUAllocated`(할당)를 보는데 패널이 `WorkerCpuUsed`(사용)만 그리면,
    알림이 와도 화면에 근거가 없어 사람이 오탐으로 판단합니다 — 실제로 그랬습니다.

    알림이 보는 지표를 같은 패널에 그려 둡니다.
    """
    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())
    panels = {p["title"]: p for p in dashboard["panels"]}

    rules = yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())
    alerted = {
        q["model"]["metricName"]
        for g in rules["groups"] for r in g["rules"]
        for q in r["data"]
        if "용량" in r["title"] and q["model"].get("metricName")
    }

    drawn = {
        t.get("metricName")
        for title, p in panels.items() if "사용" in title
        for t in p["targets"]
    }
    assert alerted <= drawn, f"알림이 보는데 안 그리는 지표: {alerted - drawn}"


def test_용량_알림은_상한_도달로_판정한다():
    """할당이 상한에 닿으면 새 워커가 못 뜹니다 — 그 순간이 대기가 시작되는 시점입니다."""
    for group in yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())["groups"]:
        for rule in group["rules"]:
            if "용량" not in rule["title"]:
                continue
            math = next(q for q in rule["data"] if q["model"].get("type") == "math")
            assert math["model"]["expression"] == "$C >= $D"


def test_작업_지표는_성격에_맞는_집계를_쓴다():
    """EMR 작업 지표는 성격이 둘로 갈립니다.

        RunningJobs / PendingJobs    순간 상태  — 지금 몇 개인지
        SuccessJobs / FailedJobs     이벤트 카운트 — 성공·실패할 때마다 1분간 1

    넷을 한 패널에 `lastNotNull` 로 두면 Success 가 거의 항상 0 으로 보입니다.
    실제로 12시간에 7건 성공했는데 화면에는 0 이었습니다.
    """
    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())
    stats = [p for p in dashboard["panels"] if p["type"] == "stat"]
    assert len(stats) >= 2, "순간 상태와 이벤트 카운트를 한 패널에 섞지 않습니다"

    EVENT = {"SuccessJobs", "FailedJobs"}
    STATE = {"RunningJobs", "PendingJobs"}
    for panel in stats:
        metrics = {t["metricName"] for t in panel["targets"]}
        calc = panel["options"]["reduceOptions"]["calcs"][0]
        if metrics <= EVENT:
            assert calc == "sum", f"{panel['title']}: 이벤트 카운트는 합계여야 합니다"
        elif metrics <= STATE:
            assert calc == "lastNotNull", f"{panel['title']}: 순간 상태는 마지막 값입니다"
        else:
            raise AssertionError(f"{panel['title']}: 성격이 다른 지표가 섞였습니다 {metrics}")

def test_할당_선을_기준선으로_오해하게_이름짓지_않는다():
    """'기준' 은 기준선(threshold)으로 읽힙니다.

    이 선은 기준이 아니라 **알림이 비교하는 값** 이고, 기준선은 '용량 상한' 입니다.
    실제로 "기준이라는 게 기준선 아니냐" 는 질문을 받았습니다.
    """
    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())

    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            label = target.get("label") or ""
            if "할당" in label:
                assert "기준" not in label, f"{panel['title']}: {label}"


def test_알림이_없는_패널을_알림이_있는_것처럼_적지_않는다():
    """용량 알림은 CPU 만 봅니다. 메모리 패널에 '알림 기준' 이라고 적어 두면
    없는 알림을 있는 것으로 읽게 됩니다.
    """
    rules = yaml.safe_load((ALERTING / "emr-rules.yaml").read_text())
    alerted = {
        q["model"]["metricName"]
        for g in rules["groups"] for r in g["rules"]
        for q in r["data"] if "용량" in r["title"] and q["model"].get("metricName")
    }
    assert "MemoryAllocated" not in alerted, "메모리 알림이 생겼으면 설명도 고치세요"

    dashboard = json.loads((DASHBOARDS / "emr.json").read_text())
    memory = next(p for p in dashboard["panels"] if p["title"] == "메모리 사용 (GB)")
    assert "알림이 없습니다" in memory["description"]
