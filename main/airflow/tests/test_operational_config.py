"""운영 설정값 주입 계약.

1. 운영 Compose는 이미지·Source API·S3·DB 비밀번호를 필수로 받는다
2. 데이터 버킷과 원격 로그가 같은 설정값을 사용한다
3. 배포 워크플로가 GitHub Variables와 Secrets를 EC2 .env로 전달한다
4. 로컬 Compose는 별도 설정 없이 기존 기본값으로 실행할 수 있다
"""

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
PROD_COMPOSE = ROOT / "docker-compose.ec2.yml"
LOCAL_COMPOSE = ROOT / "docker-compose.yml"
DEPLOY_WORKFLOW = ROOT / ".github/workflows/deploy-airflow.yml"


def _compose(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_운영_compose는_운영_설정값을_필수로_받는다():
    compose = _compose(PROD_COMPOSE)
    airflow = compose["services"]["airflow"]
    postgres_env = compose["services"]["postgres"]["environment"]
    airflow_env = airflow["environment"]

    assert str(airflow["image"]).startswith("${AIRFLOW_IMAGE:?")
    for name in ("SOURCE_API_URL", "DATA_LAKE_S3_BUCKET"):
        assert str(airflow_env[name]).startswith(f"${{{name}:?")
    assert str(postgres_env["POSTGRES_PASSWORD"]).startswith(
        "${AIRFLOW_DB_PASSWORD:?"
    )
    assert "${AIRFLOW_DB_PASSWORD:?" in airflow_env[
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    ]


def test_데이터와_로그는_같은_S3_버킷_설정값을_사용한다():
    airflow_env = _compose(PROD_COMPOSE)["services"]["airflow"]["environment"]

    assert str(airflow_env["DATA_LAKE_S3_BUCKET"]).startswith(
        "${DATA_LAKE_S3_BUCKET:?"
    )
    assert airflow_env["AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER"] == (
        "s3://${DATA_LAKE_S3_BUCKET}/logs/airflow"
    )


def test_배포_워크플로가_운영_설정을_EC2_env로_전달한다():
    workflow = DEPLOY_WORKFLOW.read_text(encoding="utf-8")
    mappings = {
        "SOURCE_API_URL": "vars.SOURCE_API_URL",
        "DATA_LAKE_S3_BUCKET": "vars.S3_BUCKET",
        "AIRFLOW_DB_PASSWORD": "secrets.AIRFLOW_DB_PASSWORD",
    }

    for name, source in mappings.items():
        assert f"{name}: ${{{{ {source} }}}}" in workflow
        assert f"echo {name}=" in workflow


def test_로컬_compose는_운영_설정_없이_실행할_수_있다():
    compose = _compose(LOCAL_COMPOSE)
    postgres_env = compose["services"]["postgres"]["environment"]
    airflow_env = compose["services"]["airflow"]["environment"]

    assert str(postgres_env["POSTGRES_PASSWORD"]).startswith(
        "${AIRFLOW_DB_PASSWORD:-"
    )
    assert "${AIRFLOW_DB_PASSWORD:-" in airflow_env[
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    ]
    assert str(airflow_env["SOURCE_API_URL"]).startswith("${SOURCE_API_URL:-")
