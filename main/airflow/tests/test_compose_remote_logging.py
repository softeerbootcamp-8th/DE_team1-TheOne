"""운영 Airflow 태스크 로그의 S3 원격 보존 계약.

1. 운영 compose 는 원격 로깅을 켜고 데이터 계층 옆 `logs/` 접두사에 쓴다
2. 업로드 객체마다 서버 측 암호화를 명시한다
3. 커넥션을 만들지 않고 인스턴스 role 로 쓴다 (`remote_log_conn_id` 미지정)
4. 로컬 compose 는 원격 로깅을 켜지 않는다 — 자격증명 없이 떠야 하므로
5. 원격 key 가 DAG·Run·Task·시도까지 갈라져 재시도 로그를 덮어쓰지 않는다
"""

from pathlib import Path

import yaml


_REPO_ROOT = Path(__file__).resolve().parents[3]
_DATA_LAYER_PREFIXES = ("source", "bronze", "silver")


def _airflow_env(compose_file: str) -> dict[str, str]:
    compose = yaml.safe_load((_REPO_ROOT / compose_file).read_text(encoding="utf-8"))
    return compose["services"]["airflow"]["environment"]


def test_운영_compose_는_태스크_로그를_S3_에_보존한다():
    env = _airflow_env("docker-compose.ec2.yml")

    assert env["AIRFLOW__LOGGING__REMOTE_LOGGING"] == "true"


def test_로그_접두사는_데이터_계층과_같은_층의_logs_아래다():
    """`logs/` 는 source·bronze·silver 와 형제입니다.

    데이터 접두사 안으로 들어가면 레이크 스캔·Lifecycle 규칙이 로그까지 함께 걸립니다.
    """
    env = _airflow_env("docker-compose.ec2.yml")
    folder = env["AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER"]

    bucket, _, key = folder.removeprefix("s3://").partition("/")

    assert folder.startswith("s3://")
    assert bucket == "${DATA_LAKE_S3_BUCKET}"
    assert str(env["DATA_LAKE_S3_BUCKET"]).startswith("${DATA_LAKE_S3_BUCKET:?")
    assert key.split("/")[0] == "logs"
    assert key.split("/")[0] not in _DATA_LAYER_PREFIXES


def test_업로드_객체마다_서버측_암호화를_명시한다():
    """버킷 기본 암호화가 바뀌어도 로그는 암호화된 채 올라가야 합니다."""
    env = _airflow_env("docker-compose.ec2.yml")

    assert env["AIRFLOW__LOGGING__ENCRYPT_S3_LOGS"] == "true"


def test_로그_업로드는_커넥션_없이_인스턴스_role_을_쓴다():
    """`remote_log_conn_id` 가 비어야 S3Hook 이 boto 기본 체인으로 떨어집니다.

    값을 채우면 그 이름의 Connection 이 Postgres 볼륨에 있어야 하고, 볼륨이 날아가면
    로그만 조용히 안 올라갑니다. 이미 RAW_STORAGE=s3 가 같은 role 로 쓰고 있습니다.
    """
    env = _airflow_env("docker-compose.ec2.yml")

    assert "AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID" not in env


def test_로컬_compose_는_원격_로깅을_켜지_않는다():
    """로컬은 AWS 자격증명 없이 떠야 합니다."""
    env = _airflow_env("docker-compose.yml")

    assert not [key for key in env if key.startswith("AIRFLOW__LOGGING__REMOTE_")]


def test_원격_key_가_DAG_Run_Task_시도까지_갈라진다():
    """원격 key = base prefix + 로컬 상대 경로라, 이 템플릿이 곧 S3 key 구조입니다.

    시도(`attempt`)가 빠지면 재시도가 같은 key 에 덮어써서 첫 실패 로그를 잃습니다.
    Airflow 기본값이지만 업그레이드로 바뀌면 조용히 유실되므로 여기서 고정합니다.
    """
    from airflow.configuration import conf

    assert "AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE" not in _airflow_env(
        "docker-compose.ec2.yml"
    )

    template = conf.get("logging", "log_filename_template")

    for key in ("dag_id=", "run_id=", "task_id=", "attempt="):
        assert key in template
