"""Airflow DAG 상태 테스트용 격리 메타데이터 DB."""

import os
from pathlib import Path
import tempfile


_AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
_TEST_HOME = tempfile.TemporaryDirectory(prefix="tlc-airflow-test-")

os.environ["AIRFLOW_HOME"] = _TEST_HOME.name
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(_AIRFLOW_ROOT / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
    f"sqlite:///{Path(_TEST_HOME.name) / 'airflow.db'}"
)
# 데이터셋별 단위 테스트는 GX 규칙만 검증합니다. Data Docs 실발행은
# test_common_validation.py가 tmp_path에서 별도로 검증합니다.
os.environ["GX_DATA_DOCS_ENABLED"] = "false"


def pytest_sessionstart(session):
    """통합 테스트가 사용할 Airflow 메타데이터 테이블을 생성합니다."""
    from airflow.utils import db

    db.initdb()


def pytest_sessionfinish(session, exitstatus):
    from airflow import settings

    settings.dispose_orm()
    _TEST_HOME.cleanup()
