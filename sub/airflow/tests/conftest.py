"""원천 DB Airflow DAG 테스트용 격리 메타데이터 DB."""

import os
from pathlib import Path
import sys
import tempfile


_DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
_AIRFLOW_ROOT = _DAGS_ROOT.parent
for path in (_AIRFLOW_ROOT, _DAGS_ROOT):
    path_text = str(path)
    if path_text in sys.path:
        sys.path.remove(path_text)
    sys.path.insert(0, path_text)
_TEST_HOME = tempfile.TemporaryDirectory(prefix="source-db-airflow-test-")

os.environ["AIRFLOW_HOME"] = _TEST_HOME.name
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(_DAGS_ROOT)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
    f"sqlite:///{Path(_TEST_HOME.name) / 'airflow.db'}"
)
os.environ["GX_DATA_DOCS_ENABLED"] = "false"


def pytest_sessionstart(session):
    from airflow.utils import db

    db.initdb()


def pytest_sessionfinish(session, exitstatus):
    from airflow import settings

    settings.dispose_orm()
    _TEST_HOME.cleanup()
