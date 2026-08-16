"""로컬과 Docker에서 공유하는 프로젝트 경로를 설정합니다."""

import sys
from pathlib import Path


AIRFLOW_DIR = Path(__file__).resolve().parents[2]
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
