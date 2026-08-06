"""환경 확인용 샘플 DAG.

`docker compose up -d` 후 UI(http://localhost:8080)에 이 DAG 가 보이면
Airflow ↔ Postgres ↔ DAG 폴더 마운트가 전부 정상이라는 뜻입니다.

확인했으면 지우세요.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_test",
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sample"],
):
    PythonOperator(
        task_id="say_hi",
        python_callable=lambda: print("hi from container"),
    )
