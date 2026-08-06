# Airflow 실행 환경. 공식 이미지 태그에 airflow/python 버전이 각인되어 있습니다.
# (versions.toml 의 airflow / python 과 일치시킬 것)
FROM apache/airflow:3.3.0-python3.11

# constraints 로 고정된 provider 설치 — pyproject.toml 의 == 핀과 같은 조합입니다.
COPY --chown=airflow:root constraints/constraints-3.3.0-py3.11.txt /tmp/constraints.txt

RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon" \
    "apache-airflow-providers-postgres" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-http" \
    "beautifulsoup4" "lxml" \
    --constraint /tmp/constraints.txt

# DAG 가 생기면 아래 한 줄 추가:
# COPY --chown=airflow:root dags/ /opt/airflow/dags/
