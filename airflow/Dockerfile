# Airflow 실행 환경. 공식 이미지 태그에 airflow/python 버전이 각인되어 있습니다.
FROM apache/airflow:3.3.0-python3.11

# PySpark 로컬 마스터 실행을 위한 자바 런타임(default-jre-headless) 설치
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# uv.lock을 그대로 설치해 로컬/CI/Docker의 패키지 버전을 일치시킵니다.
COPY --chown=airflow:root pyproject.toml uv.lock /tmp/build/
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && rm -rf /tmp/build

# DAG 가 생기면 아래 한 줄 추가:
# COPY --chown=airflow:root dags/ /opt/airflow/dags/
