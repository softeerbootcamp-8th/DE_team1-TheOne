# Airflow 실행 환경. 공식 이미지 태그에 airflow/python 버전이 각인되어 있습니다.
FROM apache/airflow:3.3.0-python3.11

# PySpark 로컬 마스터 실행을 위한 자바 런타임(default-jre-headless)과
# 렌탈 차량 대장 수집용 OCR 엔진(tesseract-ocr) 설치.
# 가격이 업체 카드 이미지 안에만 있어 pytesseract 가 이 바이너리를 호출합니다.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless tesseract-ocr \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# uv.lock을 그대로 설치해 로컬/CI/Docker의 패키지 버전을 일치시킵니다.
# ★ 빌드 컨텍스트는 저장소 루트입니다 (`make build` 참고) — COPY 경로에 airflow/ 가 붙습니다.
COPY --chown=airflow:root airflow/pyproject.toml airflow/uv.lock /tmp/build/
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && rm -rf /tmp/build

# DAG 가 생기면 아래 한 줄 추가 (컨텍스트가 저장소 루트라 airflow/ 가 붙습니다):
# COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
