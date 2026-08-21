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
COPY --chown=airflow:root main/airflow/pyproject.toml main/airflow/uv.lock /tmp/build/
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && rm -rf /tmp/build

# 여기부터는 이미지 하나로 EC2 에서 뜨게 하기 위한 소스 복사입니다. 의존성 설치 뒤에
# 두어야 소스만 바뀌었을 때 위 레이어 캐시가 살아남습니다.
# 로컬 compose 는 아래 세 경로를 전부 바인드 마운트로 덮으므로 개발 흐름은 그대로입니다.
ENV PYTHONPATH=/opt/airflow/project-root

COPY --chown=airflow:root main/airflow/dags/ /opt/airflow/dags/main/
COPY --chown=airflow:root sub/airflow/dags/ /opt/airflow/dags/sub/

# DAG 는 shared.airflow.common 과 main/sub 의 scripts 를 절대 import 하고, 그 아래로
# schema.*, sub.generators, importlib 로 sub.aws_lambda.functions.*, 그리고
# project_paths.py 가 sys.path 에 넣는 libs/pipeline_core 까지 끌어옵니다.
# 하나라도 빠지면 그 DAG 는 import 단계에서 죽습니다.
COPY --chown=airflow:root main/ /opt/airflow/project-root/main/
COPY --chown=airflow:root sub/ /opt/airflow/project-root/sub/
COPY --chown=airflow:root shared/ /opt/airflow/project-root/shared/
COPY --chown=airflow:root schema/ /opt/airflow/project-root/schema/
COPY --chown=airflow:root libs/ /opt/airflow/project-root/libs/
