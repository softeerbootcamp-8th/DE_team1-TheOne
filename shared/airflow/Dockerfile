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

# 필요한 것만 담습니다. 전에는 main/·sub/·shared/ 를 통째로 COPY 했는데, 그러면
# 이미지 입력이 사실상 저장소 전체가 되어 CI·배포 필터를 그만큼 넓혀야 하고,
# lambda 함수 코드만 고친 머지에도 airflow 가 재배포되며 실행 중인 DAG 이 끊깁니다.
# 아래 목록은 `.github/scripts/check_image_filters.py` 가 워크플로 필터와 대조합니다 —
# 여기를 늘리거나 줄이면 그 필터도 같이 고쳐야 CI 가 통과합니다.
#
# DAG 는 shared.airflow.common 과 main/sub 의 scripts 를 절대 import 하고, 그 아래로
# schema.*, sub.generators, importlib 로 main.aws_lambda.common·sub.aws_lambda.* 까지
# 끌어옵니다. 하나라도 빠지면 그 DAG 는 import 단계에서 죽습니다.
COPY --chown=airflow:root main/__init__.py /opt/airflow/project-root/main/__init__.py
COPY --chown=airflow:root main/airflow/ /opt/airflow/project-root/main/airflow/
COPY --chown=airflow:root sub/__init__.py /opt/airflow/project-root/sub/__init__.py
COPY --chown=airflow:root sub/airflow/ /opt/airflow/project-root/sub/airflow/
COPY --chown=airflow:root sub/generators/ /opt/airflow/project-root/sub/generators/
COPY --chown=airflow:root shared/__init__.py /opt/airflow/project-root/shared/__init__.py
COPY --chown=airflow:root shared/airflow/ /opt/airflow/project-root/shared/airflow/
COPY --chown=airflow:root shared/common/ /opt/airflow/project-root/shared/common/

# in-process 로 부르는 lambda 핸들러. DAG 가 importlib 로 직접 부릅니다 —
# main.aws_lambda.functions.{eia_*, lease_vehicle_inventory_*, driver_vehicle_monthly_*},
# sub.aws_lambda.functions.*. 그래서 functions/ 도 담아야 합니다.
#
# `main/aws_lambda` 에는 `__init__.py` 가 없습니다(암묵 namespace package). 없는 파일을
# COPY 하면 빌드가 죽으므로 적지 않습니다 — `main/__init__.py` 만 있으면 됩니다.
COPY --chown=airflow:root main/aws_lambda/common/ /opt/airflow/project-root/main/aws_lambda/common/
COPY --chown=airflow:root main/aws_lambda/functions/ /opt/airflow/project-root/main/aws_lambda/functions/
COPY --chown=airflow:root sub/aws_lambda/ /opt/airflow/project-root/sub/aws_lambda/
COPY --chown=airflow:root shared/aws_lambda/ /opt/airflow/project-root/shared/aws_lambda/

# BashOperator 가 실행하는 Spark job. 그 job 들이 shared.spark 와 sub/ 최상위 모듈
# (config·run_context·seeds)을 import 합니다.
COPY --chown=airflow:root main/spark/ /opt/airflow/project-root/main/spark/
COPY --chown=airflow:root sub/spark/ /opt/airflow/project-root/sub/spark/
COPY --chown=airflow:root shared/spark/ /opt/airflow/project-root/shared/spark/
COPY --chown=airflow:root sub/config.py sub/run_context.py sub/seeds.py /opt/airflow/project-root/sub/

COPY --chown=airflow:root schema/ /opt/airflow/project-root/schema/
COPY --chown=airflow:root libs/ /opt/airflow/project-root/libs/

# 생성 파라미터의 값 소유자입니다 (`sub/config.py` 의 DEFAULT_CONFIG_PATH 가 여기를
# 가리킵니다). BashOperator 로 도는 sub/spark/jobs/.../source_job.py 가 load_config()
# 를 부르므로, 없으면 그 task 가 "설정 파일이 없습니다" 로 죽습니다.
COPY --chown=airflow:root config/ /opt/airflow/project-root/config/
