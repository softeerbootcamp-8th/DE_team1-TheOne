# EMR Serverless 실행 환경.
# 베이스는 운영 EMR release label과 반드시 일치시킬 것.
# ★ :latest 는 떠다니는 태그입니다. 운영 배포 전에 digest(@sha256:...)로 고정하세요.
FROM public.ecr.aws/emr-serverless/spark/emr-7.13.0:latest

USER root

# EMR 이 제공하지 않는 라이브러리만 설치 (pyspark 는 EMR 제공분을 씀).
# uv.lock 을 그대로 풀어서 설치 — 팀원 노트북과 이미지가 같은 버전이 됩니다.
#
# ★ 반드시 python3.11 로 설치해야 합니다.
#   이 이미지의 기본 python/pip 은 3.9 인데, Spark 는 3.11 을 씁니다
#   (spark-env: PYSPARK_PYTHON=${PYSPARK_PYTHON:-/usr/bin/python3.11}).
#   그냥 `pip install` 하면 Spark 가 안 보는 3.9 site-packages 에 깔립니다.
# ★ 빌드 컨텍스트는 저장소 루트입니다 (`make build` 참고). COPY 경로에 spark/ 가
#   붙는 이유이자, libs/pipeline_core 를 가져올 수 있는 이유입니다.
#   /tmp/libs 위치는 uv.lock 의 `directory = "../libs/pipeline_core"` 와 맞춘 것입니다.
COPY spark/pyproject.toml spark/uv.lock /tmp/build/
COPY libs/pipeline_core /tmp/libs/pipeline_core

# pipeline-core 만 따로 빼서 설치합니다. requirements.txt 에 남겨두면 pip 이
# "디렉터리는 해시를 못 낸다"며 거부합니다 (해시 검증은 나머지 패키지에 그대로 유지).
RUN /usr/bin/python3.11 -m pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && /usr/bin/python3.11 -m uv export --frozen --no-dev --no-emit-project \
      --no-emit-package pipeline-core -o requirements.txt) \
 && /usr/bin/python3.11 -m pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && /usr/bin/python3.11 -m pip install --no-cache-dir --no-deps /tmp/libs/pipeline_core \
 && rm -rf /tmp/build /tmp/libs

USER hadoop

# bronze_to_silver/hvfhv 등이 저장소 루트의 schema.silver.*/schema.bronze.* 를
# import 합니다(로컬은 spark/pyproject.toml 의 pythonpath="../.." 로 풀림). jobs/
# 와 나란히 두어야 import 경로가 로컬 테스트와 같아집니다.
COPY schema/ /home/hadoop/schema/

# Spark job 이 생기면 아래 한 줄 추가:
# COPY jobs/ /home/hadoop/jobs/
