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
COPY pyproject.toml uv.lock /tmp/build/
RUN /usr/bin/python3.11 -m pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && /usr/bin/python3.11 -m uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && /usr/bin/python3.11 -m pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && rm -rf /tmp/build

USER hadoop

# Spark job 이 생기면 아래 한 줄 추가:
# COPY jobs/ /home/hadoop/jobs/
