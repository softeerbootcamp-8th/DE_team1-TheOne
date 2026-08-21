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
COPY main/spark/pyproject.toml main/spark/uv.lock /tmp/main/spark/
COPY libs/pipeline_core /tmp/libs/pipeline_core

# pipeline-core 만 따로 빼서 설치합니다. requirements.txt 에 남겨두면 pip 이
# "디렉터리는 해시를 못 낸다"며 거부합니다 (해시 검증은 나머지 패키지에 그대로 유지).
RUN /usr/bin/python3.11 -m pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/main/spark && /usr/bin/python3.11 -m uv export --frozen --no-dev --no-emit-project \
      --no-emit-package pipeline-core -o requirements.txt) \
 && /usr/bin/python3.11 -m pip install --no-cache-dir -r /tmp/main/spark/requirements.txt \
 && /usr/bin/python3.11 -m pip install --no-cache-dir --no-deps /tmp/libs/pipeline_core \
 && rm -rf /tmp/main /tmp/libs

USER hadoop

# job 코드는 저장소 루트 기준 경로 그대로 넣습니다 — import 문(main.spark.jobs.*,
# shared.spark.*, schema.*)이 로컬 테스트(spark/pyproject.toml 의 pythonpath="../..")와
# 같은 경로를 봐야 하기 때문입니다.
#
# sub/spark 도 같이 넣습니다 — sub/spark 는 자체 pyproject.toml/uv.lock 이 없이 이
# main/spark 환경을 그대로 쓰므로(aws_lambda 이미지에 sub/aws_lambda 를 합친 것과
# 같은 이유), 이미지도 나누지 않고 하나로 둡니다.
COPY schema/ /home/hadoop/schema/
COPY main/__init__.py /home/hadoop/main/__init__.py
COPY main/spark/__init__.py /home/hadoop/main/spark/__init__.py
COPY main/spark/jobs/ /home/hadoop/main/spark/jobs/
COPY shared/__init__.py /home/hadoop/shared/__init__.py
COPY shared/spark/ /home/hadoop/shared/spark/
COPY sub/__init__.py /home/hadoop/sub/__init__.py
COPY sub/spark/ /home/hadoop/sub/spark/
COPY sub/generators/ /home/hadoop/sub/generators/

# bronze_to_silver 가 S3 파티션 목록 조회에만 shared/aws_lambda 의 list_keys 를 재사용합니다
# (전용 파일 하나만 — common/ 전체를 넣으면 spark 에 없는 의존성을 끌고 옵니다).
COPY shared/aws_lambda/__init__.py /home/hadoop/shared/aws_lambda/__init__.py
COPY shared/aws_lambda/common/s3_reader.py /home/hadoop/shared/aws_lambda/common/s3_reader.py
