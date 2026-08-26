# Lambda 실행 환경 (컨테이너 방식). 베이스가 런타임 버전을 고정합니다.
FROM public.ecr.aws/lambda/python:3.11

# ★ 빌드 컨텍스트는 저장소 루트입니다 (`make build` 참고). COPY 경로에 aws_lambda/ 가
#   붙는 이유이자, libs/pipeline_core 를 가져올 수 있는 이유입니다.
#   /tmp/libs 위치는 uv.lock 의 `directory = "../libs/pipeline_core"` 와 맞춘 것입니다.
COPY main/aws_lambda/pyproject.toml main/aws_lambda/uv.lock /tmp/main/aws_lambda/
COPY libs/pipeline_core /tmp/libs/pipeline_core

# uv.lock 을 그대로 풀어서 설치 — 팀원 노트북과 이미지가 같은 버전이 됩니다.
# pipeline-core 만 따로 빼서 설치합니다. requirements.txt 에 남겨두면 pip 이
# "디렉터리는 해시를 못 낸다"며 거부합니다 (해시 검증은 나머지 패키지에 그대로 유지).
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/main/aws_lambda && uv export --frozen --no-dev --no-emit-project \
      --no-emit-package pipeline-core -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/main/aws_lambda/requirements.txt -t ${LAMBDA_TASK_ROOT} \
 && pip install --no-cache-dir --no-deps /tmp/libs/pipeline_core -t ${LAMBDA_TASK_ROOT} \
 && rm -rf /tmp/main /tmp/libs

# 메인 제품 함수는 기존 배포 핸들러 경로를 유지합니다.
COPY main/aws_lambda/functions/ ${LAMBDA_TASK_ROOT}/functions/

# main 함수들은 handler 경로와 무관하게 `main.aws_lambda.common` 을 절대 import 합니다
# (예: eia_fuel_price_layout). 위에서 functions/ 만 접두사 없이 복사해서 이 패키지가
# 이미지에 없으면 콜드스타트 시점에 "No module named 'main'" 으로 죽습니다.
COPY main/__init__.py ${LAMBDA_TASK_ROOT}/main/__init__.py
COPY main/common/ ${LAMBDA_TASK_ROOT}/main/common/
COPY main/aws_lambda/common/ ${LAMBDA_TASK_ROOT}/main/aws_lambda/common/

# 동일한 런타임 의존성을 쓰는 원천 제품 함수와 공통 유틸은 제품 namespace 그대로
# 패키징합니다. 원천 핸들러 경로는
# sub.aws_lambda.functions.<이름>.handler.lambda_handler 입니다.
COPY sub/__init__.py ${LAMBDA_TASK_ROOT}/sub/__init__.py
COPY sub/aws_lambda/ ${LAMBDA_TASK_ROOT}/sub/aws_lambda/
COPY shared/__init__.py ${LAMBDA_TASK_ROOT}/shared/__init__.py
COPY shared/aws_lambda/ ${LAMBDA_TASK_ROOT}/shared/aws_lambda/
COPY shared/common/ ${LAMBDA_TASK_ROOT}/shared/common/

# main·sub 여러 함수가 저장소 루트의 schema.* (bronze/silver/source) 를 절대 import 합니다
# (로컬은 aws_lambda/pyproject.toml 의 pythonpath="../.." 로 풀림). 이미지에 안 넣으면
# 그 함수들만 "No module named 'schema'" 로 콜드스타트 시점에 죽습니다.
COPY schema/ ${LAMBDA_TASK_ROOT}/schema/

# vehicle_catalog_raw_to_bronze 가 pytesseract 로 OCR 호출을 하는데, 그건 파이썬
# 래퍼일 뿐이라 uv.lock 이 tesseract 바이너리 자체는 못 잡습니다. 베이스가 Amazon
# Linux 2 라 기본 yum 저장소엔 tesseract 가 없어 EPEL 을 추가로 켭니다(공식
# amazon-linux-extras 는 이 최소 이미지엔 없어 EPEL rpm 을 직접 설치).
RUN yum install -y https://archives.fedoraproject.org/pub/archive/epel/7/x86_64/Packages/e/epel-release-7-14.noarch.rpm \
 && yum install -y tesseract \
 && yum clean all \
 && rm -rf /var/cache/yum
