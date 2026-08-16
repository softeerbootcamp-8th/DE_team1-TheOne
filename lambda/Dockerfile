# Lambda 실행 환경 (컨테이너 방식). 베이스가 런타임 버전을 고정합니다.
FROM public.ecr.aws/lambda/python:3.11

# ★ 빌드 컨텍스트는 저장소 루트입니다 (`make build` 참고). COPY 경로에 lambda/ 가
#   붙는 이유이자, libs/pipeline_core 를 가져올 수 있는 이유입니다.
#   /tmp/libs 위치는 uv.lock 의 `directory = "../libs/pipeline_core"` 와 맞춘 것입니다.
COPY lambda/pyproject.toml lambda/uv.lock /tmp/build/
COPY libs/pipeline_core /tmp/libs/pipeline_core

# uv.lock 을 그대로 풀어서 설치 — 팀원 노트북과 이미지가 같은 버전이 됩니다.
# pipeline-core 만 따로 빼서 설치합니다. requirements.txt 에 남겨두면 pip 이
# "디렉터리는 해시를 못 낸다"며 거부합니다 (해시 검증은 나머지 패키지에 그대로 유지).
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project \
      --no-emit-package pipeline-core -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt -t ${LAMBDA_TASK_ROOT} \
 && pip install --no-cache-dir --no-deps /tmp/libs/pipeline_core -t ${LAMBDA_TASK_ROOT} \
 && rm -rf /tmp/build /tmp/libs

# 함수 15개가 이 이미지 하나를 공유합니다. CMD 를 여기서 고정하지 않고,
# Lambda 함수 생성/업데이트 시 --image-config Command=functions.<이름>.handler.lambda_handler
# 로 함수별로 오버라이드합니다 (이미지는 한 번만 빌드/푸시하면 됨).
COPY lambda/functions/ ${LAMBDA_TASK_ROOT}/functions/

# lyft/uber_eligible_vehicles, vehicle_catalog, hvfhv 는 저장소 루트의 schema.bronze.*
# 를 import 합니다(로컬은 lambda/pyproject.toml 의 pythonpath="../.." 로 풀림). 이미지에
# 안 넣으면 그 함수들만 "No module named 'schema'" 로 콜드스타트 시점에 죽습니다.
COPY schema/ ${LAMBDA_TASK_ROOT}/schema/

# ★ vehicle_catalog_raw_to_bronze 를 이 이미지로 배포한다면 tesseract 바이너리도 함께
#   설치해야 합니다. pytesseract 는 파이썬 래퍼일 뿐이라 uv.lock 이 못 잡고, 없으면
#   OCR 호출 시점에 TesseractNotFoundError 로 죽습니다. 지금은 이 함수가 airflow
#   이미지 안에서 돌아 문제가 없습니다 (airflow/Dockerfile 이 tesseract-ocr 를 깝니다).
