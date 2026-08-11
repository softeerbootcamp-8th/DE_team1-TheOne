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

# 핸들러가 생기면 아래 두 줄 추가:
# COPY functions/ ${LAMBDA_TASK_ROOT}/functions/
# CMD ["functions.<이름>.handler.lambda_handler"]
