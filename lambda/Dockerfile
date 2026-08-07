# Lambda 실행 환경 (컨테이너 방식). 베이스가 런타임 버전을 고정합니다.
FROM public.ecr.aws/lambda/python:3.11

# uv.lock 을 그대로 풀어서 설치 — 팀원 노트북과 이미지가 같은 버전이 됩니다.
COPY pyproject.toml uv.lock /tmp/build/
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt -t ${LAMBDA_TASK_ROOT} \
 && rm -rf /tmp/build

# 핸들러가 생기면 아래 두 줄 추가:
# COPY functions/ ${LAMBDA_TASK_ROOT}/functions/
# CMD ["functions.<이름>.handler.lambda_handler"]
