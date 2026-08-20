# 원천 API(server.py) 실행 환경. Lambda가 아니라 EC2에서 상시 구동되는
# 일반 파이썬 프로세스라, main/aws_lambda 이미지와 달리 Lambda 런타임 베이스가
# 필요 없습니다. 의존성도 boto3·python-dotenv뿐이라 가볍게 유지합니다.
FROM python:3.11-slim

WORKDIR /app

# 빌드 컨텍스트는 저장소 루트입니다 — COPY 경로에 sub/, shared/ 가 붙는 이유.
RUN pip install --no-cache-dir "boto3==1.43.65" "python-dotenv==1.2.1"

COPY sub/__init__.py sub/__init__.py
COPY sub/source_api/ sub/source_api/
COPY shared/__init__.py shared/__init__.py
COPY shared/aws_lambda/ shared/aws_lambda/

ENV SOURCE_API_ENV=prod
ENV SOURCE_API_HOST=0.0.0.0
ENV SOURCE_API_PORT=8091
EXPOSE 8091

CMD ["python", "-m", "sub.source_api.server"]
