# Streamlit 대시보드 실행 환경.
FROM python:3.13-slim

WORKDIR /app

# uv.lock을 그대로 설치해 로컬/CI/Docker의 패키지 버전을 일치시킵니다.
# ★ 빌드 컨텍스트는 저장소 루트입니다 (`make build` 참고) — COPY 경로에 dashboard/ 가 붙습니다.
COPY main/dashboard/pyproject.toml main/dashboard/uv.lock /tmp/build/
RUN pip install --no-cache-dir "uv==0.12.2" \
 && (cd /tmp/build && uv export --frozen --no-dev --no-emit-project -o requirements.txt) \
 && pip install --no-cache-dir -r /tmp/build/requirements.txt \
 && rm -rf /tmp/build

# app.py/datasource.py 는 `import charts` 처럼 같은 디렉터리 상대 import 를 쓰고,
# datasource.py 는 `from schema.gold import ...` 로 저장소 루트의 schema/ 를 절대
# import 합니다 (main/dashboard/pyproject.toml 의 pythonpath=["../.."] 와 같은 모양).
# WORKDIR 를 dashboard 디렉터리로 두고 PYTHONPATH 로 그 부모(=/app)를 추가해 둘 다 만족시킵니다.
ENV PYTHONPATH=/app

COPY main/dashboard/app.py main/dashboard/charts.py main/dashboard/theme.py main/dashboard/datasource.py /app/main/dashboard/
COPY main/dashboard/.streamlit/ /app/main/dashboard/.streamlit/
COPY main/dashboard/assets/ /app/main/dashboard/assets/
COPY schema/ /app/schema/

WORKDIR /app/main/dashboard

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
