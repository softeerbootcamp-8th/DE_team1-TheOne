# =============================================================================
# Makefile  —  런타임 전체를 가로지르는 공용 명령어
# =============================================================================

RUNTIMES := airflow spark lambda
GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "nogit")
ECR      ?= <account>.dkr.ecr.ap-northeast-2.amazonaws.com

# URL 을 여기 또 적으면 versions.toml 과 갈라지므로 거기서 읽어온다.
AIRFLOW_CONSTRAINTS_URL := $(shell sed -n 's/^airflow_constraints[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' versions.toml)

.PHONY: help
help:
	@echo "lock         - 전 런타임 uv.lock 재생성"
	@echo "check        - 락파일 드리프트 검증"
	@echo "sync         - 전 런타임 로컬 환경 동기화 (팀원은 이거만 하면 됨)"
	@echo "build        - 런타임별 Docker 이미지 빌드 (태그: <runtime>:<git-sha>)"
	@echo "airflow-constraints - Airflow constraints 파일 내려받아 vendoring"

.PHONY: lock
lock:
	@for r in $(RUNTIMES); do \
		echo "==> locking $$r"; (cd $$r && uv lock) || exit 1; \
	done

.PHONY: check
check:
	@for r in $(RUNTIMES); do \
		echo "==> checking $$r"; (cd $$r && uv lock --check) || exit 1; \
	done

.PHONY: sync
sync:
	@for r in $(RUNTIMES); do \
		echo "==> syncing $$r"; (cd $$r && uv sync --frozen) || exit 1; \
	done

.PHONY: build
build:
	@for r in $(RUNTIMES); do \
		echo "==> building $$r:$(GIT_SHA)"; \
		docker build -t $(ECR)/tlc-$$r:$(GIT_SHA) $$r || exit 1; \
	done

# Airflow constraints 는 (airflow, python) 조합으로 고정됩니다.
# 이 파일이 airflow/pyproject.toml 의 == 핀을 정할 때의 근거입니다.
# --fail 이 있어야 URL 이 틀렸을 때 에러 페이지가 저장되지 않고 실패합니다.
.PHONY: airflow-constraints
airflow-constraints:
	curl -sSL --fail "$(AIRFLOW_CONSTRAINTS_URL)" \
		-o airflow/constraints/constraints-3.3.0-py3.11.txt
	@echo "vendored -> airflow/constraints/constraints-3.3.0-py3.11.txt"
