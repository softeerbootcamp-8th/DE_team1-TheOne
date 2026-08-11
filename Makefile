# =============================================================================
# Makefile  —  런타임 전체를 가로지르는 공용 명령어
# =============================================================================

RUNTIMES := airflow spark lambda
GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "nogit")
# 기본값은 비워둡니다 — 로컬에서 `make build` 하면 tlc-airflow:<sha> 로 태그됩니다.
# ECR 로 푸시할 때만 끝에 슬래시를 붙여 지정하세요:
#   make build REGISTRY=123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/
REGISTRY ?=
# Lambda/EMR 기본 아키텍처. 고정하지 않으면 Apple Silicon 팀원은 arm64 이미지를 만들고,
# 그건 x86_64 Lambda 에서 "exec format error" 로 죽습니다. Graviton 으로 갈 때만 바꾸세요.
PLATFORM ?= linux/amd64
# 레이어 캐시. 기본값(빈 값)은 캐시를 쓰지 않습니다 — 로컬은 도커가 알아서 캐시합니다.
# CI 는 매번 새 러너라 캐시가 없어서 `make build DOCKER_CACHE=gha` 로 켭니다.
# type=gha 는 buildx 의 docker-container 드라이버에서만 되고, 그 드라이버는 결과를
# 빌더 안에 둡니다 (워크플로의 setup-buildx-action install:true 가 그걸 깔아 줍니다).
#   scope 를 런타임별로 나누는 이유: 한 scope 를 세 이미지가 공유하면 서로 덮어씁니다.
#   --load 는 빌더 → 도커 스토어로 이미지를 한 벌 더 복사합니다. 러너 디스크가
#   14GB 뿐이라 셋 다 내리면 spark(EMR 베이스, 수 GB)에서 넘칩니다. 그래서 뒤이어
#   `docker run` 하는 airflow 만 내리고, 나머지는 빌드만 확인하고 버립니다(cacheonly).
DOCKER_CACHE ?=
LOADED_RUNTIME ?= airflow
ifeq ($(DOCKER_CACHE),gha)
CACHE_FLAGS = --cache-from type=gha,scope=$$r --cache-to type=gha,mode=max,scope=$$r \
	$$([ "$$r" = "$(LOADED_RUNTIME)" ] && echo --load || echo --output=type=cacheonly)
endif

# uv 설치 스크립트는 ~/.local/bin 에 바이너리를 놓습니다. 이 make 실행 안에서
# 바로 이어서 `uv` 를 호출해도 찾을 수 있도록 PATH 에 미리 넣어둡니다.
export PATH := $(HOME)/.local/bin:$(PATH)

.PHONY: help
help:
	@echo "lock         - 전 런타임 uv.lock 재생성"
	@echo "check        - 락파일 드리프트 검증"
	@echo "sync         - 전 런타임 로컬 환경 동기화 (팀원은 이거만 하면 됨)"
	@echo "test         - 전 프로젝트 pytest (활성화된 venv 와 무관하게 각자 것으로 실행)"
	@echo "uv-bin       - uv 설치 확인/설치 (sync 가 먼저 호출)"
	@echo "tesseract    - OCR 바이너리 확인/설치 (sync 가 먼저 호출)"
	@echo "build        - 런타임별 Docker 이미지 빌드 (태그: <runtime>:<git-sha>)"

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

# 프로젝트마다 venv 가 따로라, 어느 걸 활성화해뒀는지에 따라 pytest 결과가 달라집니다.
# 예를 들어 airflow venv 를 켠 채 lambda 테스트를 돌리면 pipeline_core 가 없어
# collection 단계에서 전부 죽습니다 (airflow 는 pipeline_core 를 선언하지 않음).
# 그래서 VIRTUAL_ENV 를 지우고 각 프로젝트의 uv 환경으로 실행합니다.
# tests 폴더가 없는 런타임은 건너뜁니다 — 생기면 자동으로 포함됩니다.
.PHONY: test
test:
	@for p in $(RUNTIMES) libs/pipeline_core; do \
		if [ ! -d "$$p/tests" ]; then echo "==> skip $$p (tests 없음)"; continue; fi; \
		echo "==> testing $$p"; \
		(cd $$p && env -u VIRTUAL_ENV uv run --frozen pytest -q) || exit 1; \
	done

.PHONY: sync
sync: uv-bin tesseract
	@for r in $(RUNTIMES); do \
		echo "==> syncing $$r"; (cd $$r && uv sync --frozen) || exit 1; \
	done

# uv 는 파이썬 패키지가 아니라 시스템 바이너리라 uv.lock 이 못 잡습니다.
# 없으면 curl 설치 스크립트로 깝니다 (sudo 불필요).
.PHONY: uv-bin
uv-bin:
	@if command -v uv >/dev/null 2>&1; then \
		echo "==> uv $$(uv --version)"; \
	else \
		echo "==> uv 설치 (curl)"; curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi

# uv.lock 은 파이썬 패키지만 고정합니다. tesseract 는 시스템 바이너리라
# lock 이 못 잡아서 여기서 챙깁니다. (lambda/functions/vehicle_catalog_raw_to_bronze
# 이 렌탈 가격을 이미지에서 OCR 로 읽습니다.)
.PHONY: tesseract
tesseract:
	@if command -v tesseract >/dev/null 2>&1; then \
		echo "==> tesseract $$(tesseract --version 2>&1 | head -1 | awk '{print $$2}')"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "==> tesseract 설치 (brew)"; brew install tesseract; \
	elif command -v apt-get >/dev/null 2>&1; then \
		echo "==> tesseract 설치 (apt)"; sudo apt-get update && sudo apt-get install -y tesseract-ocr; \
	else \
		echo "!! tesseract 를 직접 설치하세요 (5.x). docs/GETTING_STARTED.md 참고"; exit 1; \
	fi

# 컨텍스트는 저장소 루트입니다. lambda/spark 가 libs/pipeline_core 를 COPY 해야 하는데
# 도커는 컨텍스트 밖을 참조할 수 없어서입니다. 전송량은 .dockerignore 가 잡습니다.
.PHONY: build
build:
	@for r in $(RUNTIMES); do \
		echo "==> building $(REGISTRY)tlc-$$r:$(GIT_SHA)"; \
		docker build --platform $(PLATFORM) $(CACHE_FLAGS) -f $$r/Dockerfile \
			-t $(REGISTRY)tlc-$$r:$(GIT_SHA) . || exit 1; \
	done
