# =============================================================================
# Makefile  —  런타임 전체를 가로지르는 공용 명령어
# =============================================================================

RUNTIMES := airflow spark lambda
GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "nogit")
# 기본값은 비워둡니다 — 로컬에서 `make build` 하면 tlc-airflow:<sha> 로 태그됩니다.
# ECR 로 푸시할 때만 끝에 슬래시를 붙여 지정하세요:
#   make build REGISTRY=123456789012.dkr.ecr.ap-northeast-2.amazonaws.com/
REGISTRY ?=

.PHONY: help
help:
	@echo "lock         - 전 런타임 uv.lock 재생성"
	@echo "check        - 락파일 드리프트 검증"
	@echo "sync         - 전 런타임 로컬 환경 동기화 (팀원은 이거만 하면 됨)"
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

.PHONY: sync
sync: tesseract
	@for r in $(RUNTIMES); do \
		echo "==> syncing $$r"; (cd $$r && uv sync --frozen) || exit 1; \
	done

# uv.lock 은 파이썬 패키지만 고정합니다. tesseract 는 시스템 바이너리라
# lock 이 못 잡아서 여기서 챙깁니다. (lambda/functions/fasttrack_vehicle_pricing
# 이 렌탈 가격을 이미지에서 OCR 로 읽습니다.)
.PHONY: tesseract
tesseract:
	@if command -v tesseract >/dev/null 2>&1; then \
		echo "==> tesseract $$(tesseract --version 2>&1 | head -1 | awk '{print $$2}')"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "==> tesseract 설치 (brew)"; brew install tesseract; \
	elif command -v apt-get >/dev/null 2>&1; then \
		echo "==> tesseract 설치 (apt)"; sudo apt-get install -y tesseract-ocr; \
	else \
		echo "!! tesseract 를 직접 설치하세요 (5.x). docs/GETTING_STARTED.md 참고"; exit 1; \
	fi

.PHONY: build
build:
	@for r in $(RUNTIMES); do \
		echo "==> building $(REGISTRY)tlc-$$r:$(GIT_SHA)"; \
		docker build -t $(REGISTRY)tlc-$$r:$(GIT_SHA) $$r || exit 1; \
	done
