# =============================================================================
# Makefile  —  런타임 전체를 가로지르는 공용 명령어
# =============================================================================

RUNTIMES := main/airflow main/spark main/aws_lambda
# uv 프로젝트 전체(Docker 이미지 유무와 무관) — lock/check/test 가 순회합니다.
# build·sync 는 Docker 이미지가 있는 RUNTIMES 만 그대로 순회합니다.
UV_PROJECTS := $(RUNTIMES) main/dashboard
GIT_SHA  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "nogit")
# 기본값은 비워둡니다 — 로컬에서 `make build` 하면 theone-airflow:<sha> 로 태그됩니다.
# ECR 로 푸시할 때만 끝에 슬래시를 붙여 지정하세요:
#   make build REGISTRY=572660899671.dkr.ecr.ap-northeast-2.amazonaws.com/
REGISTRY ?=
# 이미지 이름 접두사. ECR 리포지토리 이름과 **같아야** 합니다 — 전에는 `tlc-` 였는데
# 실제 ECR 은 `theone-airflow` 라서, 배포할 때마다 사람이 손으로 다시 태그해야 했습니다.
# 자동 배포에서는 그 한 단계가 들어갈 자리가 없어 이름을 맞춥니다.
IMAGE_PREFIX ?= theone-
# 이름을 통째로 지정할 때 씁니다. ECR 리포지토리 이름이 접두사+런타임 규칙과 다를 때
# (예: aws_lambda 는 `theone-main-lambda`) 규칙을 비틀지 않고 그 이름을 그대로 넘깁니다.
# 배포 워크플로가 ECR_REPOSITORY 변수를 여기에 넘겨서, 리포지토리 이름이 단일 원천이
# 됩니다 — 이름이 바뀌어도 Makefile 을 고칠 필요가 없습니다. 런타임 하나에만 씁니다.
IMAGE_NAME ?=
# Lambda/EMR 기본 아키텍처. 고정하지 않으면 Apple Silicon 팀원은 arm64 이미지를 만들고,
# 그건 x86_64 Lambda 에서 "exec format error" 로 죽습니다. Graviton 으로 갈 때만 바꾸세요.
PLATFORM ?= linux/amd64

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
	@echo "build        - 런타임별 Docker 이미지 빌드 (태그: theone-<runtime>:<git-sha>)"
	@echo "setup-hooks  - review-engineering 검토 기록 Git 훅 설치"
	@echo "bootstrap    - DAG 가 없는 로컬 파생 산출물 4개 생성 (있으면 건너뜀)"
	@echo "               개별: zone-lookup / travel-times / driver-preferences / company-snapshot"
	@echo "               FORCE=1 을 붙이면 이미 있어도 다시 만듭니다 (스키마가 바뀐 뒤)"

.PHONY: lock
lock:
	@for r in $(UV_PROJECTS); do \
		echo "==> locking $$r"; (cd $$r && uv lock) || exit 1; \
	done

.PHONY: check
check:
	@for r in $(UV_PROJECTS); do \
		echo "==> checking $$r"; (cd $$r && uv lock --check) || exit 1; \
	done

# 프로젝트마다 venv 가 따로라, 어느 걸 활성화해뒀는지에 따라 pytest 결과가 달라집니다.
# 예를 들어 airflow venv 를 켠 채 lambda 테스트를 돌리면 pipeline_core 가 없어
# collection 단계에서 전부 죽습니다 (airflow 는 pipeline_core 를 선언하지 않음).
# 그래서 VIRTUAL_ENV 를 지우고 각 프로젝트의 uv 환경으로 실행합니다.
# tests 폴더가 없는 런타임은 건너뜁니다 — 생기면 자동으로 포함됩니다.
.PHONY: test
test:
	@for p in $(UV_PROJECTS) libs/pipeline_core; do \
		if [ ! -d "$$p/tests" ]; then echo "==> skip $$p (tests 없음)"; continue; fi; \
		echo "==> testing $$p"; \
		(cd $$p && env -u VIRTUAL_ENV uv run --frozen pytest -q) || exit 1; \
	done
# 아래 세 줄은 런타임 밖(../../sub/...)을 지목해서 부릅니다. 그러면 pytest 의 rootdir 이
# 저장소 루트로 잡히면서 런타임 pyproject.toml 을 configfile 로 집지 못하고,
# 거기 적힌 pythonpath = [..., "../.."] 가 통째로 무시됩니다. 그 결과 shared·sub 를
# import 하는 테스트가 collection 단계에서 전부 죽습니다. SPARK_RUN 과 같은 방식으로
# 저장소 루트를 직접 넘겨줍니다.
	@echo "==> testing sub/airflow"; \
	(cd main/airflow && env -u VIRTUAL_ENV PYTHONPATH=../.. uv run --frozen pytest -q ../../sub/airflow/tests) || exit 1
	@echo "==> testing sub/aws_lambda, shared/aws_lambda and shared/common"; \
	(cd main/aws_lambda && env -u VIRTUAL_ENV PYTHONPATH=../.. uv run --frozen pytest -q ../../sub/aws_lambda/tests ../../shared/aws_lambda/tests ../../shared/common/tests) || exit 1
	@echo "==> testing sub/spark"; \
	(cd main/spark && env -u VIRTUAL_ENV PYTHONPATH=../.. uv run --frozen pytest -q ../../sub/spark/tests) || exit 1

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
# lock 이 못 잡아서 여기서 챙깁니다. (sub/aws_lambda/functions/vehicle_catalog_raw_to_bronze
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
	@if [ -n "$(IMAGE_NAME)" ] && [ $$(echo $(RUNTIMES) | wc -w) -ne 1 ]; then \
		echo "IMAGE_NAME 은 런타임 하나를 지정할 때만 씁니다: RUNTIMES=$(RUNTIMES)" >&2; \
		exit 1; \
	fi
	@for r in $(RUNTIMES); do \
		name=$$(basename $$r); \
		if [ "$$name" = "aws_lambda" ]; then name="lambda"; fi; \
		image="$(IMAGE_PREFIX)$$name"; \
		if [ -n "$(IMAGE_NAME)" ]; then image="$(IMAGE_NAME)"; fi; \
		echo "==> building $(REGISTRY)$$image:$(GIT_SHA)"; \
		docker build --platform $(PLATFORM) --provenance=false --sbom=false -f $$r/Dockerfile \
			-t $(REGISTRY)$$image:$(GIT_SHA) . || exit 1; \
	done

.PHONY: setup-hooks
setup-hooks:
	@git config core.hooksPath .githooks
	@echo "==> Git review hooks enabled (.githooks)"

# =============================================================================
# bootstrap — 로컬에서 파이프라인을 돌리기 전에 있어야 하는 파생 산출물
# =============================================================================
# 외부 수집(HVFHV·카탈로그·자격·제원)은 DAG 가 합니다. 여기서 다시 만들지 않습니다 —
# 스크립트로 옮겨 적으면 DAG 와 로직이 갈려 로컬만 통과하는 상태가 됩니다.
# 여기는 DAG 가 없어서 손으로 만들어야 했던 것들만 담습니다.
#
# 이미 있으면 건너뜁니다. 각 스크립트의 입력(Bronze·Silver)은 DAG 가 만들어 둔 것이라,
# 없으면 스크립트가 어느 DAG 를 돌려야 하는지 알려주며 실패합니다.
#
# 건너뛰는 기준은 **파일 존재**이지 내용이 아닙니다. 그래서 산출물 스키마가 바뀐 뒤
# `make bootstrap` 을 돌리면 낡은 파일이 그대로 남습니다. 그럴 때 FORCE=1 을 붙이세요.
#
#   make driver-preferences FORCE=1   # 이 산출물만 다시
#   make bootstrap FORCE=1            # 4개 전부 다시
#
# 부르는 단위가 곧 범위입니다. travel-times 는 Spark 집계라 비싸므로, 하나만 바뀌었을
# 때 bootstrap 전체에 FORCE 를 걸지 마세요.
FORCE ?=

ZONE_LOOKUP_URL  := https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
ZONE_LOOKUP      := data/bronze/taxi_zone_lookup.csv
TRAVEL_TIMES     := data/silver/taxi_zone_travel_times
DRIVER_PREFS     := data/bronze/driver_preferences.parquet
COMPANY_SNAPSHOT := data/source/company

# 회사 픽스처를 어느 시점으로 만들지. 비우면 `config/generation.json` 의
# bootstrap.snapshot_date 를 씁니다 — 값은 그 파일 한 곳이 소유합니다.
#
# 이 날짜는 취향이 아니라 **데이터를 결정하는 값**입니다. 리스 시작일이
# `[lease_start_min, snapshot_date]` 에서 추첨되고, 생성기는 여기서부터 한 달씩만
# 전진할 수 있습니다(`sub/generators/synthetic_driver_trip_source/monthly.py`). 즉 이 값이
# 곧 **그 로컬에서 만들 수 있는 첫 달**입니다. 팀이 어느 달로 작업하기로 했으면
# 그 달을 넣으세요.
#
#   make company-snapshot SNAPSHOT_DATE=2025-01-01
SNAPSHOT_DATE ?=

# 건너뛰기 판정 대상. 시점을 지정했으면 그 파티션만 봅니다 — 데이터셋 디렉터리만
# 보면 다른 시점 픽스처가 하나라도 있을 때 요청한 시점을 조용히 안 만듭니다.
COMPANY_SNAPSHOT_TARGET = $(COMPANY_SNAPSHOT)$(if $(SNAPSHOT_DATE),/snapshot_date=$(SNAPSHOT_DATE))

# 메인 Spark 런타임을 공유하되 제품 코드는 저장소 루트의 네임스페이스로 구분합니다.
SPARK_RUN = cd main/spark && env -u VIRTUAL_ENV PYTHONPATH=../.. uv run --frozen python

.PHONY: bootstrap
bootstrap: zone-lookup travel-times driver-preferences company-snapshot
	@echo "==> bootstrap 완료"

# TLC 가 배포하는 265개 구역 정적 레퍼런스. HVFHV 원본과 같은 CloudFront 입니다.
.PHONY: zone-lookup
zone-lookup:
	@if [ -z "$(FORCE)" ] && [ -f "$(ZONE_LOOKUP)" ]; then \
		echo "==> skip zone-lookup (이미 있음: $(ZONE_LOOKUP))"; \
	else \
		echo "==> downloading $(ZONE_LOOKUP)"; \
		mkdir -p $(dir $(ZONE_LOOKUP)); \
		curl -fsSL $(ZONE_LOOKUP_URL) -o $(ZONE_LOOKUP) || exit 1; \
	fi

.PHONY: travel-times
travel-times:
	@if [ -z "$(FORCE)" ] && [ -d "$(TRAVEL_TIMES)" ]; then \
		echo "==> skip travel-times (이미 있음: $(TRAVEL_TIMES))"; \
	else \
		echo "==> building $(TRAVEL_TIMES)"; \
		$(SPARK_RUN) -m sub.spark.jobs.travel_times.job \
			--trips_path ../../data/silver/hvfhv --output_path ../../$(TRAVEL_TIMES) || exit 1; \
	fi

.PHONY: driver-preferences
driver-preferences:
	@if [ -z "$(FORCE)" ] && [ -f "$(DRIVER_PREFS)" ]; then \
		echo "==> skip driver-preferences (이미 있음: $(DRIVER_PREFS))"; \
	else \
		echo "==> building $(DRIVER_PREFS)"; \
		$(SPARK_RUN) -m sub.spark.jobs.driver_master.preference_job \
			--output_path ../../$(DRIVER_PREFS) --bronze_dir ../../data/bronze/hvfhv || exit 1; \
	fi

.PHONY: company-snapshot
company-snapshot:
	@if [ -z "$(FORCE)" ] && [ -d "$(COMPANY_SNAPSHOT_TARGET)" ]; then \
		echo "==> skip company-snapshot (이미 있음: $(COMPANY_SNAPSHOT_TARGET))"; \
	else \
		echo "==> building $(COMPANY_SNAPSHOT_TARGET)"; \
		$(SPARK_RUN) -m sub.generators.synthetic_company_snapshot.generate \
			--output_dir ../../$(COMPANY_SNAPSHOT) \
			$(if $(SNAPSHOT_DATE),--snapshot_date $(SNAPSHOT_DATE)) || exit 1; \
	fi
