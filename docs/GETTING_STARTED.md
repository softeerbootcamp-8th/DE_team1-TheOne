# 개발 환경 셋업 — 팀원 전원 같은 버전 쓰기

**개발은 Docker 에서 합니다.** 로컬에 Python 을 깔 필요가 없습니다.

---

## 1. 최초 1회 (팀원 각자)

```bash
# 1) Docker Desktop 설치 후 실행
# 2) 저장소 받기
git clone <repo-url> && cd DE_team1-Even1TrillionNoSee

# 3) 띄우기 (첫 실행은 이미지 빌드로 몇 분 걸립니다)
docker compose up -d
```

8080 이 다른 프로젝트에 물려 있으면 `AIRFLOW_PORT` 로 바꿔서 띄웁니다.

```bash
AIRFLOW_PORT=8090 docker compose up -d
```

브라우저에서 <http://localhost:8080> 접속. 로그인 화면 없이 바로 들어갑니다
(`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` — 로컬 1인 개발 전용 설정,
운영 배포 시 반드시 제거).

`airflow/dags/` 의 DAG 가 목록에 보이면 정상입니다. 하나도 안 보이면 import 에서 깨진 것이니
아래로 확인하세요 (정상이면 `No data found`).

```bash
docker compose exec airflow airflow dags list-import-errors
```

### 일상 명령어

```bash
docker compose up -d        # 띄우기
docker compose logs -f      # 로그 보기
docker compose down         # 내리기
docker compose down -v      # 내리면서 DB 초기화 (꼬였을 때)
docker compose build        # 라이브러리가 바뀐 뒤 이미지 다시 굽기
```

---

## 2. 버전이 고정되는 원리

이게 핵심입니다. **`uv.lock` 하나가 로컬과 Docker 양쪽의 원천입니다.**

```
pyproject.toml    "boto3==1.43.65 쓸래"           ← 직접 의존성 (리뷰 대상)
      ↓  uv lock
uv.lock           "boto3 1.43.65 + 해시 abc123…"  ← ★ git 에 커밋됨 (정확)
      │
      ├─→ docker build      이미지 안에 이 버전 그대로 설치
      └─→ uv sync --frozen  내 노트북 .venv (IDE 자동완성용, 선택)
```

`pip install boto3` 는 **설치하는 날마다 최신 버전**을 가져옵니다. 그래서 어제 받은
사람과 오늘 받은 사람이 달라집니다. `uv.lock` 을 쓰면 거기 적힌 그 버전만, 해시까지
대조해서 깔립니다. **그래서 갈릴 수가 없습니다.**

Dockerfile 안을 보시면 lock 을 이미지로 복사해서 그대로 설치합니다:

```dockerfile
COPY pyproject.toml uv.lock /tmp/build/
RUN uv export --frozen ... -o requirements.txt && pip install -r requirements.txt
```

실제로 대조해서 이미지와 lock 이 일치하는 걸 확인했습니다
(airflow 3.3.0 / pandas 2.1.4 / numpy 1.26.4 / boto3 1.43.65 …).

---

## 3. 구조

```
.github/workflows/   CI — PR 마다 버전 잠금 검증
docker-compose.yml   로컬 개발 환경 (Airflow + Postgres)
Makefile             lock / check / sync / build

airflow/    Airflow 3.3.0   → EC2          (compose 로 로컬 실행)
spark/      Spark 3.5.6     → EMR 7.13.0
lambda/     Python 3.11     → AWS Lambda
  ├─ pyproject.toml    무슨 라이브러리를 쓸지 (사람이 적음)
  ├─ uv.lock           ★ 실제 고정 — 커밋 필수
  ├─ .python-version   3.11
  ├─ Dockerfile        이미지 (uv.lock 그대로 설치)
  └─ dags/             airflow 만. 로컬에서 고치면 컨테이너에 바로 반영

config/              가정 파라미터 (기획서 11장). 버전 관리와 무관
```

### 왜 런타임을 3개로 나눴나

**의존성이 서로 충돌하기 때문입니다.**

| | Airflow | Spark |
|---|---|---|
| pandas | 2.1.4 | 3.0.1 |
| numpy | 1.26.4 | 2.4.3 |
| 근거 | Airflow 3.3.0 공식 constraints | EMR 7.13 이 제공하는 값 |

한 환경에 다 넣으면 둘 중 하나가 끌려 내려가 깨집니다.

> **spark 버전을 바꿀 때 주의.** EMR 7.13 이미지에는 파이썬이 두 개 있고
> (`python3.9` = 이미지 기본, `python3.11` = Spark 가 실제로 쓰는 것),
> 값은 **3.11 쪽**을 봐야 합니다. 기본 `python` 을 조회하면 엉뚱한 값이 나옵니다.
>
> ```bash
> docker run --rm --entrypoint /usr/bin/python3.11 \
>   public.ecr.aws/emr-serverless/spark/emr-7.13.0:latest -m pip list --format=freeze
> ```

---

## 4. 라이브러리를 추가·변경할 때

`uv.lock` 갱신은 **로컬에서** 해야 합니다. 그래서 uv는 깔아두세요.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

작업 순서:

```bash
cd airflow
# 1) pyproject.toml 의 dependencies 수정
uv lock                    # 2) 잠금 갱신 ← 빼먹으면 팀원과 버전이 갈립니다
cd .. && make check        # 3) 루트에서 확인
docker compose build && docker compose up -d   # 4) 이미지 다시 굽기
# 5) pyproject.toml 과 uv.lock 을 "같이" 커밋 → PR
```

**PR 올리기 전에 `make check` 한 번.** `pyproject.toml` 만 고치고 `uv lock` 을 안
돌렸다면 여기서 FAIL 이 납니다.

깜빡해도 **PR 을 올리면 CI 가 같은 검사를 자동으로 돌려서 빨간불**이 뜹니다
(`.github/workflows/ci.yml`). 로컬에서 미리 돌려보면 왕복을 줄일 수 있을 뿐입니다.

> CI 가 잡는 것: "`uv lock` 을 깜빡한" 실수.
> CI가 못 잡는 것: `uv lock`까지 돌려서 **잘못된 버전을 제대로 잠근** 경우.
> 예를 들어 `pandas==2.1.4` 를 `2.2.0` 으로 바꾸고 `uv lock` 을 돌리면 CI 는
> 통과합니다. 그래서 의존성 변경은 기능 변경과 분리해 PR로 올리고,
> Ruleset에 따라 다른 팀원 1명의 승인을 받아야 합니다.

### 남이 올린 변경을 받았을 때

```bash
git pull
docker compose build && docker compose up -d    # lock 이 바뀌었으면 이미지도 다시
```

### 버전을 변경할 때

세 런타임의 직접 의존성은 변경이 리뷰에서 명확히 보이도록 `==`로 고정합니다.
버전을 바꿀 때는 해당 `pyproject.toml`과 `uv.lock`을 함께 변경합니다.

```bash
cd <airflow|spark|lambda>
uv lock
cd .. && make check
```

의존성 변경은 기능 코드와 같은 PR에 섞지 않습니다.

### `libs/pipeline_core` 를 고쳤을 때

`lambda`, `spark` 는 공통 `Extractor`/`Transformer`/`Loader`/`Pipeline` 인터페이스를 담은
`libs/pipeline_core` 를 `pyproject.toml` 의 로컬 경로 의존성으로 참조합니다:

```toml
[tool.uv.sources]
pipeline-core = { path = "../libs/pipeline_core" }
```

이 방식은 처음 `uv sync` 할 때 빌드한 wheel 을 그대로 설치해두는 구조라, `libs/pipeline_core`
소스를 고친 뒤 영향받는 런타임에서 그냥 `uv sync` 만 다시 돌리면 **변경이 반영 안 될 수
있습니다.** 이럴 땐 아래처럼 명시적으로 재설치하세요:

```bash
cd lambda   # 또는 spark
uv sync --reinstall-package pipeline-core
```

### 파이썬 패키지가 아닌 것 — tesseract

`uv.lock` 은 파이썬 패키지만 고정합니다. **시스템 바이너리는 못 잠급니다.**

`lambda/functions/vehicle_catalog_raw_to_bronze` 은 렌탈 업체 사이트의 가격이
이미지 안에만 있어서 OCR(tesseract)로 읽습니다. `pytesseract` 는 이 바이너리를
호출하는 래퍼일 뿐이라, 바이너리가 없으면 실행 시점에 실패합니다.

| | 고정되는 곳 | 어떻게 |
|---|---|---|
| `pytesseract`, `pillow` | `lambda/uv.lock` | `uv lock` 이 자동 |
| **tesseract 바이너리** | 없음 (시스템 패키지) | **`make sync` 가 챙김** |

`make sync` 가 `tesseract` 타깃을 먼저 부릅니다. 없으면 macOS 는 brew,
Ubuntu/Debian 은 apt 로 깔고, 이미 있으면 버전만 찍고 넘어갑니다.

```bash
make sync
# ==> tesseract 5.5.3
# ==> syncing airflow ...
```

바이너리만 따로 확인하려면 `make tesseract`. 영어 데이터(`eng`)만 있으면 되고
brew/apt 기본 패키지에 포함되어 있습니다.

> **주의.** tesseract 는 버전에 따라 인식 결과가 달라질 수 있습니다.
> OCR 결과가 팀원마다 다르면 먼저 `tesseract --version` 을 맞춰보세요.
> 확인된 조합: **tesseract 5.5.3 / pytesseract 0.3.13 / pillow 12.3.0**
> — 차량 카드 12장 전부 제조사·모델·가격 인식 성공.

> **Lambda 배포는 아직입니다.** Lambda 베이스 이미지(AL2 / AL2023)에는
> tesseract 패키지가 없어 `yum`/`dnf` 로 안 깔립니다. 이 수집은 자주 돌릴
> 데이터가 아니라 지금은 로컬 실행만 지원합니다. 배포가 필요해지면
> Debian 베이스(`python:3.11-slim` + `awslambdaric`, tesseract 5.5.0 설치 확인)나
> AWS Textract 로 바꾸면 됩니다 — `ocr_card()` 함수 하나만 바뀝니다.

### Gas Price·차량 대장 원문 재처리

두 수집기는 네트워크 응답을 파싱하거나 OCR 하기 전에 `data/bronze` 아래의 불변
스냅샷으로 저장합니다. 파싱이 실패해도 원문은 남고, 같은 수집시각의 파일은 덮어쓰지
않습니다.

```text
data/bronze/gas_price/raw/collected_at=<UTC 수집시각>/source.html
data/bronze/gas_price/collected_date=YYYY-MM-DD/gas_price.json

data/bronze/vehicle_catalog/raw/collected_at=<UTC 수집시각>/source.html
data/bronze/vehicle_catalog/raw/collected_at=<UTC 수집시각>/images/<URL-SHA256>.bin
data/bronze/vehicle_catalog/collected_date=YYYY-MM-DD/vendor=fasttrack/<수집시각>.parquet
```

Gas Price HTML을 다시 파싱할 때는 스냅샷 경로와 경로에 기록된 UTC 수집시각을 사용합니다.

```python
from datetime import datetime, timezone
from pathlib import Path

from functions.gas_price_raw_to_bronze.extractor import GasPriceSnapshotExtractor
from functions.gas_price_raw_to_bronze.loader import GasPriceBronzeLoader

snapshot = Path("data/bronze/gas_price/raw/collected_at=<UTC 수집시각>/source.html")
timestamp = snapshot.parent.name.removeprefix("collected_at=")
collected_at = datetime.strptime(timestamp, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)
row = GasPriceSnapshotExtractor(str(snapshot)).extract()
print(GasPriceBronzeLoader("data/bronze", collected_at).write(row))
```

차량 대장은 저장 HTML에서 카드 URL을 복원하고 URL 해시로 저장 이미지를 찾은 뒤 OCR 합니다.

```python
from datetime import datetime, timezone
from pathlib import Path

from functions.common import vehicle_catalog_layout as layout
from functions.vehicle_catalog_raw_to_bronze.extractor import VehicleCatalogCardsExtractor, row_from_snapshot
from functions.vehicle_catalog_raw_to_bronze.loader import VehicleCatalogBronzeLoader

snapshot = Path("data/bronze/vehicle_catalog/raw/collected_at=<UTC 수집시각>/source.html")
timestamp = snapshot.parent.name.removeprefix("collected_at=")
collected_at = datetime.strptime(timestamp, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc)
rows = []
for card in VehicleCatalogCardsExtractor(str(snapshot)).extract():
    image = layout.image_snapshot_file("data/bronze", collected_at, card["image_url"])
    rows.append(row_from_snapshot(card, str(snapshot), str(image), collected_at))
print(VehicleCatalogBronzeLoader("data/bronze", collected_at).write(rows))
```

위 코드는 `lambda` 디렉터리에서 `uv run --frozen python`으로 실행합니다. 재처리는 원문
스냅샷을 바꾸지 않고 같은 수집일의 파생 Bronze JSON 또는 같은 수집시각의 Parquet만
다시 생성합니다. 이후 기존 Bronze → Silver 명령을 실행하면 됩니다.

---

## 5. 하지 말 것

| 하지 말 것 | 왜 |
|---|---|
| 컨테이너 안에서 `pip install` | 컨테이너 지우면 사라짐. pyproject 고치고 `uv lock` 할 것 |
| `uv.lock` 을 `.gitignore` 에 | 이걸 커밋 안 하면 버전 고정이 아예 동작 안 함 |
| `uv sync` (`--frozen` 없이) | lock 을 멋대로 갱신함 |
| `uv.lock` 손으로 편집 | 해시가 깨짐. 항상 `uv lock` 으로 재생성 |

`uv.lock` 이 **머지 충돌**나면 손으로 고치지 마세요:

```bash
git checkout --theirs airflow/uv.lock   # 아무 쪽이나 택하고
cd airflow && uv lock                   # pyproject 기준으로 재계산
```

---

## 6. 로컬 .venv 는 선택

IDE 자동완성·린트를 원하면:

```bash
make sync        # 또는 cd airflow && uv sync --frozen
```

Docker 안에서 도는 것과 **같은 버전**이 깔립니다. 실행은 Docker 에서 하니
필수는 아닙니다. 용량이 크니(airflow 만 수백 MB) 안 쓰면 `rm -rf */.venv`.

---

## 7. 아직 안 된 것

- **EMR 이미지가 `:latest`.** 떠다니는 태그라 운영 배포 전에 digest 고정 필요.
- **EMR 실제 잡 제출 미확인.** 세 이미지 모두 빌드 + 버전 대조까지 확인했지만,
  EMR Serverless 에 잡을 올려본 적은 없습니다.
- **`pyspark.pandas` 는 쓸 수 없습니다.** EMR 7.13 의 Spark 3.5.6-amzn-2 자체가
  numpy 2.x 에서 import 에 실패합니다 (`np.NaN` 제거). 로컬도 EMR 도 동일합니다.
  고치려고 numpy 를 내리면 EMR 제공분을 덮어써서 더 큰 문제가 됩니다.
  `pyspark.sql` API 와 pandas UDF 는 정상 동작하니 그쪽으로 쓰세요.
- **compose 는 로컬 개발 전용.** 운영은 EC2/EMR/Lambda 로 따로 배포합니다.
  `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10` 같은 설정도 개발용입니다.
