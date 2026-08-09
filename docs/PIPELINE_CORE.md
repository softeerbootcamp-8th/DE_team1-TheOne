# 공통 파이프라인 아키텍처 (`pipeline_core`) 가이드

이 문서는 `Extractor`, `Transformer`, `Loader`, `Pipeline`의 역할과 설계 이유, 활용 방법을 설명합니다.

## 구성

```text
libs/pipeline_core/                # 외부 의존성이 없는 공통 인터페이스
  pyproject.toml
  pipeline_core/
    extractor.py                   # Extractor(ABC): extract() -> Any
    transformer.py                 # Transformer(ABC): transform(data) -> Any
    loader.py                      # Loader(ABC): write(data) -> WriteResult
    pipeline.py                    # Pipeline, PipelineResult
  tests/
    test_interfaces.py, test_pipeline.py

lambda/
  common/loaders.py                # LocalParquetLoader(Loader)
  tests/common/test_loaders.py
  pyproject.toml                   # pipeline-core 로컬 경로 의존성

spark/
  common/session.py                # get_or_create_spark_session(app_name)
  common/io.py                     # SparkParquetExtractor, SparkParquetLoader
  tests/common/test_session.py, test_io.py
  pyproject.toml                   # pipeline-core 로컬 경로 의존성

airflow/
  dags/common/defaults.py          # DEFAULT_ARGS, notify_slack_on_failure()
  tests/dags/common/test_defaults.py
  pyproject.toml                   # pytest 개발 의존성
```

현재 변경 사항은 모두 로컬에 있으며 아직 커밋되지 않았습니다.

## 설계 원칙

### Bronze에서는 원본만 적재

[메달리온 아키텍처](https://www.databricks.com/blog/what-is-medallion-architecture)에 따라 Bronze에는 원본을 그대로 적재하고, 정제와 집계는 Silver·Gold 단계에서 수행합니다.

- Bronze: `Extract → Load`
- Silver·Gold: `Extract → Transform → Load`

따라서 Lambda 크롤러는 `Extractor`와 `Loader`를 사용하고, Spark 작업은 필요에 따라 `Transformer`를 추가합니다.

### 단계별 인터페이스 분리

추출·변환·적재 로직은 하나의 클래스에 묶지 않고 독립된 추상 클래스로 분리했습니다. 이 OETL(Orchestrated ETL) 구조는 각 단계를 재사용하고 독립적으로 테스트하기 쉽습니다. 예를 들어 하나의 `LocalParquetLoader`를 여러 크롤러에서 공유하면서 저장 로직만 따로 검증할 수 있습니다.

- `Extractor`: 다음 단계에 전달할 데이터를 만듭니다. Lambda에서는 외부 API 호출이나 크롤링을, Spark에서는 Bronze·Silver 파티션 읽기를 담당합니다.
- `Transformer`: 데이터를 정제하거나 집계합니다. 주로 Spark의 Bronze→Silver, Silver→Gold 작업에서 사용합니다.
- `Loader`: 데이터를 목적지에 적재합니다. ETL의 Load 단계와 바로 대응하도록 `Sink` 대신 `Loader`로 이름을 정했습니다.
- `Pipeline`: `Extractor → [Transformer] → Loader` 순서로 실행합니다. 시작·종료 로그와 소요시간 측정을 공통 처리하므로 Lambda 핸들러를 `Pipeline(extractor, loader).run()` 형태로 통일할 수 있습니다.

`Pipeline`은 실패를 기록한 뒤 예외를 다시 발생시킵니다. 재시도와 실패 알림은 Airflow의 `on_failure_callback`이 담당합니다.

### 범용적인 적재 결과

`Loader.write()`는 `WriteResult`를 반환합니다. 적재 위치 필드의 이름은 파일 경로만 연상시키는 `path` 대신 `location`을 사용합니다.

`location`에는 파일 경로뿐 아니라 `schema.table` 같은 테이블 식별자도 담을 수 있습니다. 실제 값은 각 `Loader` 구현체가 결정합니다.

### 런타임별 의존성 분리

`lambda`, `spark`, `airflow`는 pandas와 numpy 버전이 서로 달라 의존성을 분리합니다. 자세한 내용은 `docs/GETTING_STARTED.md`를 참고하세요.

공통 인터페이스를 런타임마다 복사하면 변경 사항이 어긋날 수 있습니다. 이를 방지하기 위해 `abc`, `typing`, `dataclasses`만 사용하는 `libs/pipeline_core` 패키지를 만들고, Lambda와 Spark에서 로컬 경로 의존성으로 참조합니다. 각 런타임의 `uv.lock`은 그대로 유지하며 하나의 uv workspace로 통합하지 않습니다.

```toml
# lambda/pyproject.toml, spark/pyproject.toml
[tool.uv.sources]
pipeline-core = { path = "../libs/pipeline_core" }
```

## 현재 범위에서 제외한 항목

- `S3ParquetLoader`, `PostgresLoader`: 버킷 구조, 자격증명, 연결 방식이 정해진 뒤 구현합니다. 기존 `Loader` 계약을 따르면 현재 구현체와 교체할 수 있습니다.
- Airflow Operator: 실제 DAG가 없으므로 Lambda 호출이나 EMR Step 제출 Operator는 만들지 않았습니다. 현재는 공통 `default_args`와 Slack 실패 알림만 제공합니다.
- Docker 빌드 컨텍스트: 현재 `lambda/Dockerfile`, `spark/Dockerfile`, `Makefile`은 각 런타임 디렉터리를 빌드 컨텍스트로 사용합니다. `libs/` 경로 의존성을 포함하려면 배포 전에 빌드 컨텍스트를 저장소 루트로 변경해야 합니다.

> **주의:** Docker 빌드 컨텍스트는 아직 수정하지 않았습니다. 현재 상태에서 `make build`를 실행하면 Lambda와 Spark 이미지 빌드가 실패합니다.

## 사용 방법

### Lambda 크롤러 추가

다음은 `gas_price` 크롤러 예시입니다.

```python
# lambda/functions/gas_price/extractor.py
from pipeline_core.extractor import Extractor


class GasPriceExtractor(Extractor):
    name = "gas_price"

    def extract(self) -> list[dict]:
        # 외부 API 호출 및 파싱
        ...
        return rows
```

```python
# lambda/functions/gas_price/handler.py
import pyarrow as pa

from pipeline_core.pipeline import Pipeline
from common.loaders import LocalParquetLoader
from .extractor import GasPriceExtractor


SCHEMA = pa.schema([("date", pa.date32()), ("price", pa.float64())])


def lambda_handler(event, context):
    loader = LocalParquetLoader(
        base_dir="data/bronze",
        dataset="gas_price",
        schema=SCHEMA,
        partition_by=["date"],
    )
    result = Pipeline(GasPriceExtractor(), loader).run()
    return {
        "row_count": result.write_result.row_count,
        "location": result.write_result.location,
    }
```

테스트는 다음 명령으로 실행합니다.

```bash
cd lambda
uv run pytest
```

### Spark 작업 추가

다음은 Bronze의 `gas_price` 데이터를 정제해 Silver에 적재하는 예시입니다.

```python
# spark/jobs/bronze_to_silver/gas_price/transformer.py
from pipeline_core.transformer import Transformer


class GasPriceCleanTransformer(Transformer):
    def transform(self, df):
        return df.dropDuplicates(["date"]).filter(df.price.isNotNull())
```

```python
# spark/jobs/bronze_to_silver/gas_price/job.py
from pipeline_core.pipeline import Pipeline
from common.io import SparkParquetExtractor, SparkParquetLoader
from common.session import get_or_create_spark_session
from .transformer import GasPriceCleanTransformer


def main():
    spark = get_or_create_spark_session("gas_price_bronze_to_silver")
    Pipeline(
        SparkParquetExtractor(spark, "data/bronze/gas_price"),
        SparkParquetLoader("data/silver/gas_price", partition_by=["date"]),
        transformer=GasPriceCleanTransformer(),
    ).run()


if __name__ == "__main__":
    main()
```

Bronze→Silver와 Silver→Gold의 구조는 같습니다. 데이터셋별로 `Transformer`와 입출력 경로만 달라집니다. Bronze·Silver 데이터는 데이터셋마다 별도 Extractor를 만들지 말고 `SparkParquetExtractor(spark, path)`를 재사용하세요.

로컬 테스트에는 Java 17이 필요합니다. PySpark 3.5.6은 최신 JDK와 호환되지 않습니다.

```bash
brew install openjdk@17
cd spark
JAVA_HOME=/opt/homebrew/opt/openjdk@17 uv run pytest
```

### Airflow DAG에 공통 설정 적용

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.common.defaults import DEFAULT_ARGS


with DAG(
    dag_id="gas_price_collect",
    default_args=DEFAULT_ARGS,  # 재시도 2회, 5분 간격, 실패 시 Slack 알림
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    ...
```

Slack 알림을 사용하려면 Airflow 컨테이너에 `SLACK_WEBHOOK_URL` 환경 변수를 설정해야 합니다. 설정하지 않으면 경고 로그를 남기고 알림만 건너뜁니다. DAG 실행에는 영향을 주지 않습니다.

### `pipeline_core` 변경 사항 반영

`libs/pipeline_core`를 수정한 뒤에는 `uv sync`만으로 변경 사항이 반영되지 않을 수 있습니다. 다음과 같이 패키지를 명시적으로 다시 설치하세요.

```bash
cd lambda  # 또는 spark
uv sync --reinstall-package pipeline-core
```

자세한 내용은 `docs/GETTING_STARTED.md`의 "라이브러리를 추가·변경할 때" 절을 참고하세요.

## 테스트

```bash
cd libs/pipeline_core && uv run pytest  # 9 tests
cd lambda && uv run pytest             # 2 tests
cd spark && uv run pytest              # 3 tests, Java 17 필요
cd airflow && uv run pytest            # 3 tests
```
