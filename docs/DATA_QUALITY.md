# 데이터 품질

적재가 성공했다는 것과 데이터가 맞다는 것은 다릅니다.
이 문서는 **틀린 값이 조용히 하류로 흘러가는 것을 막는 장치**들을 정리합니다.

- [1. 검증 게이트](#1-검증-게이트)
- [2. 원자적 공개](#2-원자적-공개)
- [3. 스키마 드리프트 감지](#3-스키마-드리프트-감지)
- [4. 테스트와 CI](#4-테스트와-ci)

---

## 1. 검증 게이트

모든 적재 태스크 뒤에 **Great Expectations 검증 태스크**가 붙습니다. 실패하면 하류로 내려가지 않습니다.

```
raw_to_bronze → validate_bronze → bronze_to_silver → validate_silver
```

검증 결과는 정적 **Data Docs** 로 발행되어 어떤 규칙이 왜 깨졌는지 브라우저에서 볼 수 있습니다.
([validation.py](../shared/airflow/common/validation.py))

**검증 태스크에는 `retries=0` 을 명시적으로 겁니다.**
같은 입력을 다시 검증해도 결과는 같습니다. DAG 기본값을 그대로 물려받으면
깨진 데이터를 발견하고도 30분을 더 기다린 뒤에야 알림이 옵니다.

HVFHV와 기사 Bronze에서 필수 컬럼이 빠진 경우만 예외입니다. 원천이 같은 달의
내용을 고쳤을 수 있으므로 검증 태스크가 수집을 한 번 다시 호출하고 새 결과를
검증합니다. 다시 받은 파일도 불완전하면 즉시 실패합니다.

HVFHV 정제는 필수 컬럼을 Silver 기대 타입으로 명시 변환합니다. 변환할 수 없는 값은
불합격 행으로 버리고, `NULL`·범위 이탈·타입 불일치 건수를 각각 로그로 남깁니다.
월별 불합격 허용 비율은 5%이며 5% 이상이면 Silver 적재를 중단합니다.

---

## 2. 원자적 공개

Parquet 을 목적지에 직접 쓰면 쓰는 도중의 **잘린 파일**을 하류가 읽을 수 있습니다.
같은 디렉터리에 고유 임시 파일을 완성한 뒤 `replace` 로 교체합니다.

```python
def atomic_write(path: Path, writer: Callable[[Path], object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        writer(temporary)
        temporary.replace(path)      # 원자적 교체
    finally:
        temporary.unlink(missing_ok=True)
```

Spark 쓰기는 `partitionOverwriteMode=dynamic` 을 씁니다 —
재실행 시 해당 파티션만 덮어쓰고 다른 달은 건드리지 않습니다.
이 옵션이 없으면 `mode("overwrite")` 가 데이터셋 디렉터리 전체를 지우고 다시 씁니다.
**월 배치가 재시도될 때 지난달이 통째로 사라지는 사고가 여기서 납니다.**

월별 Bronze 원천은 같은 `year_month`에 새 내용이 제공되면 `data.parquet`을 원자적으로 교체합니다. 원천의 행 수·checksum은 Main 계약으로 전달하지 않습니다.

---

## 3. 스키마 드리프트 감지

외부 원천은 예고 없이 컬럼을 바꿉니다. 수집 시점에 기대 스키마와 대조해
**누락 / 타입 불일치 / 신규 추가**를 구분해 보고합니다.
([schema_validator.py](../shared/lambda_runtime/common/schema_validator.py))

```
❌ 누락된 컬럼: `combined_mpg` (기대 타입: `double`)
⚠️ 타입 불일치 컬럼 `model_year`: 기대=`int32`, 실제=`string`
➕ 신규 추가된 컬럼: `range_miles_epa` (`double`)
```

누락·타입 불일치는 파이프라인을 세우고, 신규 추가는 로그로만 남깁니다 —
원천이 컬럼을 더한 것만으로 수집을 멈출 이유는 없습니다.

---

## 4. 테스트와 CI

**테스트 541개.** 변환 로직뿐 아니라 **DAG 자체를 계약으로 테스트**합니다 —
재시도 규약, `max_active_runs`, BashOperator 의 `PYTHONPATH`, 핸들러 이름, 태스크 의존 순서.

PR 마다 GitHub Actions 가 4가지를 검증합니다.

| Job | 검증 내용 | 실행 조건 |
| --- | --- | --- |
| `version-lock` | `pyproject.toml` 을 고치고 `uv lock` 을 안 돌린 PR 차단 | 항상 |
| `test` | 런타임별 uv 환경으로 pytest | 항상 |
| `docker-build` | 이미지가 실제로 빌드되는지 | **이미지의 입력**이 바뀔 때 |
| `dag-import` | Airflow 가 DAG 를 읽을 수 있는지 | `airflow/` 가 바뀔 때 |

`docker-build` 는 소스가 아니라 **이미지의 입력**(Dockerfile · 의존성 파일 · `libs/pipeline_core`)을 기준으로 거릅니다 —
DAG 나 핸들러를 고쳐도 이미지 내용은 같기 때문입니다.

`dag-import` 를 별도 job 으로 뗀 이유: 이미지 빌드에 붙여 두면 `dags/` 만 고친 PR 에서 이미지가 스킵되며
이 검증까지 함께 스킵됩니다. **Airflow 는 import 에 실패한 DAG 를 조용히 건너뛰므로**,
배포 후 "화면에 안 보인다"로 발견되기 전에 여기서 잡습니다.
