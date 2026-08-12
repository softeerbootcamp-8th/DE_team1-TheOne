"""Gas Price Raw -> Bronze DAG와 적재 결과 검증 조건을 확인합니다.

1. DAG는 Raw 적재 후 Bronze 검증을 실행한다.
2. layout 경로에 저장된 정상 Bronze JSON은 통과한다.
3. Handler 응답, 파일 경로·존재 여부, JSON 필수값이 잘못되면 거부한다.
4. 0·음수·NaN 가격과 NY/regular가 아닌 데이터는 거부한다.
5. collected_at 시간대와 UTC 수집일이 파티션 날짜와 다르면 거부한다.
"""

import importlib
import json

import pytest

from dags import gas_price_raw_to_bronze_dag as dag_module

# DAG 모듈이 저장소 루트를 sys.path에 추가한 뒤 불러옵니다.
# `lambda`는 예약어라 일반 import 문을 사용할 수 없습니다.
layout = importlib.import_module("lambda.functions.common.gas_price_layout")

DAG = dag_module.gas_price_raw_to_bronze_dag
validate_bronze = DAG.get_task("validate_bronze").python_callable

COLLECTED_DATE = "2026-08-09"
VALID_RECORD = {
    "state": "NY",
    "fuel_type": "regular",
    "price_raw": "$3.159",
    "price_date_raw": "08/08/26",
    "source_url": "https://gasprices.aaa.com/?state=NY",
    "collected_at": "2026-08-09T12:00:00+00:00",
}


@pytest.fixture
def bronze_dir(tmp_path, monkeypatch):
    """검증 Task가 보는 Bronze 루트를 임시 디렉터리로 바꿉니다."""
    monkeypatch.setattr(dag_module, "BRONZE_DIR", str(tmp_path))
    return str(tmp_path)


def record_of(**overrides) -> dict:
    record = VALID_RECORD.copy()
    record.update(overrides)
    return record


def write_bronze(bronze_dir: str, body=None):
    path = layout.bronze_file(bronze_dir, COLLECTED_DATE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        body if isinstance(body, str) else json.dumps(VALID_RECORD if body is None else body),
        encoding="utf-8",
    )
    return path


def result_of(path, **overrides) -> dict:
    result = {
        "row_count": 1,
        "locations": [str(path)],
        "collected_date": COLLECTED_DATE,
    }
    result.update(overrides)
    return result


# --- DAG 구조 -----------------------------------------------------------------


def test_dag_id와_task_구성이_정확하다():
    assert DAG.dag_id == "gas_price_raw_to_bronze_pipeline"
    assert {task.task_id for task in DAG.tasks} == {"raw_to_bronze", "validate_bronze"}


def test_bronze_검증은_raw_적재_뒤에_실행된다():
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}


# --- 정상 케이스 ---------------------------------------------------------------


def test_정상_bronze_json은_검증을_통과한다(bronze_dir):
    path = write_bronze(bronze_dir)

    validate_bronze(result_of(path))


# --- Handler 응답이 잘못된 경우 -------------------------------------------------


def test_handler_결과가_dict가_아니면_거부한다():
    with pytest.raises(TypeError, match="dict"):
        validate_bronze(["not", "a", "dict"])


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"row_count": 0}, id="row_count가 0"),
        pytest.param({"row_count": 2}, id="row_count가 2"),
        pytest.param({"locations": []}, id="locations가 비어 있음"),
        pytest.param({"locations": ["a", "b"]}, id="locations가 2개"),
        pytest.param({"collected_date": 20260809}, id="collected_date가 문자열이 아님"),
        pytest.param({"collected_date": "2026/08/09"}, id="collected_date 형식 오류"),
    ],
)
def test_handler_응답_계약이_깨지면_거부한다(bronze_dir, overrides):
    path = write_bronze(bronze_dir)

    with pytest.raises(ValueError):
        validate_bronze(result_of(path, **overrides))


# --- 적재 경로와 파일이 잘못된 경우 --------------------------------------------


def test_layout이_정한_경로가_아니면_거부한다(bronze_dir, tmp_path):
    path = write_bronze(str(tmp_path / "elsewhere"))

    with pytest.raises(ValueError, match="적재 경로"):
        validate_bronze(result_of(path))


def test_적재_파일이_없으면_거부한다(bronze_dir):
    path = layout.bronze_file(bronze_dir, COLLECTED_DATE)

    with pytest.raises(FileNotFoundError, match="적재 파일"):
        validate_bronze(result_of(path))


# --- Bronze JSON 내용이 잘못된 경우 --------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("{not json", id="JSON 파싱 실패"),
        pytest.param([], id="최상위가 객체가 아님"),
        pytest.param(
            {key: value for key, value in VALID_RECORD.items() if key != "price_raw"},
            id="price_raw 누락",
        ),
        pytest.param(record_of(price_date_raw="2026-08-08"), id="가격 기준일 형식 오류"),
        pytest.param(record_of(collected_at="not-a-datetime"), id="수집시각 형식 오류"),
    ],
)
def test_json_파싱이나_필수값_변환에_실패하면_거부한다(bronze_dir, body):
    path = write_bronze(bronze_dir, body)

    with pytest.raises(ValueError, match="JSON 값"):
        validate_bronze(result_of(path))


@pytest.mark.parametrize("price_raw", ["$0.00", "-$1.00", "NaN"])
def test_0원_음수_nan_가격은_거부한다(bronze_dir, price_raw):
    path = write_bronze(bronze_dir, record_of(price_raw=price_raw))

    with pytest.raises(ValueError, match="가격은 0보다"):
        validate_bronze(result_of(path))


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"state": "NJ"}, id="뉴욕주가 아님"),
        pytest.param({"fuel_type": "diesel"}, id="regular가 아님"),
    ],
)
def test_ny_regular_데이터가_아니면_거부한다(bronze_dir, overrides):
    path = write_bronze(bronze_dir, record_of(**overrides))

    with pytest.raises(ValueError, match="state 또는 fuel_type"):
        validate_bronze(result_of(path))


def test_collected_at에_시간대가_없으면_거부한다(bronze_dir):
    path = write_bronze(
        bronze_dir,
        record_of(collected_at="2026-08-09T12:00:00"),
    )

    with pytest.raises(ValueError, match="시간대"):
        validate_bronze(result_of(path))


def test_collected_at의_utc_날짜가_수집일과_다르면_거부한다(bronze_dir):
    path = write_bronze(
        bronze_dir,
        record_of(collected_at="2026-08-09T23:30:00-04:00"),
    )

    with pytest.raises(ValueError, match="collected_at과 collected_date"):
        validate_bronze(result_of(path))


@pytest.mark.parametrize("source_url", [None, "", "   "])
def test_source_url이_비어_있으면_거부한다(bronze_dir, source_url):
    path = write_bronze(bronze_dir, record_of(source_url=source_url))

    with pytest.raises(ValueError, match="source_url"):
        validate_bronze(result_of(path))
