"""설정 로더와 RunContext 계약.

완료 조건 1~5 를 그대로 옮깁니다.

1. 같은 config 로 RunContext 두 번 생성 → run_id 동일
2. config 값 하나만 바꿔도 run_id 변경
3. (아직 없음) derive_seed 결정성 — 시드 체계 재구성은 후속 작업입니다
4. (아직 없음) stage 만 달라도 다른 seed — 같은 후속 작업
5. 잘못된 config 는 로드 실패

3·4 는 이번 범위(설정 통합)에 `derive_seed`/`Stage` 가 없어서 비어 있습니다. 후속
lifecycle/시드 작업에서 이 파일에 추가합니다 — 조건 번호를 그대로 남겨 둡니다.
"""

import copy
import json

import pytest

from conftest import TEST_CONFIG, TEST_CONFIG_DATA
from sub.config import (
    DEFAULT_CONFIG_PATH,
    ConfigError,
    build_config,
    canonical_json,
    load_config,
)
from sub.run_context import RunContext

TARGET_MONTH = "2026-08"


def _data(**overrides) -> dict:
    """테스트 리터럴을 깊은 복사해 일부만 바꿉니다."""
    data = copy.deepcopy(TEST_CONFIG_DATA)
    for path, value in overrides.items():
        keys = path.split(".")
        cursor = data
        for key in keys[:-1]:
            cursor = cursor[key]
        cursor[keys[-1]] = value
    return data


# ── 완료 조건 1 ────────────────────────────────────────────────────────────────
def test_같은_config로_두_번_만들면_run_id가_같다():
    first = RunContext.create(TARGET_MONTH, build_config(_data()))
    second = RunContext.create(TARGET_MONTH, build_config(_data()))
    assert first.run_id == second.run_id
    assert first.config_hash == second.config_hash


def test_created_at은_run_id에_영향을_주지_않는다():
    """계보 기록용 필드라 해시에 들어가면 안 됩니다 — 들어가면 조건 1 이 깨집니다."""
    first = RunContext.create(TARGET_MONTH, TEST_CONFIG, created_at="2026-08-01T00:00:00+00:00")
    second = RunContext.create(TARGET_MONTH, TEST_CONFIG, created_at="2027-01-01T00:00:00+00:00")
    assert first.created_at != second.created_at
    assert first.run_id == second.run_id


# ── 완료 조건 2 ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "path, value",
    [
        ("global_seed", 43),
        ("driver.initial_count", 2_001),
        ("driver.join_rate", 0.009),
        ("driver.exit_rate", 0.006),
        ("driver.vehicle_change_rate", 0.03),
        ("bootstrap.snapshot_date", "2026-02-01"),
        ("bootstrap.sample_per_month", 999),
        ("allocation.bucket_size", 6),
    ],
)
def test_config_값_하나만_바꿔도_run_id가_바뀐다(path, value):
    base = RunContext.create(TARGET_MONTH, build_config(_data()))
    changed = RunContext.create(TARGET_MONTH, build_config(_data(**{path: value})))
    assert changed.run_id != base.run_id, f"{path} 변경이 run_id 에 반영되지 않았습니다"


def test_score_weights_구성이_달라지면_run_id가_바뀐다():
    base = RunContext.create(TARGET_MONTH, build_config(_data()))
    # 합은 1.0 을 유지하면서 배분만 바꿉니다 — 합 검증을 통과하면서 결과는 달라지는
    # 변경이라 해시가 반드시 반응해야 합니다.
    shifted = _data()
    shifted["allocation"]["score_weights"]["time"] = 0.2475
    shifted["allocation"]["score_weights"]["distance"] = 0.3050
    changed = RunContext.create(TARGET_MONTH, build_config(shifted))
    assert changed.run_id != base.run_id


def test_대상_월이_다르면_run_id가_다르고_config_hash는_같다():
    august = RunContext.create("2026-08", TEST_CONFIG)
    september = RunContext.create("2026-09", TEST_CONFIG)
    assert august.run_id != september.run_id
    # 같은 설정으로 다른 달을 돌릴 수 있어야 합니다 — target_month 를 config 에 넣지
    # 않은 이유가 이것입니다.
    assert august.config_hash == september.config_hash


# ── 완료 조건 5 ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "raw, expected",
    [
        (_data(**{"driver.join_rate": 1.5}), "0 이상 1 이하"),
        (_data(**{"driver.exit_rate": -0.1}), "0 이상 1 이하"),
        (_data(**{"allocation.score_weights": {
            "time": 0.5, "distance": 0.5, "airport": 0.5, "manhattan": 0.5, "tier": 0.5,
        }}), "합이 1.0"),
        (_data(**{"bootstrap.snapshot_date": "2026-01-15"}), "매월 1일"),
        (_data(**{"bootstrap.snapshot_date": "2026-13-01"}), "날짜 형식"),
        (_data(**{"bootstrap.sample_per_month": 0}), "1 이상"),
        (_data(**{"driver.initial_count": 0}), "1 이상"),
        (_data(**{"global_seed": 42.5}), "정수"),
        (_data(**{"allocation.bucket_size": True}), "정수"),
    ],
)
def test_잘못된_값은_로드_실패(raw, expected):
    with pytest.raises(ConfigError, match=expected):
        build_config(raw)


def test_오타_키는_로드_실패():
    typo = _data()
    typo["driver"]["exit_rat"] = typo["driver"].pop("exit_rate")
    with pytest.raises(ConfigError, match="알 수 없는 키"):
        build_config(typo)


def test_최상위_오타_키도_로드_실패():
    typo = _data()
    typo["allocaiton"] = typo.pop("allocation")
    with pytest.raises(ConfigError, match="알 수 없는 키|필수 키 누락"):
        build_config(typo)


def test_필수_키_누락은_로드_실패():
    missing = _data()
    del missing["bootstrap"]["sample_per_month"]
    with pytest.raises(ConfigError, match="필수 키 누락"):
        build_config(missing)


def test_score_weights_키_누락은_로드_실패():
    missing = _data()
    del missing["allocation"]["score_weights"]["tier"]
    with pytest.raises(ConfigError, match="키가 계약과 다릅니다"):
        build_config(missing)


@pytest.mark.parametrize("target_month", ["2026-8", "26-08", "2026-08-01", "2026/08", "", "여덟월"])
def test_형식_틀린_target_month는_실패(target_month):
    with pytest.raises(ConfigError, match="target_month"):
        RunContext.create(target_month, TEST_CONFIG)


def test_설정_파일이_없으면_실패(tmp_path):
    with pytest.raises(ConfigError, match="설정 파일이 없습니다"):
        load_config(tmp_path / "없는파일.json")


def test_JSON이_깨졌으면_실패(tmp_path):
    broken = tmp_path / "generation.json"
    broken.write_text("{ 'not': json, }", encoding="utf-8")
    with pytest.raises(ConfigError, match="올바른 JSON"):
        load_config(broken)


# ── 로더가 실제 파일을 읽는 유일한 곳 ────────────────────────────────────────────
def test_저장소의_설정_파일이_계약을_지킨다():
    """프로덕션 `config/generation.json` 을 읽는 테스트는 여기 하나뿐입니다.

    값을 검사하지 않습니다 — 값은 튜닝 대상이고, 검사하면 테스트가 설정 변경을
    막습니다. 파일이 **로드 가능한지**만 봅니다.
    """
    config = load_config()
    assert config.driver.initial_count >= 1
    assert canonical_json(config)


def test_기본_경로가_저장소_루트의_config를_가리킨다():
    assert DEFAULT_CONFIG_PATH.name == "generation.json"
    assert DEFAULT_CONFIG_PATH.parent.name == "config"
    assert (DEFAULT_CONFIG_PATH.parent.parent / "sub").is_dir()


def test_정렬_직렬화는_키_순서에_흔들리지_않는다():
    shuffled = json.loads(json.dumps(_data()))
    shuffled["allocation"] = shuffled.pop("allocation")
    shuffled["global_seed"] = shuffled.pop("global_seed")
    assert canonical_json(build_config(shuffled)) == canonical_json(build_config(_data()))
