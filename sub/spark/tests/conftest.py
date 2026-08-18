"""테스트 전용 설정 리터럴.

**`config/generation.json` 을 읽지 않습니다.** 테스트가 프로덕션 설정 파일을 읽으면
값을 튜닝할 때마다 무관한 테스트가 깨지고, 테스트가 설정 변경을 막는 족쇄가 됩니다.
실제 파일을 읽는 것은 로더 자체를 검증하는 `test_config.py` 한 곳뿐입니다.

여기 값이 `config/generation.json` 과 우연히 같은 것들이 있습니다(기사 2,000명,
스냅샷 2026-01-01). 같아야 해서 같은 게 아니라, 기존 테스트가 그 세계를 전제로
기댓값을 적어 두었기 때문입니다 — 프로덕션 설정이 바뀌어도 이 값은 그대로 둡니다.
기사 수만은 `snapshot.GROUP_COUNTS` 합과 맞아야 합니다(구성비의 소유자가 코드).
"""

from datetime import date

from sub.config import build_config

TEST_SNAPSHOT_DATE = date(2026, 1, 1)
TEST_LEASE_START_MIN = date(2023, 1, 1)
TEST_SEED = 42
TEST_MODEL_YEAR = 2023

TEST_CONFIG_DATA = {
    "global_seed": TEST_SEED,
    "driver": {
        "initial_count": 2_000,
        "join_rate": 0.008,
        "exit_rate": 0.007,
        "vehicle_change_rate": 0.02,
    },
    "bootstrap": {
        "snapshot_date": TEST_SNAPSHOT_DATE.isoformat(),
        # 프로덕션은 200,000 입니다. 테스트는 작은 픽스처만 읽으므로 줄여 둡니다 —
        # 이 값이 결과를 바꾸지 않는 곳에서만 쓰입니다.
        "sample_per_month": 1_000,
    },
    "allocation": {
        "score_weights": {
            "time": 0.2975,
            "distance": 0.2550,
            "airport": 0.1700,
            "manhattan": 0.1275,
            "tier": 0.15,
        },
        "bucket_size": 5,
    },
}

TEST_CONFIG = build_config(TEST_CONFIG_DATA)
TEST_SCORE_WEIGHTS = TEST_CONFIG.allocation.score_weights
