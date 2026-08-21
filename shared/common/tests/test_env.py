"""로컬 `.env` 로딩 경로 계약. 이슈 #536.

`load_dotenv` 는 없는 경로에 예외 없이 `False` 를 돌려줍니다. 그래서 경로가 틀리면
**조용히 아무것도 안 읽고**, 나중에 자격 증명이 없다는 엉뚱한 곳에서 실패합니다.
실제로 `parents[4]` 로 저장소 바깥(`~/Desktop`)을 가리키고 있었습니다.
"""

from pathlib import Path

from shared.common import env


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def test_env_파일은_저장소_루트를_가리킨다():
    assert env._ENV_FILE == REPOSITORY_ROOT / ".env"


def test_Lambda_에서는_로컬_env_를_읽지_않는다(monkeypatch):
    """실행 환경 변수를 로컬 파일이 덮어쓰면 안 됩니다."""
    loaded = []
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "some-function")
    monkeypatch.setattr(env, "load_dotenv", lambda path: loaded.append(path))

    env.load_local_env()

    assert loaded == []


def test_로컬에서는_저장소_루트_env_를_읽는다(monkeypatch):
    loaded = []
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.setattr(env, "load_dotenv", lambda path: loaded.append(path))

    env.load_local_env()

    assert loaded == [REPOSITORY_ROOT / ".env"]
