"""EMR Serverless 배포 생존 계약.

1. deferrable trigger가 요구하는 async AWS client 설치
2. 동일 이미지 재배포는 Airflow 컨테이너를 강제로 재생성하지 않음
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_deferrable_EMR용_async_AWS_client가_설치된다():
    from aiobotocore.session import get_session
    from aiobotocore.waiter import create_waiter_with_client

    assert callable(get_session)
    assert callable(create_waiter_with_client)


def test_Airflow_배포는_동일이미지를_강제로_재생성하지_않는다():
    workflow = (ROOT / ".github/workflows/deploy-airflow.yml").read_text()

    assert "--pull always airflow" in workflow
    assert "--force-recreate airflow" not in workflow
