"""EMR Serverless 배포 생존 계약.

1. deferrable trigger가 요구하는 async AWS client 설치
2. 동일 이미지 재배포는 Airflow 컨테이너를 강제로 재생성하지 않음
3. 요청 자원이 애플리케이션 maximumCapacity 를 넘지 않음 — 넘으면 마지막
   executor 가 조용히 안 뜨고 느려지기만 함
4. executor 수를 명시 — 없으면 한 잡이 용량을 독차지
5. Gold 셔플 파티션이 총 코어 수의 배수
"""

import re
from pathlib import Path

import pytest


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


# EMR Serverless 애플리케이션 theone-spark 의 maximumCapacity 입니다.
# 콘솔에서 바꾸면 여기도 바꿔야 합니다 — 어긋나면 이 테스트가 지켜주지 못합니다.
#   aws emr-serverless get-application --application-id <id> \
#     --query application.maximumCapacity
APP_MAX_VCPU = 12
APP_MAX_MEMORY_GB = 48

SPARK_JOBS = {
    "monthly_taxi_trip_raw_to_silver_dag",
    "monthly_taxi_trip_silver_to_gold_dag",
}


def _submit_parameters(module_name: str) -> dict[str, str]:
    text = (ROOT / "main/airflow/dags" / f"{module_name}.py").read_text()
    return dict(re.findall(r"--conf (spark\.[\w.]+)=([\w.]+)", text))


def _container_gb(memory: str, overhead: str | None) -> int:
    """EMR 이 실제로 잡는 워커 메모리(GB).

    overhead 를 명시하지 않으면 Spark 기본값 max(384MB, 힙의 10%) 가 붙고, EMR 은
    그 합을 1GB 단위로 올림합니다. 6g 요청이 7GB 로 잡히는 이유입니다 —
    실측에서도 워커당 7GB 로 나옵니다.
    """
    heap_mb = int(float(memory.rstrip("g")) * 1024)
    overhead_mb = (
        int(float(overhead.rstrip("g")) * 1024)
        if overhead
        else max(384, heap_mb // 10)
    )
    return -(-(heap_mb + overhead_mb) // 1024)


@pytest.mark.parametrize("module_name", sorted(SPARK_JOBS))
def test_요청_자원이_애플리케이션_상한을_넘지_않는다(module_name):
    """넘으면 마지막 executor 가 못 뜨고 **조용히** 적은 수로 돕니다.

    실패하지 않아서 느려진 것만 보이고 원인은 안 보입니다. executor 메모리나 개수를
    올릴 때 이 계산을 같이 하도록 고정합니다.
    """
    conf = _submit_parameters(module_name)

    executors = int(conf["spark.dynamicAllocation.maxExecutors"])
    driver_gb = _container_gb(
        conf["spark.driver.memory"], conf.get("spark.driver.memoryOverhead")
    )
    executor_gb = _container_gb(
        conf["spark.executor.memory"], conf.get("spark.executor.memoryOverhead")
    )

    vcpu = int(conf["spark.driver.cores"]) + executors * int(conf["spark.executor.cores"])
    memory_gb = driver_gb + executors * executor_gb

    assert vcpu <= APP_MAX_VCPU, f"{vcpu} vCPU > 상한 {APP_MAX_VCPU}"
    assert memory_gb <= APP_MAX_MEMORY_GB, f"{memory_gb} GB > 상한 {APP_MAX_MEMORY_GB}"


@pytest.mark.parametrize("module_name", sorted(SPARK_JOBS))
def test_executor_수를_명시한다(module_name):
    """`maxExecutors` 가 없으면 애플리케이션 상한까지 무한정 늘어납니다.

    지금은 상한이 곧 이 값이라 결과가 같지만, 상한을 또 올리거나 두 잡을 동시에
    돌리면 한 잡이 용량을 독차지해 다른 잡이 대기합니다.
    """
    conf = _submit_parameters(module_name)

    assert "spark.dynamicAllocation.maxExecutors" in conf
    assert int(conf["spark.dynamicAllocation.initialExecutors"]) == int(
        conf["spark.dynamicAllocation.maxExecutors"]
    ), "배치 잡은 처음부터 다 띄우는 게 빠릅니다 — 램프업 대기가 없습니다"


def test_Gold_셔플_파티션이_코어_수의_배수다():
    """배수가 아니면 마지막 웨이브가 일부 코어만 쓰고 나머지는 놉니다.

    executor 5개 x 2코어 = 10코어이므로 40 이면 정확히 4웨이브입니다.
    """
    conf = _submit_parameters("monthly_taxi_trip_silver_to_gold_dag")
    cores = int(conf["spark.dynamicAllocation.maxExecutors"]) * int(
        conf["spark.executor.cores"]
    )

    assert int(conf["spark.sql.shuffle.partitions"]) % cores == 0
