"""운영 Lambda 원격 호출의 최소 계약.

1. 공통 operator는 JSON 객체를 dict XCom으로 바꾸고 Lambda 오류를 숨기지 않는다.
2. 운영 Main DAG 3종은 대상 함수 5개만 동기 Lambda operator로 실행한다.
"""

import pytest
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

from dags import driver_vehicle_monthly_snapshot_raw_to_silver_dag as driver
from dags import lease_vehicle_inventory_raw_to_silver_dag as lease
from dags import monthly_taxi_trip_raw_to_silver_dag as monthly
from shared.airflow.common.lambda_remote import JsonLambdaInvokeFunctionOperator


def test_공통_operator는_JSON_dict를_반환하고_Lambda오류를_노출한다(monkeypatch):
    operator = JsonLambdaInvokeFunctionOperator(
        task_id="invoke",
        function_name="example",
    )
    monkeypatch.setattr(
        LambdaInvokeFunctionOperator,
        "execute",
        lambda self, context: '{"row_count": 1}',
    )
    assert operator.execute({}) == {"row_count": 1}

    def fail(self, context):
        raise ValueError("Lambda function execution resulted in error")

    monkeypatch.setattr(LambdaInvokeFunctionOperator, "execute", fail)
    with pytest.raises(ValueError, match="Lambda function execution resulted in error"):
        operator.execute({})


@pytest.mark.parametrize(
    ("module", "factory", "expected"),
    [
        (
            monthly,
            monthly.monthly_taxi_trip_raw_to_silver_pipeline,
            {"raw_to_bronze": "monthly_taxi_trip_raw_to_bronze"},
        ),
        (
            driver,
            driver.driver_vehicle_monthly_snapshot_raw_to_silver_pipeline,
            {
                "raw_to_bronze": "driver_vehicle_monthly_snapshot_raw_to_bronze",
                "bronze_to_silver": "driver_vehicle_monthly_snapshot_bronze_to_silver",
            },
        ),
        (
            lease,
            lease.lease_vehicle_inventory_raw_to_silver_pipeline,
            {
                "raw_to_bronze": "lease_vehicle_inventory_raw_to_bronze",
                "bronze_to_silver": "lease_vehicle_inventory_bronze_to_silver",
            },
        ),
    ],
)
def test_운영_DAG는_대상_Lambda를_동기_원격호출한다(module, factory, expected):
    dag = factory()
    for task_id, function_name in expected.items():
        task = dag.get_task(task_id)
        assert isinstance(task, JsonLambdaInvokeFunctionOperator)
        assert task.function_name == function_name
        assert task.aws_conn_id == "aws_default"
        assert task.invocation_type == "RequestResponse"
        assert task.payload.startswith("{")
