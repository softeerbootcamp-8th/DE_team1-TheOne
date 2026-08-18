"""리스 업체 보유 차량 데이터를 Bronze에 적재합니다."""

import json

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import collect_monthly_dataset


configure_lambda_logging()

DATASET = "lease_vehicle_inventory"


def lambda_handler(event: dict | None = None, context=None) -> dict:
    return collect_monthly_dataset(event or {}, dataset=DATASET, dataset_dir=DATASET)


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
