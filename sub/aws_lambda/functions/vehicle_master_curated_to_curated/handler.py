"""네 개 Curated 를 합쳐 차량 마스터 Curated 를 만듭니다.

다른 함수와 달리 Raw 를 읽지 않습니다. 입력도 출력도 Curated 입니다.

출력 파티션 날짜는 **읽은 원천의 최신 수집일**입니다. 실행일(UTC)이 아닙니다.

Asset 트리거로 도는데 실행일을 쓰면, 상류가 매월 1일에 낸 데이터를 재시도 때문에
2일에 조립하면 `collected_date=2일` 로 적재됩니다. 같은 달 데이터가 두 파티션에
나뉘고, 하류가 연월로 찾을 때 어느 쪽이 맞는지 알 수 없습니다.

`collected_date` 이벤트 값은 **읽기 상한**으로만 씁니다 (원천은 이 날짜 이하의 최신
파티션에서 각각 읽습니다). 과거 날짜로 다시 돌렸을 때 그때 없던 파티션이 섞이지
않게 하는 장치입니다 — main Gold 의 `resolve_target_year_month` 와 같은 규칙입니다.
"""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import build_extractor
from .loader import build_loader
from .transformer import VehicleMasterCuratedTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    as_of = (
        event.get("collected_date")
        or os.getenv("COLLECTED_DATE")
        or datetime.now(timezone.utc).date().isoformat()
    )
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    curated_dir = event.get("curated_dir") or os.getenv("CURATED_DIR", "data/source/curated")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")

    extractor = build_extractor(storage, curated_dir, as_of, bucket=bucket)
    # 출력 파티션은 읽은 원천 중 가장 최신 수집일을 씁니다. 최소값이 아닌 이유는
    # 원천마다 파티션이 다른 날에 놓일 수 있고, 그중 하나라도 새 데이터를 실었다면
    # 이 조립물은 그 시점 스냅샷이기 때문입니다. 어느 원천이 언제 것인지는
    # 응답의 `source_collected_dates` 에 그대로 남습니다.
    collected_date = max(extractor.resolve_source_dates().values())
    loader = build_loader(storage, curated_dir, collected_date, bucket=bucket)
    result = Pipeline(
        extractor,
        loader,
        transformer=VehicleMasterCuratedTransformer(),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        # 도시별로 파일 하나씩 씁니다 — 도시 수는 len(locations) 입니다.
        "locations": loader.paths,
        "collected_date": collected_date,
        # 읽기 상한. 출력 파티션(collected_date)과 다를 수 있습니다.
        "as_of": as_of,
        # 어느 원천 스냅샷으로 만들었는지. 원천마다 수집 주기가 달라 날짜가
        # 제각각이라 결과만 보고는 알 수 없습니다.
        "source_collected_dates": extractor.source_collected_dates,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
