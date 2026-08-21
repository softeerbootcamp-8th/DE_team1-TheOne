"""DAG 사이를 잇는 Curated Asset 정의.

생산자(각 수집 DAG)와 소비자(`vehicle_master_curated_to_curated_dag`)가 **같은 객체**를 봐야
합니다. URI 를 양쪽에 문자열로 적으면 한 글자만 달라도 실패하지 않고 조용히
트리거가 안 걸립니다 — DAG 가 핸들러 이름을 문자열로 넘겼다가 겪은 것과 같은
실패 유형입니다(#322).

URI 는 `curated://<데이터셋>` 형식입니다. 실제 저장 경로가 아니라 **논리 이름**이고,
로컬 파일이든 S3 든 이 값은 그대로 둡니다.

Asset 이벤트는 생산자 태스크가 **성공했을 때만** 발행됩니다. 그래서 `outlets` 는
적재 태스크가 아니라 **검증 태스크**에 답니다. 적재 직후에 발행하면 검증 전 상태가
소비자에게 흘러갑니다.
"""

from airflow.sdk import Asset

VEHICLE_CATALOG_CURATED = Asset("curated://vehicle_catalog")
UBER_ELIGIBLE_VEHICLES_CURATED = Asset("curated://uber_eligible_vehicles")
LYFT_ELIGIBLE_VEHICLES_CURATED = Asset("curated://lyft_eligible_vehicles")
FUELECONOMY_VEHICLE_SPECS_CURATED = Asset("curated://fueleconomy_vehicle_specs")

# 통합 결과. 하류가 이걸 구독합니다.
VEHICLE_MASTER_CURATED = Asset("curated://vehicle_master")
