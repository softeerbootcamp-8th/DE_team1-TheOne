"""[기사 운행 이력] Silver 스키마.

운행 한 행에 **그 시점의** 리스 한 건을 붙인 표입니다. 그래서 컬럼을 여기 손으로 다시
적지 않고 두 입력 스키마에서 파생합니다 — 손으로 적으면 상류가 컬럼을 늘렸을 때 이
파일만 뒤처지고, 그 어긋남은 적재 실패가 아니라 Gold 의 빈 값으로 드러납니다.
(스키마 소유권을 `schema/` 에 모으는 이유는 #466 참고.)

생산자는 `spark/jobs/driver_trip/`, 소비자는 `spark/jobs/silver_to_gold/` 와
적재 후 검증(`airflow/scripts/hvfhv_driver_trip_silver/tasks.py`) 입니다.
"""

from schema.silver.driver_vehicle_leases import SCHEMA as _LEASE_SCHEMA
from schema.silver.hvfhv import FINAL_SCHEMA as _TRIP_SCHEMA

# HVFHV Clean Silver 가 NULL 자리표시로 들고 오는 컬럼입니다 — 채우는 값이 리스 쪽에
# 있습니다. 빼지 않으면 select 에 같은 이름이 두 번 들어가는데, select 는 중복 이름을
# 허용해 조용히 지나가고 **쓰기 단계에서야** COLUMN_ALREADY_EXISTS 로 죽습니다.
TRIP_PLACEHOLDER_COLUMNS = ("driver_id", "taxi_model_id")
# 조인 키라 양쪽 값이 같습니다. 운행 쪽 하나만 싣습니다.
LEASE_JOIN_COLUMNS = ("taxi_id",)

# 계보 컬럼은 없습니다 (#481). 예전에는 `snapshot_date` 로 "어느 시점 회사 상태를
# 썼나" 를 남겼는데, 그건 이 단계가 회사 스냅샷 폴더를 직접 골라 읽던 시절 얘기입니다.
# 지금 리스 Clean Silver 는 `year_month` 파티션 하나가 그 달 1일자 상태라, 그 값은
# 언제나 `{year_month}-01` 이었습니다 — 파티션 키를 다르게 적은 것에 불과합니다.
# 스냅샷 시점은 가짜 데이터를 만드는 쪽(`synthetic_driver_trip_source`)의 관심사입니다.

TRIP_COLUMNS = tuple(
    field.name for field in _TRIP_SCHEMA if field.name not in TRIP_PLACEHOLDER_COLUMNS
)
LEASE_COLUMNS = tuple(
    name for name in _LEASE_SCHEMA.names if name not in LEASE_JOIN_COLUMNS
)
COLUMNS = (*TRIP_COLUMNS, *LEASE_COLUMNS)

# 행 하나를 식별하고 하류 조인을 성립시키는 값. 비면 Gold 집계가 실패가 아니라
# 조용히 줄어든 숫자로 나옵니다.
KEY_COLUMNS = ("trip_key", "driver_id", "customer_id", "lease_id", "taxi_id")

# 적재 후 검증이 반드시 확인할 컬럼. 전체 계약은 위 `COLUMNS` 가 소유하고, 여기는
# 키·계약 기간·파티션·차량 식별처럼 틀렸을 때 하류에서 조용히 새는 값만 봅니다.
REQUIRED_COLUMNS = frozenset(
    {
        *KEY_COLUMNS,
        "pickup_datetime",
        "lease_started_on",
        "lease_ended_on",
        "year_month",
        "make_key",
        "model_key",
        "model_year",
    }
)
