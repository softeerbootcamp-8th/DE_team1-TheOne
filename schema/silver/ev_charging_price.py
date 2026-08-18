"""[뉴욕주 충전소 요금] CLEAN Silver 스키마.

EIA 전력 통계는 **월 단위 ¢/kWh** 인데 여기는 **일별 $/kWh** 입니다. 하류(연료비 통합)가
날짜로 조인하므로 그 달 전 일수가 빠짐없이 있어야 하고, 하루라도 비면 그 날이 통째로
매칭에 실패합니다 — 에러가 아니라 조용히 줄어든 집계로 나타납니다.

생산자는 `main/aws_lambda/functions/eia_electricity_price_bronze_to_silver`,
소비자는 연료비 통합 단계입니다.

계보 컬럼을 두지 않는 이유
------------------------
EIA 가 최근 약 17개월을 `Preliminary` 로 두고 나중에 `Final` 로 바꿉니다. 그 상태와
수집 시점은 **통합 단계 산출물**(`gas_ev_price`)이 이미 `bronze_collected_date`·
`ev_price_status` 로 싣고 있어, 여기서 또 들고 있으면 같은 사실이 두 곳에 남습니다.
이 표는 "그 날의 충전 단가" 하나만 책임집니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("ev_price", pa.float64()),
    ]
)
