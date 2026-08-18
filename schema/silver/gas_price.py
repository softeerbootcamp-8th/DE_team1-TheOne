"""[뉴욕주 휘발유 요금] CLEAN Silver 스키마.

EIA 원본은 **주간 관측치**인데 여기는 **일별** 입니다. 하류(연료비 통합)가 날짜로
조인하므로 그 달 전 일수가 빠짐없이 있어야 하고, 하루라도 비면 그 날이 통째로 매칭에
실패합니다 — 에러가 아니라 조용히 줄어든 집계로 나타납니다.

각 날짜에는 **그 날 이하 가장 최근 주간 관측치**가 들어갑니다. 선형 보간하지 않는
이유는 EIA 주간값이 "그 주의 관측 평균" 이라 다음 관측까지 유효한 값으로 보는 편이
원 데이터에 가깝기 때문입니다.

생산자는 `main/aws_lambda/functions/eia_gas_price_bronze_to_silver`,
소비자는 연료비 통합 단계입니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
    ]
)
