"""[뉴욕주 충전소 요금] CLEAN Silver 스키마.

EIA 전력 통계는 **월 단위 ¢/kWh** 인데 여기는 **일별 $/kWh** 입니다. 하류(연료비 통합)가
날짜로 조인하므로 그 달 전 일수가 빠짐없이 있어야 하고, 하루라도 비면 그 날이 통째로
매칭에 실패합니다 — 에러가 아니라 조용히 줄어든 집계로 나타납니다.

생산자는 `main/aws_lambda/functions/eia_electricity_price_bronze_to_silver`,
소비자는 연료비 통합 단계입니다.

계보 컬럼
--------
`ev_price_status` 는 EIA 가 그 달을 `Preliminary` 로 뒀는지 `Final` 로 확정했는지입니다
(최근 약 17개월이 잠정). `bronze_collected_date` 는 어느 수집분으로 만들었는지고요.
같은 달을 다시 만들었을 때 숫자가 달라지는 원인이 이 둘이라, 남겨두지 않으면
"지난번과 왜 다르지" 에 답할 수 없습니다.

통합 단계가 아니라 **여기** 두는 이유는, 두 값이 통합의 성질이 아니라 전력 원천의
성질이기 때문입니다 — 휘발유 원본에는 상태 표시가 아예 없고(주간 소매가에 날짜·가격
두 컬럼뿐), 수집일도 두 원천이 서로 다릅니다 (#518).
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("ev_price", pa.float64()),
        ("bronze_collected_date", pa.date32()),
        ("ev_price_status", pa.string()),
    ]
)
