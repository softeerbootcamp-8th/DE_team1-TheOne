"""[연료비] 통합 Silver 스키마.

휘발유와 전기 단가를 날짜 하나에 나란히 담습니다. Gold 가 운행 날짜로 조인하므로
**그 달 전 일수가 빠짐없이** 있어야 합니다. 하루라도 비면 그 날 운행이 통째로
매칭 실패하고, 그건 에러가 아니라 조용히 줄어든 집계로 나타납니다.

생산자는 EIA 공개 통계 하나입니다 — 이력 파일에서 대상 월을 뽑아 씁니다. 전에는
AAA·NLR 크롤링도 같은 자리에 썼는데, 매일 수집해야 그 달이 채워지는 구조라 과거를
채울 수 없고 한 달을 완성하려면 60번의 수집 성공이 필요해서 접었습니다.

계보 컬럼이 필요한 이유
---------------------
같은 달을 두 번 만들면 숫자가 다를 수 있습니다. EIA 가 전력 통계의 최근 약 17개월을
`Preliminary` 로 표시하고 나중에 `Final` 로 확정하기 때문입니다. 변환은 항상 가장
최근 수집분을 쓰므로, 시간이 지나 다시 만들면 확정값으로 바뀝니다.

그 변화는 감출 것이 아니라 설명할 수 있어야 합니다. 그래서 세 컬럼을 함께 씁니다.

    price_source            어디서 왔는지 (역할이 아니라 출처)
    bronze_collected_date   어느 수집분으로 만들었는지
    ev_price_status         그 달 전력값이 잠정인지 확정인지

`ev_price_status` 가 전기 쪽에만 있는 것은 휘발유 원본에 상태 표시가 없기 때문입니다
(주간 소매가 시계열에 날짜·가격 두 컬럼뿐).
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
        ("ev_price", pa.float64()),
        ("price_source", pa.string()),
        ("bronze_collected_date", pa.date32()),
        ("ev_price_status", pa.string()),
    ]
)

# 출처 이름. 역할("backfill")이 아니라 **어디서 왔는지**로 둡니다.
EIA = "eia"

# EIA 가 파일에 직접 적는 값입니다. 우리가 정하는 것이 아니라 읽어서 옮깁니다.
PRELIMINARY = "Preliminary"
FINAL = "Final"
