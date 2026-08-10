"""데이터셋 간 차종 조인 키 규칙.

리스 업체 차량 대장(`vehicle_catalog`), 배차 가능 목록(`uber_eligible_vehicles`),
차종별 제원(`fueleconomy_vehicle_specs`) 은 같은 차를 서로 다르게 적습니다.

    대장    "OUTLANDER SPORT"   (카드 이미지 OCR 이라 전부 대문자)
    uber    "Outlander Sport"
    제원    "Outlander Sport 4WD" / baseModel "Outlander Sport"

표기를 예쁘게 고치는 대신 **대문자 조인 키를 따로** 만듭니다. 제목 형식으로
바꾸면 "RAV4" 가 "Rav4" 가 되는데 정작 여러 데이터셋이 "RAV4" 로 적고 있어
오히려 조인이 깨집니다. 어느 한쪽 표기를 정답으로 고르지 않고 양쪽을
대문자로 맞추는 편이 안전합니다.

규칙이 한 곳에 있어야 하는 이유: 한쪽만 바꾸면 조인 결과가 조용히 0건이
됩니다. 실패하지 않고 그냥 안 붙습니다.
"""

import re

# 연속 공백을 하나로 줄입니다. 카드 이미지 OCR 특성상 단어 사이 공백이
# 두 칸으로 들어오는 경우가 있습니다.
WHITESPACE_RE = re.compile(r"\s+")


def normalize_key(value: object) -> str | None:
    """조인용 키 — 앞뒤 공백 제거, 연속 공백 축약, 대문자. 빈 값은 None."""
    text = WHITESPACE_RE.sub(" ", str(value or "").strip())
    return text.upper() or None
