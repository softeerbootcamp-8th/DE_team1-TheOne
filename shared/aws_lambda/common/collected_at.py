"""수집 시각 해석.

크롤러 4종이 같은 규칙을 써야 해서 한 곳에 둡니다. 각자 `datetime.now()` 를 부르면
지정 일자 수집을 넣을 때 네 곳을 따로 고쳐야 하고, 실제로 그중 하나가 빠집니다.
"""

import re
from datetime import date, datetime, time, timezone

# 지정 일자로 수집할 때 쓰는 시각. 자정으로 고정해야 같은 날 두 번 돌려도 파티션과
# 행의 값이 같습니다 — 실행 시각을 쓰면 재실행마다 행이 달라집니다.
COLLECTION_TIME = time(0, 0, tzinfo=timezone.utc)

# DAG 파라미터의 pattern 과 같은 형식만 받습니다. `date.fromisoformat` 은 3.11 부터
# "20260501" 같은 압축 형식도 받아서, 핸들러를 직접 부르면 DAG 검증을 우회합니다.
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_collected_at(event: dict | None, *, now: datetime | None = None) -> datetime:
    """`event["collected_date"]` 가 있으면 그 날 00:00 UTC, 없으면 현재 시각.

    파티션 키와 행의 `collected_at` 이 **함께** 움직여야 합니다. Bronze 검증이 행에서
    뽑은 날짜와 파티션 날짜가 같은지 보기 때문에, 한쪽만 바꾸면 적재는 되고 검증에서
    죽습니다.

    돌려받은 값은 그대로 파티션 경로와 행에 쓰입니다 — 호출하는 쪽이 날짜를 다시
    계산하지 않게 하려는 것입니다.
    """
    requested = (event or {}).get("collected_date")
    if not requested:
        return now or datetime.now(timezone.utc)

    if isinstance(requested, date) and not isinstance(requested, datetime):
        return datetime.combine(requested, COLLECTION_TIME)

    text = str(requested).strip()
    if not _ISO_DATE.fullmatch(text):
        raise ValueError(f"collected_date 는 YYYY-MM-DD 여야 합니다: {requested!r}")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"collected_date 는 YYYY-MM-DD 여야 합니다: {requested!r}"
        ) from exc
    return datetime.combine(parsed, COLLECTION_TIME)
