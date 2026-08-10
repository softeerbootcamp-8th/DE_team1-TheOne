"""리스 업체 보유 차량 대장을 다른 데이터셋과 조인 가능한 형태로 정제합니다.

Bronze 의 차종 표기는 업체 카드 이미지에서 읽은 값이라 전부 대문자입니다
("OUTLANDER SPORT"). 반면 차종별 제원(fueleconomy)과 배차 가능 목록(uber)은
"Outlander Sport" 처럼 일반 표기를 씁니다. 그대로는 붙지 않습니다.

표기를 예쁘게 고치는 대신 **대문자 조인 키를 따로 만듭니다.** 제목 형식으로
바꾸면 "RAV4" 가 "Rav4" 가 되는데, 정작 두 데이터셋 모두 "RAV4" 로 적고 있어
오히려 조인이 깨집니다. 어느 한쪽 표기를 정답으로 고르지 않고 양쪽을 대문자로
맞추는 편이 안전합니다.

원문(make/model/raw_name)은 그대로 남겨 추적할 수 있게 합니다.
"""

import math
import re
from datetime import datetime, timezone

# 조인 키에서 연속 공백을 하나로 줄입니다. 카드 이미지 OCR 특성상 단어 사이
# 공백이 두 칸으로 들어오는 경우가 있습니다.
WHITESPACE_RE = re.compile(r"\s+")

EXPECTED_PRICE_PERIOD = "week"
# 주간 렌트료로 볼 수 없는 값을 걸러냅니다. 관측된 범위는 514~749 USD 입니다.
MIN_WEEKLY_PRICE_USD = 50.0
MAX_WEEKLY_PRICE_USD = 5000.0


def _key(value: object) -> str | None:
    """조인용 키 — 앞뒤 공백 제거, 연속 공백 축약, 대문자."""
    text = WHITESPACE_RE.sub(" ", str(value or "").strip())
    return text.upper() or None


def _as_utc(value: object) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ValueError("collected_at에 시간대가 없습니다")
    return parsed.astimezone(timezone.utc)


def _weekly_price(value: object) -> float:
    try:
        price = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError("price_usd가 숫자가 아닙니다") from exc
    if not math.isfinite(price):
        raise ValueError("price_usd가 유효하지 않습니다")
    if not MIN_WEEKLY_PRICE_USD <= price <= MAX_WEEKLY_PRICE_USD:
        raise ValueError(f"주간 렌트료가 허용 범위를 벗어났습니다: {price}")
    return price


def transform(rows: list[dict]) -> list[dict]:
    """Bronze 차량 대장을 Silver 행 목록으로 반환합니다."""
    if not rows:
        raise ValueError("변환할 차량 대장 Bronze 데이터가 없습니다.")

    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    silver: list[dict] = []

    for row in rows:
        bronze_path = str(row.get("bronze_path") or "<unknown>")
        label = row.get("raw_name") or row.get("model") or "<unknown>"
        try:
            vendor = str(row.get("vendor") or "").strip()
            if not vendor:
                raise ValueError("vendor가 비어 있습니다")

            make_key = _key(row.get("make"))
            model_key = _key(row.get("model"))
            if not make_key or not model_key:
                # 이미지에서 차종을 못 읽은 행입니다. 조인 키를 만들 수 없습니다.
                raise ValueError("make 또는 model이 비어 있습니다")

            if row.get("price_period") != EXPECTED_PRICE_PERIOD:
                raise ValueError(f"price_period가 {EXPECTED_PRICE_PERIOD}가 아닙니다")

            identity = (vendor, make_key, model_key)
            if identity in seen:
                raise ValueError("같은 업체에 동일 차종이 중복됩니다")
            seen.add(identity)

            source_url = str(row.get("source_url") or "").strip()
            if not source_url:
                raise ValueError("source_url이 비어 있습니다")

            silver.append(
                {
                    "vendor": vendor,
                    # 조인 키 — 상대 데이터셋도 대문자로 맞춰서 붙입니다.
                    "make_key": make_key,
                    "model_key": model_key,
                    "weekly_price_usd": _weekly_price(row.get("price_usd")),
                    # 어느 Bronze 파일에서 나왔는지. 같은 날 여러 번 수집하면
                    # 파일이 여러 개라 파티션 경로만으로는 특정이 안 됩니다.
                    "bronze_path": bronze_path,
                    # 아래는 적재하지 않고 검증에만 씁니다.
                    "collected_at": _as_utc(row.get("collected_at")),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"{bronze_path} {label}: {exc}")

    if errors:
        raise ValueError("차량 대장 Silver 변환 실패:\n- " + "\n- ".join(errors))

    collected_dates = {row["collected_at"].date() for row in silver}
    if len(collected_dates) != 1:
        raise ValueError("하나의 Bronze 스냅샷에 수집일이 섞여 있습니다.")

    return silver
