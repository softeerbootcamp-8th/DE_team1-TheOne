"""차종별 제원을 차량 대장/배차 목록과 조인 가능한 형태로 정제합니다.

Bronze 는 원본 CSV 를 그대로 실은 것이라 **값이 전부 문자열**이고 컬럼이 84개입니다.
Silver 에서 쓰는 컬럼만 골라 숫자로 바꾸고 조인 키를 붙입니다.

조인 키를 둘 만드는 이유:

    대장      "OUTLANDER SPORT"
    제원 model     "Outlander Sport 4WD"   <- 구동방식 접미사가 붙어 그대로는 안 붙음
    제원 baseModel "Outlander Sport"       <- 접미사가 빠진 값

`model_key` 로 먼저 붙이고 안 붙으면 `base_model_key` 로 떨어지는 식으로 쓰라고
둘 다 남깁니다. 어느 쪽을 쓸지는 조인하는 쪽(Gold)이 정합니다.

행을 합치지 않습니다. 원본은 같은 (연식, 제조사, 차종)에 대해 변속기/구동방식별로
여러 행을 갖는데, 그중 무엇을 대표로 삼을지는 정제가 아니라 집계 판단입니다.
Silver 는 원본 레코드 하나당 한 행을 유지하고 `source_id` 로 되짚게 합니다.

조인 키 규칙은 `common.join_keys` 를 따릅니다 (차량 대장과 반드시 동일해야 함).
"""

import logging
from datetime import datetime, timezone

from pipeline_core.transformer import Transformer

from shared.aws_lambda.common.join_keys import normalize_key

logger = logging.getLogger(__name__)

# 원본에 1984년식부터 들어 있습니다. 범위를 벗어나면 파싱 사고로 봅니다.
MIN_MODEL_YEAR = 1980
MAX_MODEL_YEAR = 2100

# 조인 키를 만들 수 없는 행은 건너뜁니다. 공공 CSV 5만 행이라 결측이 조금씩
# 섞이는데 전량 실패시키면 그달 수집이 통째로 날아갑니다.
# 다만 비율이 이 값을 넘으면 원본 구조가 바뀐 것으로 보고 실패시킵니다.
#
# 수집 주기가 월 1회로 짧아져(#311) 놓쳐도 다음 달에 다시 받습니다. 이 값을
# 더 조여도 되는지는 별도 판단이 필요합니다.
MAX_SKIP_RATIO = 0.01


def _as_utc(value: object) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ValueError("collected_at에 시간대가 없습니다")
    return parsed.astimezone(timezone.utc)


def _model_year(value: object) -> int:
    try:
        year = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"year가 숫자가 아닙니다: {value!r}") from exc
    if not MIN_MODEL_YEAR <= year <= MAX_MODEL_YEAR:
        raise ValueError(f"연식이 허용 범위를 벗어났습니다: {year}")
    return year


def _optional_float(value: object, column: str) -> float | None:
    """빈 값은 None. 원본은 해당 없는 항목을 "0" 으로 채워 보내기도 합니다."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError as exc:
        raise ValueError(f"{column}이 숫자가 아닙니다: {text!r}") from exc
    if number < 0:
        raise ValueError(f"{column}이 음수입니다: {number}")
    return number


class VehicleSpecsSilverTransformer(Transformer):
    """Bronze 제원을 조인 가능한 Silver 행 목록으로 정제합니다."""

    def transform(self, data: list[dict]) -> list[dict]:
        if not data:
            raise ValueError("변환할 차종별 제원 Bronze 데이터가 없습니다.")

        silver: list[dict] = []
        skipped: list[str] = []
        seen: set[tuple[str, str]] = set()

        for row in data:
            bronze_path = str(row.get("bronze_path") or "<unknown>")
            label = f"id={row.get('id')} {row.get('make')} {row.get('model')}"
            try:
                source = str(row.get("source") or "").strip()
                if not source:
                    raise ValueError("source가 비어 있습니다")

                source_id = str(row.get("id") or "").strip()
                if not source_id:
                    raise ValueError("id가 비어 있습니다")

                make_key = normalize_key(row.get("make"))
                model_key = normalize_key(row.get("model"))
                if not make_key or not model_key:
                    raise ValueError("make 또는 model이 비어 있습니다")

                identity = (source, source_id)
                if identity in seen:
                    raise ValueError(f"원본 id가 중복됩니다: {source_id}")
                seen.add(identity)

                silver.append(
                    {
                        "source": source,
                        "source_id": source_id,  # 원본 레코드 식별자 (계보)
                        "year": _model_year(row.get("year")),
                        # 조인 키 — 대장 / 배차 목록과 같은 규칙으로 만듭니다.
                        "make_key": make_key,
                        "model_key": model_key,
                        # 구동방식 접미사가 빠진 이름. model_key 로 안 붙을 때 쓰는 대안.
                        "base_model_key": normalize_key(row.get("baseModel")),
                        # 연비(MPG). 전기차는 MPGe(휘발유 환산)라 단위가 다릅니다.
                        "combined_mpg": _optional_float(row.get("comb08"), "comb08"),
                        # 전비(kWh/100mi). 내연기관은 0 으로 채워져 옵니다.
                        "combined_kwh_per_100mi": _optional_float(
                            row.get("combE"), "combE"
                        ),
                        # 주행거리(mile). 전기차/PHEV 만 값이 있습니다.
                        "range_miles": _optional_float(row.get("range"), "range"),
                        # EV / Plug-in Hybrid / Hybrid ... 내연기관은 비어 있습니다.
                        "atv_type": (str(row.get("atvType") or "").strip() or None),
                        "bronze_path": bronze_path,
                        # 아래는 적재하지 않고 검증에만 씁니다.
                        "collected_at": _as_utc(row.get("collected_at")),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                skipped.append(f"{bronze_path} {label}: {exc}")

        if not silver:
            raise ValueError("차종별 제원 Silver 변환 결과가 0건입니다.")

        skip_ratio = len(skipped) / len(data)
        if skip_ratio > MAX_SKIP_RATIO:
            # 앞의 몇 건만 보여줍니다. 수천 건이 실패하면 메시지가 로그를 덮습니다.
            head = "\n- ".join(skipped[:10])
            raise ValueError(
                f"건너뛴 행이 너무 많습니다: {len(skipped)}/{len(data)} "
                f"({skip_ratio:.1%} > {MAX_SKIP_RATIO:.1%}, 원본 구조 변경 의심)\n- {head}"
            )
        if skipped:
            logger.warning(
                "조인 키를 만들 수 없어 건너뛴 행: %d/%d (예: %s)",
                len(skipped),
                len(data),
                skipped[0],
            )

        collected_dates = {row["collected_at"].date() for row in silver}
        if len(collected_dates) != 1:
            raise ValueError("하나의 Bronze 스냅샷에 수집일이 섞여 있습니다.")

        logger.info("silver_transform done rows=%d skipped=%d", len(silver), len(skipped))
        return silver
