"""main Spark job에서 쓰는 `service_area=` 경로 규칙."""

import re
from pathlib import Path


SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def service_area_segment(service_area: str | None) -> str:
    if service_area is None:
        return ""
    if not SERVICE_AREA_PATTERN.fullmatch(service_area or ""):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return f"service_area={service_area}"


def join_segments(*segments: str | None) -> str:
    return "/".join(segment for segment in segments if segment)


def candidate_segments(service_area: str | None) -> tuple[str | None, ...]:
    if service_area is None:
        return (None,)
    return (service_area_segment(service_area), None)


def candidate_roots(
    root: str | Path, service_area: str | None = None
) -> tuple[Path, ...]:
    base = Path(root)
    return tuple(
        base / segment if segment else base
        for segment in candidate_segments(service_area)
    )


def candidate_prefixes(
    *head: str, service_area: str | None = None
) -> tuple[str, ...]:
    return tuple(
        join_segments(*head, segment) if segment else join_segments(*head)
        for segment in candidate_segments(service_area)
    )


def gold_csv_path(
    output_dir: str,
    dataset: str,
    year_month: str,
    service_area: str | None = None,
) -> Path:
    dataset_root = Path(output_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area if area else dataset_root)
        / f"year_month={year_month}"
        / f"{dataset}.csv"
    )
