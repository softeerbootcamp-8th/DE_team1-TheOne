"""main Spark job에서 쓰는 `service_area=` 경로 규칙."""

import re
from pathlib import Path


SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def service_area_segment(service_area: str) -> str:
    if not SERVICE_AREA_PATTERN.fullmatch(service_area):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return f"service_area={service_area}"


def join_segments(*segments: str | None) -> str:
    return "/".join(segment for segment in segments if segment)


def service_area_root(root: str | Path, service_area: str) -> Path:
    return Path(root) / service_area_segment(service_area)


def service_area_prefix(*head: str, service_area: str) -> str:
    return join_segments(*head, service_area_segment(service_area))


def gold_csv_path(
    output_dir: str,
    dataset: str,
    year_month: str,
    service_area: str,
) -> Path:
    dataset_root = Path(output_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area)
        / f"year_month={year_month}"
        / f"{dataset}.csv"
    )
