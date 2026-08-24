"""EIA 연료비 Silver 버전 이름 계약."""

import re


COLLECTED_AT_TOKEN_PATTERN = re.compile(r"^\d{8}T\d{12}Z$")
SOURCE_COLLECTED_AT_PATTERN = re.compile(
    r"^source_collected_at=(\d{8}T\d{12}Z)$"
)
INPUT_VERSION_PATTERN = re.compile(
    r"^input_version=gas-(\d{8}T\d{12}Z)__ev-(\d{8}T\d{12}Z)$"
)
FUEL_FILE_NAME = "ny_fuel.parquet"


def require_collected_at_token(token: str) -> str:
    if not COLLECTED_AT_TOKEN_PATTERN.fullmatch(token):
        raise ValueError(f"수집 버전 토큰이 올바르지 않습니다: {token!r}")
    return token


def source_collected_at_token(segment: str) -> str | None:
    match = SOURCE_COLLECTED_AT_PATTERN.fullmatch(segment)
    return match.group(1) if match else None


def fuel_input_version(gas_token: str, ev_token: str) -> str:
    gas = require_collected_at_token(gas_token)
    ev = require_collected_at_token(ev_token)
    return f"input_version=gas-{gas}__ev-{ev}"


def fuel_source_tokens(segment: str) -> tuple[str, str] | None:
    match = INPUT_VERSION_PATTERN.fullmatch(segment)
    return match.groups() if match else None
