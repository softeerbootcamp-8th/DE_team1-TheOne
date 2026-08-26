"""Silver 입력 파일의 내용 digest.

fingerprint 가 입력 경로만 보면 같은 버전 디렉터리를 다른 내용으로 재발행했을 때
예전 Gold 를 재사용합니다(#1088). 그래서 적재 전에 실제 객체 바이트를 해시해
경로와 함께 fingerprint 에 반영합니다.

버전 디렉터리는 키 정렬 순서로 파일 이름 + 본문을 이어 해시합니다 — 파일 추가·
삭제·내용 변경이 모두 digest 를 바꿉니다. 본문은 청크로 스트리밍해 월간 parquet
(수백 MB)도 메모리에 통째로 올리지 않습니다.
"""

import hashlib
from collections.abc import Callable
from pathlib import Path

from shared.common.s3_reader import (
    get_object_stream,
    is_s3_uri,
    list_keys,
    parse_s3_uri,
)

_CHUNK_BYTES = 1024 * 1024


def _hash_named_files(files: list[tuple[str, Callable[[], object]]]) -> str:
    """(이름, 열기 함수) 목록을 정렬해 이름과 본문을 하나의 SHA-256 으로 묶습니다."""
    digest = hashlib.sha256()
    for name, opener in sorted(files, key=lambda item: item[0]):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        with opener() as handle:
            while True:
                chunk = handle.read(_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def silver_input_digest(path: str) -> str:
    """단일 파일 또는 버전 디렉터리의 내용 SHA-256.

    S3 는 디렉터리가 없으므로 경로 접두사로 객체를 나열해 같은 규칙을 적용합니다.
    `_SUCCESS` 같은 마커도 입력 상태의 일부라 함께 해시합니다.
    """
    if is_s3_uri(path):
        bucket, key = parse_s3_uri(path)
        base_key = key.rstrip("/")
        # 접두사 경계를 "/" 로 고정하지 않으면 version_dir 이 version_dir_old 도
        # 함께 나열합니다.
        candidates = [
            candidate
            for candidate in list_keys(bucket, f"{base_key}/")
            if candidate != f"{base_key}/"
        ]
        if not candidates:
            # 파일 경로(fuel.parquet)로 직접 들어온 경우입니다.
            candidates = [
                candidate
                for candidate in list_keys(bucket, base_key)
                if candidate == base_key
            ]
        if not candidates:
            raise FileNotFoundError(f"Silver 입력이 비어 있습니다: {path}")
        files = [
            (
                candidate,
                lambda captured=candidate: get_object_stream(bucket, captured)[0],
            )
            for candidate in candidates
        ]
        return _hash_named_files(files)

    root = Path(path)
    if root.is_file():
        return _hash_named_files([(root.name, lambda target=root: target.open("rb"))])
    if root.is_dir():
        files = [
            (
                str(child.relative_to(root)),
                lambda target=child: target.open("rb"),
            )
            for child in sorted(root.rglob("*"))
            if child.is_file()
        ]
        if not files:
            raise FileNotFoundError(f"Silver 입력이 비어 있습니다: {path}")
        return _hash_named_files(files)
    raise FileNotFoundError(f"Silver 입력이 없습니다: {path}")
