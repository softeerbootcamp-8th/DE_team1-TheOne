"""도메인 공용 S3 Bronze/Silver 읽기 헬퍼.

bronze_to_silver 함수들이 S3에서 여러 파일을 나열·조회할 때 쓰는 최소 프리미티브.
쓰기 쪽 대칭 구현체는 s3_loader.py 참고.
"""

from typing import BinaryIO

import boto3


def list_keys(bucket: str, prefix: str) -> list[str]:
    """bucket/prefix 아래 모든 객체 키를 나열합니다. list_objects_v2 페이지네이션을 처리합니다."""
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def get_object_bytes(bucket: str, key: str) -> bytes:
    """bucket/key 객체 본문을 bytes 로 읽어옵니다."""
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def get_object_stream(bucket: str, key: str) -> tuple[BinaryIO, int]:
    """bucket/key 객체를 스트림으로 엽니다. 큰 파일을 통째로 메모리에 올리지 않으려는
    호출부(예: source_api)를 위한 것으로, `.read(size)`로 청크 단위로 읽고 다 쓰면
    `.close()` 해야 합니다. 작은 파일은 `get_object_bytes`를 쓰세요.
    """
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"], response["ContentLength"]


def is_s3_uri(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/key...` -> `(bucket, key)`.

    `s3a://` 도 받습니다 — Spark 쪽 설정에서 그 스킴을 쓰는 코드와 같은 문자열을
    pandas 경로로 넘기는 일이 있어서입니다.
    """
    if not is_s3_uri(uri):
        raise ValueError(f"s3:// 또는 s3a:// 로 시작해야 합니다: {uri!r}")
    without_scheme = uri.split("://", 1)[1]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"버킷과 키가 모두 필요합니다: {uri!r}")
    return bucket, key


def read_parquet_uri(uri: str, columns: list[str] | None = None):
    """`s3://` 또는 로컬 경로의 Parquet 을 DataFrame 으로 읽습니다.

    `pd.read_parquet` 에 `s3://` 를 그대로 넘기지 않는 이유는 `s3fs` 가 필요하고,
    그것이 `aiobotocore` 를 끌고 와 런타임의 `boto3` 핀과 충돌하기 때문입니다.

    `columns` 를 주면 그 컬럼만 pandas 로 올립니다. 몇 개만 쓰는데 전부 올리면
    조용히 비싼 이유는 **parquet 과 pandas 의 표현 비용이 다르기** 때문입니다 —
    문자열은 parquet 에서 dictionary 로 인코딩돼 몇 MiB 지만, pandas object dtype
    이 되면 행마다 파이썬 str 객체가 생겨 컬럼당 GB 급으로 부풀어 오릅니다.
    HVFHV 월별 원천(약 2천만 행)에서 실측한 비압축 크기가 근거입니다 (#894).

        전체 25개 컬럼            548 MiB
        실제로 쓰는 3개            106 MiB
        그중 문자열 8개 컬럼(parquet)  3 MiB  ← pandas 에서 폭증하는 것들

    객체 본문 자체는 여전히 통째로 내려받습니다. parquet 은 footer 랜덤 액세스가
    필요해 `get_object_stream` 의 `StreamingBody` 로는 읽을 수 없습니다.
    """
    import io

    import pandas as pd

    if not is_s3_uri(uri):
        return pd.read_parquet(uri, columns=columns)
    bucket, key = parse_s3_uri(uri)
    return pd.read_parquet(io.BytesIO(get_object_bytes(bucket, key)), columns=columns)


def parent_uri(uri: str, levels: int = 1) -> str:
    """경로에서 상위 `levels` 단계를 올라갑니다. `s3://` 를 보존합니다.

    `pathlib.Path` 를 쓰면 `s3://b/x` 가 `s3:/b/x` 로 뭉개져 스킴이 깨집니다
    (`//` 를 하나로 접습니다).
    """
    if not is_s3_uri(uri):
        from pathlib import Path

        path = Path(uri)
        for _ in range(levels):
            path = path.parent
        return str(path)

    scheme, _, rest = uri.partition("://")
    parts = rest.rstrip("/").split("/")
    if len(parts) <= levels:
        raise ValueError(f"{levels} 단계를 올라갈 수 없습니다: {uri!r}")
    return f"{scheme}://" + "/".join(parts[:-levels])
