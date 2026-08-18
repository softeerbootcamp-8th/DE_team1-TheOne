"""도메인 공용 S3 Bronze/Silver 읽기 헬퍼.

bronze_to_silver 함수들이 S3에서 여러 파일을 나열·조회할 때 쓰는 최소 프리미티브.
쓰기 쪽 대칭 구현체는 .s3_loader 참고.
"""

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
