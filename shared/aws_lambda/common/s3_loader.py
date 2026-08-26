"""도메인 공용 S3 Bronze Loader.

직렬화(parquet/json/원문)는 도메인 로더가 맡고, 이 클래스는 이미 만들어진 bytes 를
정해진 bucket/key 에 그대로 올리는 역할만 합니다. S3 PutObject 는 객체 단위로
원자적이라 로컬 LocalXxxLoader 의 atomic_write 같은 임시파일 교체가 필요 없습니다.
"""

import os
from dataclasses import dataclass

import boto3
from pipeline_core.loader import Loader, WriteResult

from shared.common.env import load_local_env
from shared.common.success_marker import marker_key, quarantine_marker_key

BUCKET_ENV_VAR = "DATA_LAKE_S3_BUCKET"


@dataclass(frozen=True)
class S3Object:
    """S3Loader.write() 에 넘길 적재 단위.

    body: 이미 직렬화된 바이트(parquet/json/원문 등)
    row_count: 적재된 행 수. 행 개념이 없는 원문 저장이면 0.
    """

    body: bytes
    row_count: int = 0


class S3Loader(Loader):
    """bucket/key 로 지정된 위치에 bytes 를 그대로 PutObject 합니다."""

    def __init__(
        self,
        key: str,
        bucket: str | None = None,
        *,
        invalidate_parent_success: bool = False,
    ):
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._key = key
        self._client = boto3.client("s3")
        self._invalidate_parent_success = invalidate_parent_success

    def write(self, data: S3Object) -> WriteResult:
        location = f"s3://{self._bucket}/{self._key}"
        if self._invalidate_parent_success:
            parent = self._key.rsplit("/", 1)[0]
            self._client.delete_object(
                Bucket=self._bucket,
                Key=marker_key(parent),
            )
            self._client.delete_object(
                Bucket=self._bucket,
                Key=quarantine_marker_key(parent),
            )
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._key,
            Body=data.body,
            ServerSideEncryption="AES256",
        )
        return WriteResult(location=location, row_count=data.row_count)
