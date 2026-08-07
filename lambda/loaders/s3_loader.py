from typing import Any
from .base_loader import BaseLoader

class S3Loader(BaseLoader):
    def load(self, data: Any, bucket_name: str, object_key: str, **kwargs) -> None:
        """
        데이터를 AWS S3 버킷에 적재합니다.
        
        Args:
            data: 저장할 데이터 (로컬 파일 임시 경로 또는 데이터프레임)
            bucket_name: 대상 S3 버킷 이름 (예: BRONZE 영역 버킷)
            object_key: S3에 저장될 파일명과 경로 (예: raw/hvfhv/2024-01.parquet)
        """
        print(f"[S3Loader] '{bucket_name}' 버킷의 '{object_key}' 경로로 데이터를 적재합니다.")
        pass
