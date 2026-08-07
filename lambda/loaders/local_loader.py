from typing import Any
import shutil
import os
from .base_loader import BaseLoader

class LocalLoader(BaseLoader):
    def load(self, data: Any, target_path: str, **kwargs) -> None:
        
        # 1. 원본 데이터(파일 경로) 유효성 검사
        if not isinstance(data, str):
            raise ValueError(f"[LocalLoader] data 파라미터는 원본 파일 경로(str)여야 합니다. (현재 타입: {type(data)})")
            
        if not os.path.exists(data):
            raise FileNotFoundError(f"[LocalLoader] 원본 데이터 파일을 찾을 수 없습니다: {data}")

        # 2. 목적지 폴더가 없으면 재귀적으로 생성
        target_dir = os.path.dirname(target_path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)
            
        # 3. 데이터 파일을 목적지로 복사
        shutil.copy2(data, target_path)
