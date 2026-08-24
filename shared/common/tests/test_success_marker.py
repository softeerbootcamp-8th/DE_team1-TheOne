from pathlib import Path

from shared.common.success_marker import (
    data_key_is_complete,
    data_path_is_complete,
    marker_key,
    marker_path,
)


def test_데이터는_같은_디렉터리의_SUCCESS가_있어야_완료다(tmp_path):
    data = tmp_path / "collected_at=x" / "data.parquet"
    data.parent.mkdir()
    data.touch()

    assert data_path_is_complete(data) is False

    marker_path(data.parent).touch()

    assert data_path_is_complete(data) is True


def test_S3_데이터도_같은_prefix의_SUCCESS가_있어야_완료다():
    key = "bronze/x/year_month=2026-08/collected_at=x/data.parquet"

    assert data_key_is_complete(key, {key}) is False
    assert data_key_is_complete(
        key,
        {key, marker_key(str(Path(key).parent))},
    ) is True
