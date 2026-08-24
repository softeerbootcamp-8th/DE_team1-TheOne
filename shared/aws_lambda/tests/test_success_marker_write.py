from shared.aws_lambda.common.atomic_write import invalidate_success_marker
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object


def test_로컬_재처리는_기존_SUCCESS를_무효화한다(tmp_path):
    directory = tmp_path / "year_month=2026-08"
    directory.mkdir()
    marker = directory / "_SUCCESS"
    marker.touch()

    invalidate_success_marker(directory)

    assert not marker.exists()


def test_S3_재처리는_SUCCESS를_먼저_지우고_데이터를_쓴다(monkeypatch):
    calls = []

    class FakeClient:
        def delete_object(self, **kwargs):
            calls.append(("delete", kwargs))

        def put_object(self, **kwargs):
            calls.append(("put", kwargs))

    monkeypatch.setattr(
        "shared.aws_lambda.common.s3_loader.boto3.client",
        lambda service: FakeClient(),
    )

    S3Loader(
        key="silver/x/year_month=2026-08/data.parquet",
        bucket="lake",
        invalidate_parent_success=True,
    ).write(S3Object(body=b"data", row_count=1))

    assert calls[0] == (
        "delete",
        {"Bucket": "lake", "Key": "silver/x/year_month=2026-08/_SUCCESS"},
    )
    assert calls[1][0] == "put"
    assert calls[1][1]["Key"] == "silver/x/year_month=2026-08/data.parquet"
