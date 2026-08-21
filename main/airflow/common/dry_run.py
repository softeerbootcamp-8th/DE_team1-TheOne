"""Airflow dry-run Lambda 이벤트에 배포 저장소 설정을 붙입니다."""

import os


def configure_dry_run_event(event: dict, params: dict) -> dict:
    if params.get("dry_run") is not True:
        return event

    event["dry_run"] = True
    storage = os.getenv("RAW_STORAGE")
    bucket = os.getenv("DATA_LAKE_S3_BUCKET")
    if storage:
        event["storage"] = storage
    if bucket:
        event["bucket"] = bucket
    return event
