"""가짜 기사·운행·보유 차량 월별 릴리스를 내려주는 HTTP API."""

from __future__ import annotations

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

DATASETS = {
    "hvfhv_taxi_trips",
    "driver_vehicle_monthly_snapshot",
    "lease_vehicle_inventory",
}
DATASET_PATTERN = re.compile(r"^/v1/data/(\d{4}-\d{2})/datasets/([a-z_]+)$")
LATEST_DATASET_PATTERN = re.compile(r"^/v1/data/latest/datasets/([a-z_]+)$")


class ReleaseRequestHandler(BaseHTTPRequestHandler):
    release_root = Path("data/source/synthetic_driver_trip_api")

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
        self._route(head_only=False)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler contract
        self._route(head_only=True)

    def _route(self, *, head_only: bool) -> None:
        path = urlsplit(self.path).path.rstrip("/") or "/"
        if path == "/health":
            self._send_json({"status": "ok"}, head_only=head_only)
            return
        if match := LATEST_DATASET_PATTERN.fullmatch(path):
            releases = sorted(self.release_root.glob("year_month=????-??"))
            if not releases:
                self.send_error(404, "data not found")
                return
            year_month = releases[-1].name.removeprefix("year_month=")
            self.send_response(307)
            self.send_header(
                "Location", f"/v1/data/{year_month}/datasets/{match.group(1)}"
            )
            self.end_headers()
            return
        if match := DATASET_PATTERN.fullmatch(path):
            self._send_dataset(match.group(1), match.group(2), head_only=head_only)
            return
        self.send_error(404, "endpoint not found")

    def _manifest(self, release: Path) -> dict | None:
        path = release / "manifest.json"
        if not path.is_file():
            self.send_error(404, "release not found")
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            self.send_error(500, "invalid release manifest")
            return None

    def _send_dataset(self, year_month: str, dataset: str, *, head_only: bool) -> None:
        if dataset not in DATASETS:
            self.send_error(404, "dataset not found")
            return
        release = self.release_root / f"year_month={year_month}"
        manifest = self._manifest(release)
        if manifest is None:
            return
        metadata = manifest.get("datasets", {}).get(dataset, {})
        self._send_file(
            release / str(metadata.get("file", "")),
            head_only=head_only,
        )

    def _send_json(self, value: dict, *, head_only: bool) -> None:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_file(self, path: Path, *, head_only: bool) -> None:
        if not path.is_file() or path.parent.parent != self.release_root:
            self.send_error(404, "dataset file not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.apache.parquet")
        self.send_header("Content-Length", str(path.stat().st_size))
        self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.end_headers()
        if not head_only:
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    self.wfile.write(chunk)

    def log_message(self, format: str, *args) -> None:
        print(f"source-api: {format % args}")


def create_server(
    release_root: str | Path,
    host: str = "127.0.0.1",
    port: int = 8091,
) -> ThreadingHTTPServer:
    # ponytail: 팀 내부 가짜 원천용 stdlib 서버. 외부 트래픽이 생기면 object storage로 교체합니다.
    handler = type(
        "ConfiguredReleaseRequestHandler",
        (ReleaseRequestHandler,),
        {"release_root": Path(release_root).resolve()},
    )
    return ThreadingHTTPServer((host, port), handler)


def main(args_list: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="가짜 기사-운행 원천 다운로드 API")
    parser.add_argument(
        "--root",
        default=os.getenv(
            "SOURCE_RELEASE_DIR",
            "data/source/synthetic_driver_trip_api",
        ),
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args(args_list)
    server = create_server(args.root, args.host, args.port)
    print(f"source API: http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
