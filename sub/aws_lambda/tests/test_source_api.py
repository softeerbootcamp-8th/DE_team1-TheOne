"""원천 API 의 라우팅·경로 탈출 방어 (#591).

`main` 이 원천 내부를 직접 안 읽고 이 API 만 쓰는 것이 설계 전제라, 여기가 뚫리면
경계 자체가 무의미해집니다. 그런데 지금까지 커버는 정상 다운로드 한 건뿐이었습니다.

경로 탈출 방어는 `path.parent.parent != release_root` 라는 **로컬 파일시스템 전제**로
짜여 있습니다. AWS 로 옮기면 S3 키 기준으로 다시 짜야 하므로, 그 전에 기대 동작을
여기 고정해 둡니다.

1. 정상 다운로드 — 바이트·Content-Type
2. `latest` 307 리다이렉트, 릴리스가 없으면 404
3. 경로 탈출 — manifest 의 `file` 이 밖을 가리켜도 404
4. 라우팅 거부 — 모르는 데이터셋·대문자·한 자리 월·datasets 없는 경로·루트
5. manifest 상태 — 없으면 404, 깨졌으면 500
6. `/health`·HEAD·쿼리스트링·트레일링 슬래시
"""

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from sub.source_api.server import DATASETS, create_server

YEAR_MONTH = "2026-01"
BODIES = {name: f"PAR1-{name}".encode() for name in sorted(DATASETS)}


@pytest.fixture
def release_root(tmp_path):
    release = tmp_path / f"year_month={YEAR_MONTH}"
    release.mkdir()
    for name, body in BODIES.items():
        (release / f"{name}.parquet").write_bytes(body)
    _write_manifest(release, {n: {"file": f"{n}.parquet"} for n in BODIES})
    # 릴리스 **밖**의 파일 — 경로 탈출이 성공하면 이게 새어 나갑니다.
    (tmp_path.parent / "SECRET.txt").write_text("top secret")
    return tmp_path


def _write_manifest(release: Path, datasets: dict) -> None:
    (release / "manifest.json").write_text(
        json.dumps({"year_month": YEAR_MONTH, "seed": 42, "datasets": datasets}),
        encoding="utf-8",
    )


@pytest.fixture
def api(release_root):
    server = create_server(release_root, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def raw_get(base: str, path: str) -> tuple[int, bytes]:
    """정규화 없이 그대로 보냅니다. `urllib` 은 `..` 를 지워버립니다."""
    import socket
    from urllib.parse import urlsplit

    parts = urlsplit(base)
    with socket.create_connection((parts.hostname, parts.port), timeout=5) as sock:
        sock.sendall(
            f"GET {path} HTTP/1.1\r\nHost: {parts.netloc}\r\nConnection: close\r\n\r\n".encode()
        )
        chunks = []
        while chunk := sock.recv(65536):
            chunks.append(chunk)
    raw = b"".join(chunks)
    status = int(raw.split(b" ", 2)[1])
    return status, raw


def get(base: str, path: str, method: str = "GET"):
    """(상태코드, 본문, 최종URL). 4xx·5xx 는 예외 대신 값으로 돌려줍니다."""
    request = urllib.request.Request(f"{base}{path}", method=method)
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read(), response.geturl()
    except urllib.error.HTTPError as error:
        return error.code, error.read(), None


@pytest.mark.parametrize("dataset", sorted(BODIES))
def test_공개_데이터셋은_바이트_그대로_내려간다(api, dataset):
    status, body, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/{dataset}")

    assert (status, body) == (200, BODIES[dataset])


def test_latest_는_실제_월_URL_로_리다이렉트한다(api):
    status, body, final = get(api, "/v1/data/latest/datasets/hvfhv_taxi_trips")

    assert status == 200
    assert final.endswith(f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips")
    assert body == BODIES["hvfhv_taxi_trips"]


def test_릴리스가_하나도_없으면_latest_는_404(tmp_path):
    server = create_server(tmp_path, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, _, _ = get(
            f"http://127.0.0.1:{server.server_port}",
            "/v1/data/latest/datasets/hvfhv_taxi_trips",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    assert status == 404


@pytest.mark.parametrize(
    "escape",
    ["../../SECRET.txt", "../SECRET.txt", "/etc/passwd", "sub/../../SECRET.txt"],
)
def test_manifest_가_릴리스_밖을_가리켜도_내보내지_않는다(api, release_root, escape):
    """manifest 는 우리가 만들지만, 그게 뚫리면 릴리스 밖 파일이 새어 나갑니다."""
    _write_manifest(
        release_root / f"year_month={YEAR_MONTH}", {"hvfhv_taxi_trips": {"file": escape}}
    )

    status, body, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips")

    assert status == 404
    assert b"secret" not in body.lower()


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/v1/data/2026-01",                                   # datasets 없음
        "/v1/data/2026-1/datasets/hvfhv_taxi_trips",          # 한 자리 월
        "/v1/data/202601/datasets/hvfhv_taxi_trips",          # 구분자 없음
        "/v1/data/2026-01/datasets/HVFHV_TAXI_TRIPS",         # 대문자
        "/v1/data/2026-01/datasets/manifest",                 # 내부 파일
        "/v1/data/2026-01/datasets/nope",                     # 없는 데이터셋
    ],
)
def test_계약_밖_경로는_거부한다(api, path):
    status, _, _ = get(api, path)

    assert status == 404


def test_manifest_에_있어도_공개_목록_밖이면_안_내보낸다(api, release_root):
    """manifest 는 내부 문서라 공개 대상이 아닌 항목이 들어올 수 있습니다.

    화이트리스트를 지워도 `nope` 같은 이름은 파일이 없어 어차피 404 라, 그것만으로는
    화이트리스트가 살아 있는지 알 수 없습니다. **실재하는 파일**을 manifest 에 얹어야
    화이트리스트가 유일한 방어선이 됩니다.
    """
    release = release_root / f"year_month={YEAR_MONTH}"
    (release / "internal_notes.parquet").write_bytes(b"PAR1-internal")
    datasets = {n: {"file": f"{n}.parquet"} for n in BODIES}
    datasets["internal_notes"] = {"file": "internal_notes.parquet"}
    _write_manifest(release, datasets)

    status, body, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/internal_notes")

    assert status == 404
    assert b"internal" not in body


@pytest.mark.parametrize(
    "year_month",
    ["../..", "2026-01/../../..", ".", "%2e%2e%2f%2e%2e"],
)
def test_월_구간으로_디렉터리를_벗어날_수_없다(api, year_month):
    """월 형식이 느슨해지면 이 구간이 경로 탈출 통로가 됩니다.

    `urllib` 이 `..` 를 정규화해 보내므로 일반 클라이언트로는 서버까지 닿지 않습니다.
    공격자는 정규화하지 않으니 **raw 소켓**으로 보냅니다.
    """
    status, body = raw_get(api, f"/v1/data/{year_month}/datasets/hvfhv_taxi_trips")

    assert status == 404
    assert b"secret" not in body.lower()


def test_없는_월은_404(api):
    status, _, _ = get(api, "/v1/data/2099-12/datasets/hvfhv_taxi_trips")

    assert status == 404


def test_manifest_가_없으면_404(api, release_root):
    (release_root / f"year_month={YEAR_MONTH}" / "manifest.json").unlink()

    status, _, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips")

    assert status == 404


def test_manifest_가_깨졌으면_500(api, release_root):
    """404 로 뭉개면 "릴리스가 없음"과 "릴리스가 망가짐"을 구분할 수 없습니다."""
    (release_root / f"year_month={YEAR_MONTH}" / "manifest.json").write_text("{ broken")

    status, _, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips")

    assert status == 500


def test_health_는_상태를_돌려준다(api):
    status, body, _ = get(api, "/health")

    assert (status, json.loads(body)) == (200, {"status": "ok"})


def test_HEAD_는_본문_없이_길이만_준다(api):
    status, body, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips", "HEAD")

    assert (status, body) == (200, b"")


@pytest.mark.parametrize("suffix", ["/", "?x=1"])
def test_트레일링_슬래시와_쿼리스트링을_무시한다(api, suffix):
    status, body, _ = get(api, f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips{suffix}")

    assert (status, body) == (200, BODIES["hvfhv_taxi_trips"])
