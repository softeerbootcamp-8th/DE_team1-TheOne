"""로컬과 EMR Serverless Spark 세션 설정 분리 시나리오.

1. 운영은 spark-submit이 정한 master와 driver 주소를 덮어쓰지 않음
2. 로컬은 local[3]과 loopback driver 주소를 유지
"""

from types import SimpleNamespace

from shared.spark.common import session as session_module


class FakeBuilder:
    def __init__(self):
        self.calls = []
        self.session = SimpleNamespace(
            conf=SimpleNamespace(set=lambda *args: None),
        )

    def appName(self, value):
        self.calls.append(("appName", value))
        return self

    def master(self, value):
        self.calls.append(("master", value))
        return self

    def config(self, name, value):
        self.calls.append(("config", name, value))
        return self

    def getOrCreate(self):
        return self.session


def _session_with(builder, monkeypatch, *, local_mode):
    monkeypatch.setattr(
        session_module,
        "SparkSession",
        SimpleNamespace(builder=builder),
    )
    return session_module.get_or_create_spark_session(
        "monthly_taxi_trip_bronze_to_silver",
        local_mode=local_mode,
    )


def test_운영은_EMR_spark_submit의_master와_driver주소를_덮지_않는다(monkeypatch):
    builder = FakeBuilder()

    _session_with(builder, monkeypatch, local_mode=False)

    assert not [call for call in builder.calls if call[0] == "master"]
    assert not [
        call
        for call in builder.calls
        if call[:2] in {
            ("config", "spark.driver.bindAddress"),
            ("config", "spark.driver.host"),
        }
    ]


def test_로컬은_local3과_loopback_driver주소를_유지한다(monkeypatch):
    builder = FakeBuilder()

    _session_with(builder, monkeypatch, local_mode=True)

    assert ("master", "local[3]") in builder.calls
    assert ("config", "spark.driver.bindAddress", "127.0.0.1") in builder.calls
    assert ("config", "spark.driver.host", "127.0.0.1") in builder.calls


# --- 이미지 타임존 데이터베이스 -------------------------------------------------

def test_spark_이미지는_tzdata를_marker없이_설치한다():
    """`applyInPandas` 가 EMR 에서 `ZoneInfoNotFoundError` 로 죽는 것을 막습니다.

    pandas 는 tzdata 를 `sys_platform == 'emscripten' or sys_platform == 'win32'`
    marker 로만 선언합니다. 그러면 `shared/spark/Dockerfile` 의 `uv export` 결과에도
    그 marker 가 붙어 Linux 이미지에서 pip 이 건너뛰고, EMR Serverless 베이스 이미지에는
    시스템 `/usr/share/zoneinfo` 가 없어 `zoneinfo.ZoneInfo("UTC")` 가 실패합니다.
    `spark.sql.session.timeZone` 을 UTC 로 고정하고 있어(#460) 그 변환을 반드시 탑니다.

    로컬에서는 시스템 tzdb 가 있어 재현되지 않으므로, 런타임 대신 **선언**을 봅니다.
    """
    import tomllib
    from pathlib import Path

    project = Path(__file__).resolve().parents[1]
    dependencies = tomllib.loads(
        (project / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["dependencies"]

    assert any(name.startswith("tzdata==") for name in dependencies), (
        "tzdata 를 직접 선언해야 uv export 가 marker 없이 내보냅니다: " f"{dependencies}"
    )

    lock = tomllib.loads((project / "uv.lock").read_text(encoding="utf-8"))
    (spark_package,) = [p for p in lock["package"] if p["name"] == "tlc-spark"]
    assert {"name": "tzdata"} in spark_package["dependencies"]
