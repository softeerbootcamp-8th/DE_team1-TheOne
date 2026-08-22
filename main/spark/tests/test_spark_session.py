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
