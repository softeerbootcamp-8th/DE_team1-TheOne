import os
import time
from typing import Optional

from pyspark.sql import SparkSession

# 타임존을 고정하는 이유
# --------------------
# 아무것도 지정하지 않으면 Spark 세션은 JVM 기본값(= 머신 TZ)을 씁니다. 그러면 같은
# 코드·같은 입력이 개발자마다 다른 결과를 냅니다.
#
# Silver 의 `pickup_datetime` 은 뉴욕 현지 벽시계가 **INT96** 으로 담긴 값이고, INT96 은
# 읽는 쪽 세션 타임존에 반응합니다. Gold 는 `to_date(pickup_datetime)` 을 연료비의
# `date` 에 조인하므로, 밀린 만큼 그 달 마지막 날 운행이 다음 달로 넘어가 가격을 못
# 만나고 조인이 깨집니다 (실측: Asia/Seoul 머신에서 2025-05 운행 254,848건 중
# 5,091건 = 2.0%).
#
# 저장된 벽시계를 변형 없이 읽는 UTC 로 맞춥니다. `America/New_York` 으로 두면 이미
# 현지 시각인 값을 한 번 더 당기게 되어 오히려 틀립니다.
SESSION_TIME_ZONE = "UTC"

# 프로세스 TZ 도 같이 고정해야 하는 이유
# ---------------------------------
# 세션만 고정하면 **비대칭**이 생깁니다. `createDataFrame` 은 파이썬 datetime 을
# 내부 값으로 바꿀 때 세션 타임존이 아니라 **프로세스 TZ** 를 쓰고, `to_date` 는 세션
# 타임존을 씁니다. 둘이 다르면 파이썬으로 만든 데이터가 읽을 때 밀립니다 — 실제로
# 세션만 UTC 로 고정했을 때 파이썬 쪽에서 데이터를 만드는 테스트 5개가 깨졌습니다.
# 같은 이유로 allocator 처럼 파이썬에서 `.date()` 를 쓰는 코드도 함께 어긋납니다.
PROCESS_TIME_ZONE = "UTC"


def _pin_process_time_zone() -> None:
    """파이썬 쪽 시각 변환도 세션과 같은 타임존을 쓰게 맞춥니다."""
    if os.environ.get("TZ") != PROCESS_TIME_ZONE:
        os.environ["TZ"] = PROCESS_TIME_ZONE
        # tzset 은 Unix 전용입니다. 없으면 환경변수만 남기고 넘어갑니다.
        if hasattr(time, "tzset"):
            time.tzset()


# hadoop-aws/aws-java-sdk-bundle 버전. pyspark==3.5.6 이 번들한 Hadoop 클라이언트가
# 3.3.4 라 그 버전에 맞춥니다 — hadoop-aws 와 번들 Hadoop 버전이 어긋나면 흔히
# NoSuchMethodError 로 늦게, 애매하게 실패합니다(#712).
#
# main/spark/pyproject.toml 의 pyspark 버전을 올리면 이 값도 같이 확인하세요 —
# uv 가 관리하는 pip 의존성이 아니라 spark.jars.packages 로 받는 Maven 좌표라
# uv.lock 이 어긋남을 잡아주지 않습니다. 번들 버전은
# `jars/hadoop-client-api-*.jar` 파일명으로 확인합니다.
_S3A_HADOOP_VERSION = "3.3.4"
_S3A_AWS_SDK_VERSION = "1.12.262"


def get_or_create_spark_session(
    app_name: str,
    driver_memory: Optional[str] = None,
    *,
    local_mode: bool = True,
    enable_s3: bool = False,
) -> SparkSession:
    """로컬은 local[3], 운영은 spark-submit이 지정한 EMR 세션을 사용합니다.

    `enable_s3=True` 는 로컬 pyspark(pip 설치본)에 hadoop-aws/aws-java-sdk-bundle
    을 얹어 `s3://` 를 읽을 수 있게 합니다 — EMR 은 EMRFS 가 이미 있어 필요 없고,
    이미 만들어진 세션(EMR)에서는 이 config 들이 `getOrCreate()` 에서 조용히
    무시되므로 운영에는 영향이 없습니다(#712).
    """
    _pin_process_time_zone()

    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.session.timeZone", SESSION_TIME_ZONE
    )
    if local_mode:
        builder = (
            builder.master("local[3]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            # bindAddress 만 두면 듣는 주소와 알리는 주소가 갈립니다.
            # 로컬 컨테이너에서는 둘 다 loopback 으로 맞춰야 broadcast join 이
            # 광고된 컨테이너 주소로 접속하다 실패하지 않습니다.
            .config("spark.driver.host", "127.0.0.1")
        )
    if driver_memory:
        builder = builder.config("spark.driver.memory", driver_memory)
    if enable_s3:
        builder = (
            builder.config(
                "spark.jars.packages",
                f"org.apache.hadoop:hadoop-aws:{_S3A_HADOOP_VERSION},"
                f"com.amazonaws:aws-java-sdk-bundle:{_S3A_AWS_SDK_VERSION}",
            )
            # 기존 코드가 전부 s3:// 를 쓰므로(EMRFS 관례), s3:// 도 S3A 로
            # 풀어야 호출부를 하나도 안 고칩니다. s3a:// 도 그대로 받습니다.
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
    session = builder.getOrCreate()
    # getOrCreate 는 이미 만들어진 세션을 돌려줄 수 있고, 그때는 위 config 가 무시됩니다.
    # 타임존은 런타임에 바꿀 수 있으므로 반환 직전에 다시 못박습니다.
    session.conf.set("spark.sql.session.timeZone", SESSION_TIME_ZONE)
    return session
