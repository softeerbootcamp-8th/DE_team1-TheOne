import sys
import pyspark.util

# PySpark Connect 모드 자동 전환 방지
pyspark.util._is_remote_only = False

from jobs.bronze_to_silver.hvfhv.job import main

if __name__ == "__main__":
    main(sys.argv[1:])
