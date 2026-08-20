"""blue_print.md 파이프라인의 실행 가능한 프로토타입.

**목적은 성능이 아니라 "이 설계가 끝까지 돌아가는가"와 "매칭이 얼마나 되는가"입니다.**

기존 `sub/spark/jobs/driver_assignment` 는 같은 일을 Spark 로 합니다. 이 패키지가
따로 있는 이유는 로컬에 JRE 가 없어 Spark 를 띄울 수 없기 때문입니다. pandas 로
같은 알고리즘을 돌려 설계의 성립 여부와 매칭률을 먼저 측정하고, 숫자가 납득되면
Spark 경로에 옮깁니다.

blue_print.md 의 5단계를 그대로 모듈로 나눴습니다.

    curated.py      3 · curated    — 검증된 실데이터 (크롤링 + TLC)
    synthesize.py   4A · synthesize — 합성 개입 (기사·배정·lifecycle)
    attribution.py  4B · attribution — 실 트립 + 합성 신원, 제약 검증 6종
    published.py    5 · published   — data contract 3종
    metrics.py      매칭 품질 측정
    run.py          한 달 실행 + 월별 상태 체크포인트
"""

from __future__ import annotations

import time

_START = time.perf_counter()


def log(message: str, *, indent: int = 2) -> None:
    """경과 시간을 붙여 한 줄 찍습니다.

    `flush=True` 가 핵심입니다. Airflow 의 `BashOperator` 는 stdout 을 파이프로
    받는데, 그러면 파이썬이 블록 버퍼링(8KB)으로 바뀌어 몇 분치 로그가 한꺼번에
    나옵니다. "돌고 있는지 죽었는지 모르겠다"의 절반이 그 버퍼입니다.

    절대 시각이 아니라 **경과**를 찍습니다. 어느 단계가 오래 걸리는지 보려면
    시각을 빼는 계산을 사람이 하게 만들 이유가 없습니다.
    """
    elapsed = time.perf_counter() - _START
    print(f"{' ' * indent}[{int(elapsed // 60):02d}:{elapsed % 60:04.1f}] {message}", flush=True)
