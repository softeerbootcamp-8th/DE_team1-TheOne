# silver_to_gold가 EMR 워커 메모리 초과로 죽음 (exit 137)

## 증상

`monthly_taxi_trip_silver_to_gold` DAG의 EMR Serverless job이 실패.

```
EMR Serverless job 실패: FAILED - Job failed. ExitCode: 137. Last few exceptions:
Worker has been killed as memory usage exceeded configured memory size,
consider increasing memory size...
```

죽기 직전 마지막 SQL 활동은 `_driver_rank#249689 = 6` 필터였고, 그 시점 driver
broadcast 카운터는 379, `MemoryStore` 여유 공간은 3.4GiB → 483.9MiB로 줄어든
상태였음. 최종 산출 행 수(기사 2000 × 조합 6 × 차량 12대 ≈ 14만 행)로는 설명이
안 되는 크기.

## 원인

`recommendation_algorithm/base.py`의 `_allocate_candidates_by_stock` 라운드
루프가 `RevenueFirstAlgorithm`의 threshold 5개마다 처음부터 다시 호출됨
(v1 1회 + v2 threshold 5회 = 6회, #997 이후). 매 라운드마다 `assigned` driver_id
목록·`occupied_stock`·`used_stock` 조인이 자동 broadcast join으로 처리되는데
명시적 정리가 없어, 호출 6번 동안 정리 속도를 못 따라가고 broadcast가 계속
쌓임(broadcast_379까지 확인).

## 해결

`_allocate_candidates_by_stock`에 `group_columns` 파라미터를 추가해 threshold를
candidates의 차원(crossJoin)으로 얹고 랭킹·`used_stock`·재고 경쟁 윈도우의
partition/join 키에 포함시켜, threshold 5개를 **한 번의 라운드 루프**로 배정.
`occupied_stock`(실제 현재 보유 현황)은 threshold와 무관해 그대로 공유. 호출
횟수가 6회 → 2회(v1 1 + v2 1)로 줄어 broadcast 생성 빈도가 함께 줄어듦.

`_allocate_candidates_by_stock` 호출 재현 명령:

```bash
cd main/spark && PYTHONPATH=../.. uv run --frozen pytest tests/test_revenue_first_algorithm.py -q
```
