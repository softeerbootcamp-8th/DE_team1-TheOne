# Shuffle Partition 최적화
- 요약
    - 파티션 수 200(default)로 고정
    - 계산량대비 과한 파티션 수로 task scheduling 비용 증가 우려
    - 실험을 통한 최적 파티션 수로 변경 (기존 시간 대비 약 **40% 절감**)
- 목차
    1. [문제](#문제)
    2. [문제 해결 접근](#접근)


## 문제
- 기존: partition 수 200 고정 (기본값)
- 예상 문제: 계산량 대비 **task scheduling 비용 증가**
- 접근
    1. 목적: 계산량 < task scheduling 비용 확인
        - partition 수만 바꿔 비교 (동일한 입력 및 변환 로직)
        - 측정 범위: Silver - Gold Pandas 계산 지점 (Gold Load 포함 X)

## 접근
### 1. partition 수 절감 이득 확인 (200->16)
> Partition 수가 과하게 많아 Scheduling Overhead가 더 큼

| `spark.sql.shuffle.partitions` | 실행 시간 |
| :--: | :--: |
| 200 (Default) | 41.619초 |
| 16 | 25.302초 |

### 2. partition 수 최적값 찾기
> Partition 수를 8/16/32로 변경하며 최적 개수 찾기

| `spark.sql.shuffle.partitions` | 실행 시간 |
|:--:|:--:|
| 200 | 28.982초 |
| 8 | 28.716초 |
| **32** | **25.194초** |