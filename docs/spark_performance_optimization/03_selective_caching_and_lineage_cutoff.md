# 반복 계산을 줄이기 위한 선택적 캐싱과 lineage 절단

- 요약
  - 같은 중간 결과를 대조, 두 추천 알고리즘, 품질 검사, 최종 변환에서 다시 사용해 Spark가 앞선 계산을 반복할 수 있었음
  - 재사용 횟수가 많고 Join·집계 비용이 큰 DataFrame만 `persist`하고 작업 종료 시 `unpersist`
  - 재고 배정 라운드마다 `localCheckpoint`로 이전 라운드의 긴 계산 이력을 끊고 파티션 수를 8개로 정리
  - Spark UI 캡처에서 캐시 54MiB, 디스크 사용 0B와 8개 파티션의 checkpoint RDD를 확인

## 문제

Spark DataFrame은 결과를 바로 계산하지 않는다. `count`, `first`, `toPandas` 같은 Action이 실행될 때 필요한 이전 연산을 거슬러 올라가 계산한다.

기사별 한 달 운행 실적은 합계 대조, 두 추천 알고리즘, 최종 출력에서 반복해서 사용된다. 추천 결과도 비즈니스 규칙 검사와 최종 변환에서 다시 사용된다. 아무 DataFrame도 저장하지 않으면 같은 Join과 집계가 Action마다 다시 실행될 수 있다.

반대로 모든 중간 결과를 캐싱하면 메모리가 오래 점유된다. 재사용되지 않는 DataFrame까지 저장하면 캐시 관리 비용만 늘어난다.

재고 배정은 후보 순위 1위부터 다음 순위로 이동하는 라운드 구조다. 새 라운드의 결과가 이전 라운드 결과와 그 이전 계산을 계속 참조하면 lineage가 라운드 수만큼 길어진다. 실행 계획과 복구해야 할 계산 이력이 계속 커질 수 있다.

## 접근

DataFrame의 크기보다 재사용 횟수와 다시 계산할 연산의 무게를 먼저 봤다. 여러 Action에서 쓰이면서 Join이나 집계를 포함한 결과만 저장 대상으로 골랐다.

수명이 명확한 캐시는 사용 직후 해제했다. 전체 작업이 어느 지점에서 실패하더라도 주요 캐시는 `finally`에서 해제하도록 했다.

재고 배정 라운드는 캐싱만으로 해결하지 않았다. 앞선 라운드 전체를 계속 참조하지 않도록 각 라운드 결과를 로컬 checkpoint로 바꿨다.

## 해결

다음 중간 결과를 선택적으로 저장한다.

- 운행 기록에 기사·차량·연료비를 붙인 결과
- 기사별 한 달 운행 실적
- 두 알고리즘을 합친 최종 추천 결과
- 여러 임계값이 공유하는 기사×차량 후보
- 라운드마다 반복해서 읽는 기사별 후보 순위

여러 임계값을 계산하는 추천에서는 기사×차량 후보를 한 번 만든 뒤 `persist`한다. 후보 조합 수 검사와 모든 임계값 배정이 이 결과를 공유한다. 추천이 끝나면 즉시 해제한다.

기사별 후보 순위를 매긴 결과도 라운드 루프 전에 저장한다. 각 라운드는 해당 순위의 제안, 이미 배정된 기사, 남은 재고를 다시 읽는다. 루프가 끝나면 순위 DataFrame을 해제한다.

라운드 결과는 `coalesce(8).localCheckpoint(eager=False)`로 바꾼다. 다음 라운드는 이전 라운드까지의 전체 Join·Window·Union 계보 대신 checkpoint 결과에서 시작한다. 8개라는 값은 현재 로컬 실행 크기에 맞춘 고정값이며 데이터 크기에 따라 다시 측정해야 한다.

작업 본문에서 저장한 운행 결합 결과, 기사별 집계, 최종 추천은 `finally`에서 해제한다. 계산이나 적재 중 예외가 발생해도 같은 정리 경로를 지난다.

## 검증

Spark UI의 Storage 탭에서 저장된 운행 결합 결과와 기사별 집계가 각각 100% 캐시된 것을 확인했다. 같은 화면에는 8개 파티션으로 만들어진 여러 `MapPartitionsRDD`도 표시된다. 이는 라운드별 local checkpoint 결과다.

![선택적으로 저장된 DataFrame과 checkpoint RDD](../assets/silver_to_gold_strategic_cache_storage.png)

Executors 탭 캡처에서 확인한 값은 다음과 같다.

| 항목 | 관측값 |
|---|---:|
| Storage Memory | 54MiB / 434.4MiB |
| Disk Used | 0B |
| RDD Blocks | 307 |
| Input | 436.4MiB |
| Shuffle Read | 19.2MiB |
| Shuffle Write | 14.8MiB |

![캐시 실행의 Executor 지표](../assets/silver_to_gold_strategic_cache_executors.png)

이 값은 한 실행의 상태를 보여준다. 캐시 적용 전후 실행 시간 비교 자료는 아니므로, 캐싱이 몇 퍼센트 빨라졌다고 쓰지 않는다. 라운드별 `localCheckpoint`의 실행 시간 비교는 [반복 배정 루프의 실행 시간과 메모리 부족 문제 해결](07_allocation_loop_runtime_optimization.md)에 따로 정리했다.

코드에서는 주요 세 DataFrame이 `finally`에서 해제되는지 확인했다. 임계값 추천 후보와 후보 순위도 사용 범위가 끝난 직후 해제된다.

## 결론

반복해서 쓰는 중간 결과만 메모리에 남기고, 라운드 반복은 checkpoint로 이전 계산과 분리했다. Spark UI에서 선택한 DataFrame과 8개 파티션의 checkpoint 결과가 실제 Storage에 올라간 것을 확인했다.

local checkpoint 결과가 여러 RDD 블록으로 남는 모습도 확인된다. 라운드 수나 후보 수가 크게 늘면 블록 수와 Storage Memory를 다시 보고, checkpoint 주기와 파티션 수를 조정해야 한다.

### 관련 코드와 자료

- 주요 DataFrame 수명 관리: `main/spark/jobs/silver_to_gold/job.py`
- 운행 결합 결과 캐싱: `main/spark/jobs/silver_to_gold/transformer.py`
- 후보 캐싱: `main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py`
- 라운드별 checkpoint: `main/spark/jobs/silver_to_gold/recommendation_algorithm/base.py`
- Spark UI Storage 캡처: `docs/assets/silver_to_gold_strategic_cache_storage.png`
- Spark UI Executors 캡처: `docs/assets/silver_to_gold_strategic_cache_executors.png`
