# 반복 계산을 줄이기 위한 선택적 캐싱과 lineage 절단

- 요약
  - 같은 중간 결과를 대조, 두 추천 알고리즘, 품질 검사, 최종 변환에서 다시 사용해 Spark가 앞선 계산을 반복할 수 있었음
  - 재사용 횟수가 많고 Join·집계 비용이 큰 DataFrame 5종만 `persist`하고 사용 범위가 끝나면 `unpersist`
  - 재고 배정 라운드마다 `localCheckpoint`로 이전 라운드의 긴 계산 이력을 끊고 파티션 수를 8개로 정리
  - Spark UI 캡처에서 캐시 54MiB, 디스크 사용 0B와 8개 파티션의 checkpoint RDD를 확인
  - 전체 파이프라인 반복 측정에서 선택적 캐시는 캐시를 끈 조건보다 중앙값 기준 43.3% 빨랐음

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

추천 내부의 기사×차량 후보와 후보 순위는 현재 `try/finally`가 아니라 정상 반환 경로에서 해제한다. 이 범위에서 예외가 발생하면 애플리케이션 종료 전까지 캐시가 남을 수 있으므로, 내부 단계의 예외 안전성이 필요해지면 해제 구문을 `finally`로 옮겨야 한다.

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

위 값은 과거 한 실행의 Storage 상태를 보여준다. 현재 코드에서도 최소 Silver 입력의 결합 결과를 `count()`로 materialize하고 같은 결과에서 `groupBy` Action을 실행해 캐시 읽기와 해제를 다시 확인했다.

```text
in_memory_scan=True
stored_rdds_before_unpersist=1
stored_rdds_after_unpersist=0
```

재사용 계획에 `InMemoryTableScan`이 나타났고 `unpersist(blocking=True)` 뒤 저장 RDD가 0개가 됐다.

![현재 코드의 캐시 재사용 계획](../assets/silver_to_gold_current_cache_scan.png)

### 현재 전체 파이프라인 수행 시간

2026-08-27에 현재 코드의 전체 Silver → Gold 계산을 한 번 워밍업한 뒤 캐시만 켜고 끄며 각 조건을 3회 측정했다.

- 입력: 운행 678,892행, 기사 2,000명, 차량 12종, 연료비 31일
- 환경: Spark 3.5.6, Java 17, `local[3]`, driver memory 6GiB, Darwin arm64
- 공통 설정: `spark.sql.shuffle.partitions=40`, 명시적 Broadcast, AQE 활성화
- cache off: 현재 코드의 `DataFrame.persist()`와 `unpersist()`만 no-op으로 변경하고 `localCheckpoint`는 두 조건 모두 유지
- 범위: 로컬 Parquet 읽기부터 비즈니스 검증과 두 Gold 결과의 `toPandas()`까지
- 제외: 입력 생성, Spark 시작·워밍업, CSV·PostgreSQL 적재
- 정확성: 모든 실행에서 집계 2,000행, 추천 12,000행으로 동일

| 조건 | 1회 | 2회 | 3회 | 평균 | 중앙값 |
|---|---:|---:|---:|---:|---:|
| **cache on** | 28.131초 | 28.162초 | 32.825초 | **29.706초** | **28.162초** |
| cache off | 51.276초 | 49.673초 | 46.204초 | 49.051초 | 49.673초 |

cache on은 중앙값 기준 21.511초, **43.3%** 단축됐고 평균 기준으로는 39.4% 단축됐다. 이번 측정에서는 캐시 효과만 분리하기 위해 배정 라운드의 `localCheckpoint`를 유지했다. checkpoint 자체의 수행 시간과 계획 크기 비교는 [반복 배정 루프의 실행 시간과 메모리 부족 문제 해결](07_allocation_loop_runtime_optimization.md)에 따로 정리했다.

코드에서는 주요 세 DataFrame이 `finally`에서 해제되는지 확인했다. 임계값 추천 후보와 후보 순위는 정상 실행에서 사용 범위가 끝난 직후 해제된다.

## 결론

반복해서 쓰는 중간 결과 5종만 메모리에 남기고, 라운드 반복은 checkpoint로 이전 계산과 분리했다. Spark UI와 현재 실행 계획에서 캐시가 실제 재사용되는 것을 확인했으며, 현재 로컬 전체 경로에서는 캐시를 끈 조건보다 중앙값 기준 43.3% 빨랐다.

local checkpoint 결과가 여러 RDD 블록으로 남는 모습도 확인된다. 라운드 수나 후보 수가 크게 늘면 블록 수와 Storage Memory를 다시 보고, checkpoint 주기와 파티션 수를 조정해야 한다.

### 관련 코드와 자료

- 주요 DataFrame 수명 관리: `main/spark/jobs/silver_to_gold/job.py`
- 운행 결합 결과 캐싱: `main/spark/jobs/silver_to_gold/transformer.py`
- 후보 캐싱: `main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py`
- 라운드별 checkpoint: `main/spark/jobs/silver_to_gold/recommendation_algorithm/base.py`
- 현재 캐시 재사용 계획: `docs/assets/silver_to_gold_current_cache_scan.png`
- Spark UI Storage 캡처: `docs/assets/silver_to_gold_strategic_cache_storage.png`
- Spark UI Executors 캡처: `docs/assets/silver_to_gold_strategic_cache_executors.png`
