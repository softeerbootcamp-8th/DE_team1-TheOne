# Spark 최적화

## Silver → Gold — Shuffle Partition 최적화

### 문제

Spark 기본값인 `spark.sql.shuffle.partitions=200`으로 실행하면 Silver → Gold의 여러
shuffle stage가 200개 task로 생성됐습니다. 계산량보다 task scheduling 비용이 커지는지
확인하기 위해 partition 수만 바꾸고 동일 입력과 변환 로직으로 비교했습니다.

측정 범위는 Silver 읽기부터 Gold DataFrame 3종을 `toPandas()`로 계산하는 지점까지이며,
Gold 적재는 포함하지 않았습니다.

### 1차 실측

| `spark.sql.shuffle.partitions` | 실행 시간 | 변화 |
| ---: | ---: | ---: |
| 200 (Spark 기본값) | 41.619초 | 기준 |
| 16 | 25.302초 | **16.317초(39.2%) 단축** |

첫 실측에서 기본값 200이 이 job에는 과하다는 것을 확인했습니다. 16을 바로 확정하지 않고
인접 후보 8과 32를 같은 방식으로 추가 비교했습니다.

### 후보 재측정

| `spark.sql.shuffle.partitions` | 실행 시간 | Gold 행 수 (`집계 / 추천 / 보고서`) |
| ---: | ---: | ---: |
| 200 | 28.982초 | 2,000 / 2,000 / 1 |
| 8 | 28.716초 | 2,000 / 2,000 / 1 |
| 16 | 28.289초 | 2,000 / 2,000 / 1 |
| **32** | **25.194초** | **2,000 / 2,000 / 1** |

1차 실측과 후보 재측정은 별도 실행이라 JVM 준비 상태 등에 따라 절대 시간이 다릅니다.
두 실험 모두 partition 수를 줄였을 때 개선됐고, 후보 재측정에서는 32가 가장 빨랐습니다.
Gold 3종의 행 수도 모두 같아 최종값으로 32를 채택했습니다.

### 추가로 실행한 최적화 실험

셔플 파티션 외에도 현재 Silver → Gold 실행 경로에서 효과가 있을 수 있는 네 가지를
직접 실행했습니다. 비교 기준은 `shuffle.partitions=32`, AQE 활성화, 기존 broadcast 유지,
추가 입력 캐시 없음 상태의 **25.194초**입니다.

| 실험 | 실행 방법 | 실행 시간 | 기준 대비 | 결론 |
| --- | --- | ---: | ---: | --- |
| AQE 비활성화 | `spark.sql.adaptive.enabled=false` | 27.085초 | 7.5% 느림 | AQE 유지 |
| broadcast 비활성화 | 명시적 hint 제거 + `autoBroadcastJoinThreshold=-1` | 28.777초 | 14.2% 느림 | 기존 broadcast 유지 |
| Silver 입력 추가 캐시 | `hvfhv.persist()` 후 동일 변환 | 25.455초 | 1.0% 느림 | 추가 캐시 기각 |
| 배정 checkpoint 지연 | `localCheckpoint(eager=True → False)` | 21.198초 | 별도 동일 조건 기준 대비 7.3% 단축 | 적용 |

네 실험 모두 Gold 행 수는 `2,000 / 2,000 / 1`로 기준 실행과 같았습니다.

#### AQE

AQE를 끄고 같은 입력을 실행해 보니 27.085초가 걸렸습니다. AQE가 runtime 통계를 이용해
shuffle partition을 조정하는 현재 실행이 더 빨랐으므로 별도 설정을 추가하지 않고
Spark의 AQE 활성 상태를 유지합니다.

#### Broadcast Join

코드의 명시적 `broadcast()`만 제거하면 Spark가 자동 broadcast를 다시 선택할 수 있습니다.
그래서 hint를 제거하고 `spark.sql.autoBroadcastJoinThreshold=-1`도 함께 적용해 broadcast를
완전히 끈 상태로 실행했습니다. 실행 시간이 28.777초로 늘어 기존 broadcast join을 유지합니다.

#### Silver 입력 추가 캐시

검증과 변환 전에 `hvfhv`를 추가로 `persist()`해 반복 읽기를 줄이는 방식을 실행했습니다.
25.455초로 기준보다 빨라지지 않았고 캐시 메모리만 추가로 사용하므로 적용하지 않았습니다.

#### Stage 수 감소 — eager checkpoint 제거

차량 재고를 순위별로 배정하는 반복문은 매 순위마다
`localCheckpoint(eager=True)`를 실행하고 있었습니다. lineage를 끊는 checkpoint는 필요하지만,
각 반복에서 즉시 action을 발생시킬 필요는 없어 `eager=False`를 비교했습니다.

| 방식 | 실행 시간 | 완료 Jobs | 완료 Stages | Gold 행 수 (`집계 / 추천 / 보고서`) |
| --- | ---: | ---: | ---: | ---: |
| 기존 eager checkpoint | 22.874초 | 138 | 138 | 2,000 / 2,000 / 1 |
| **lazy checkpoint** | **21.198초** | **126** | **126** | **2,000 / 2,000 / 1** |

완료 Jobs와 Stages가 각각 12개(8.7%) 줄었고 실행 시간은 1.676초(7.3%) 단축됐습니다.
Gold 3종을 컬럼 정렬·행 정렬한 뒤 계산한 SHA-256도 기존 실행과 모두 같았습니다.
따라서 배정 로직과 checkpoint 자체는 유지하면서 즉시 실행만 제거했습니다.

##### Spark UI — Jobs 비교

변경 전에는 eager checkpoint가 반복마다 별도 Job을 실행해 완료 Job이 138개였습니다.

![변경 전 Spark UI Jobs 138개](./images/silver_to_gold_before_jobs.jpg)

lazy checkpoint 적용 후 완료 Job은 126개로 줄었습니다.

![변경 후 Spark UI Jobs 126개](./images/silver_to_gold_after_jobs.jpg)

##### Spark UI — Stages 비교

변경 전 완료 Stage는 138개이며, 목록에서 `localCheckpoint`가 별도 Stage로 실행된 것을
확인할 수 있습니다.

![변경 전 Spark UI Stages 138개](./images/silver_to_gold_before_stages.jpg)

변경 후 완료 Stage는 126개입니다. 최종 shuffle 연산은 설정값대로 `32/32` task로
실행됐습니다.

![변경 후 Spark UI Stages 126개](./images/silver_to_gold_after_stages.jpg)

##### Spark UI — Executor 누적 지표

스크린샷을 위한 동일 입력 추가 실행에서 Executor 누적 지표도 비교했습니다. 추가 실행은
실행 시간 변동이 있어 화면의 Jobs·Stages·task·I/O 수치만 비교 근거로 사용했습니다.

| 지표 | 기존 eager checkpoint | lazy checkpoint | 변화 |
| --- | ---: | ---: | ---: |
| 완료 task | 1,015 | 919 | **96개(9.5%) 감소** |
| RDD block | 133 | 61 | **72개 감소** |
| Input | 453.5 MiB | 436.6 MiB | **16.9 MiB 감소** |
| Shuffle Read | 4.3 MiB | 7.2 MiB | 2.9 MiB 증가 |
| Shuffle Write | 4.3 MiB | 4.3 MiB | 동일 |

![변경 전 Spark UI Executors](./images/silver_to_gold_before_executors.jpg)

![변경 후 Spark UI Executors](./images/silver_to_gold_after_executors.jpg)

이 변경은 shuffle I/O 자체를 줄인 최적화는 아닙니다. Shuffle Write는 같고 Read는
늘었지만, 반복 checkpoint action을 미뤄 실제 실행된 Jobs·Stages·task 수를 줄였습니다.

##### Spark UI — checkpoint Stage 상세

변경 후 checkpoint Stage 하나를 열어 32개 task, 입력 4.6 MiB, 전체 task 시간 0.2초를
확인했습니다.

![변경 후 checkpoint Stage 상세](./images/silver_to_gold_after_checkpoint_stage_detail.jpg)

### 적용

Silver → Gold Spark 제출 파라미터에만 다음 설정을 추가했습니다.

```text
--conf spark.sql.shuffle.partitions=32
```

적용 위치: [`monthly_taxi_trip_silver_to_gold_dag.py`](../main/airflow/dags/monthly_taxi_trip_silver_to_gold_dag.py)

차량 재고 배정 반복문의 checkpoint는 다음과 같이 지연 실행으로 변경했습니다.

```python
assigned = assigned.coalesce(8).localCheckpoint(eager=False)
```

적용 위치: [`transformer.py`](../main/spark/jobs/silver_to_gold/transformer.py)

---

## 서브 파이프라인 — Silver 기사 배정 Spark 최적화

> 아래 내용은 메인 Silver → Gold 파이프라인이 아니라, 서브 파이프라인에서 사용하는
> 기사 배정 Spark job의 최적화 기록입니다.

월 **2,040만 행**에 기사 **2,000명**을 배정하는 작업이 해당 서브 파이프라인에서 가장 무거웠습니다.
이 절은 해당 job이 죽던 원인과 고친 과정입니다.

> 실행 시간 측정치는 정리 중입니다. 실험 산출물은 [`data/experiments/`](../data/experiments/) 에 시점별로 보관합니다.

### 증상 — 늘릴 수 없는 메모리에서 죽음

`applyInPandas` 는 그룹 하나를 **통째로 Arrow 배치로 만들어** 파이썬에 넘깁니다.
이 버퍼는 JVM 힙이 아니라 **직접(off-heap) 메모리**라 `--driver-memory` 를 아무리 올려도
Arrow 의 `UnpooledDirectByteBuf` 할당에서 그대로 죽습니다.

날짜로만 그룹핑하면 그룹 하나가 **1,100만 행**입니다.

---

### 조치

| # | 조치 | 내용 | 효과 |
| --- | --- | --- | --- |
| 1 | **그룹 키 재설계** | 날짜 → **(기사 버킷 × 날짜)** | 그룹당 1,100만 행 → **3만 행대**, 병렬성 **200배** |
| 2 | **컬럼 프루닝** | Arrow 전달 컬럼 **51개 → 12개** | 구역명·요금 8종·선호 배열(행당 8개) 등 배정과 무관한 값 제외 |
| 3 | **후보 생성 방향 전환** | 운행 기준 → **기사 버킷 기준** | 어차피 버려질 94.3% 에 대한 후보 생성 자체를 제거 |
| 4 | **캐시** | 구역 이동시간 · 기사 차원 | 원본 Parquet 3회 스캔 → 1회, 파티션 없는 윈도우 재계산 제거 |

#### 1번이 로직을 바꾸지 않는 이유

`_allocate_day` 의 상태는 (기사, 하루) 단위이고 기사는 버킷 하나에만 속하므로,
그룹 안에 그 기사의 그날 후보가 **모두** 들어옵니다. 그룹을 쪼갰지만 배정 결과는 동일합니다.

#### 3번 — 기사가 병목입니다

기사 2,000명이 한 달에 소화 가능한 운행은 약 117만 건으로 전체 트립의 **5.7%** 뿐입니다.
"운행마다 기사를 뽑는" 방향으로 후보를 만들면 어차피 버려질 94.3% 에 대해서도 후보를 생성하게 됩니다.
대신 **운행을 기사 버킷에 미리 나눠 줍니다** — 같은 운행을 두 버킷이 다툴 일이 없어 중복 배정도 원천 차단됩니다.

기사 하나가 아니라 10명씩 묶는 이유는 **여유의 재분배**입니다.
선호 시간대가 드문 기사는 자기 몫만으로는 목표 운행 수를 못 채웁니다 —
실측 여유 배수가 최소 1.3배, 2배 미만이 10명 있었습니다. 같은 버킷의 여유 있는 기사와 풀을 공유하면 그 편차가 메워집니다.

#### 4번을 하게 된 단서

로그에 `WindowExec: No Partition Defined` 경고가 반복해서 찍혔습니다.
2,000행짜리 작은 차원인데 파티션 없는 윈도우가 붙어 있어, 캐시하지 않으면
이후 모든 action 에서 조인 3개 + 윈도우가 통째로 다시 돌고 있었습니다.

---

### 실험 이력

가설 하나에 산출물 한 벌씩 남겼습니다. ([`data/experiments/`](../data/experiments/))

| 실험 | 가설 |
| --- | --- |
| `flat_allocator` | 배정을 평면 구조로 — 베이스라인 |
| `narrow_candidates` | 후보 컬럼을 줄이면 Arrow 부담이 줄 것 |
| `broadcast_shuffle64` | 작은 차원 브로드캐스트 + 셔플 파티션 64 |
| `binary_tie` | 타이브레이크 해시를 가볍게 |
| `eligible_slots` | 자격 슬롯 선계산으로 후보 축소 |
| `full20m` | 전체 2,000만 행 실측 |

*(실행 시간 before/after 표 작성 예정)*
