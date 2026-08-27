# 작은 참조 데이터를 Broadcast Join으로 처리하기

- 요약
  - 67만 건이 넘는 운행 기록에 수십~수천 행의 참조 데이터를 붙이는 과정에서 큰 데이터까지 재분배하고 정렬하는 실행 계획이 나타남
  - 작은 쪽을 각 실행 노드에 복사하는 `Broadcast Join`을 명시하고, 기사×차량 후보와 임계값 확장에도 같은 방식을 적용
  - Spark UI에서 `SortMergeJoin`이 있던 구간이 `BroadcastHashJoin`으로 바뀐 것을 확인
  - Python UDF를 사용하지 않아 계산식이 Spark SQL 실행 계획 안에 남음

## 문제

월별 운행 기록은 크지만 차량 정보, 기사별 현재 차량, 일별 연료비는 상대적으로 작다. 크기 차이를 이용하지 않으면 Spark는 양쪽 데이터를 같은 키로 다시 나누고 정렬한 뒤 결합할 수 있다.

실제 Spark UI 캡처에서 678,892행의 운행 흐름과 작은 참조 데이터의 Join 일부가 `SortMergeJoin`으로 실행됐다. 해당 구간 앞에는 200개 파티션의 `Exchange`와 정렬 단계가 있었다. 큰 데이터 쪽에서 기록된 Shuffle Write는 31.8MiB였다.

차량 추천 후보를 만들 때는 기사 N명과 차량 모델 M개의 모든 조합이 필요하다. 결과가 N×M행이 되는 것은 업무 규칙상 피할 수 없다. 다만 M행의 차량 목록까지 클러스터 전체에서 다시 나누는 작업은 피할 수 있다.

## 접근

Join마다 어느 쪽이 월별 운행 기록이고 어느 쪽이 작은 참조 데이터인지 구분했다. 기사 차량 정보, 차량 재고, 일별 연료비, 거리대별 요금 배수는 작은 쪽으로 판단했다.

Spark의 자동 판단에만 맡기지 않고 작은 DataFrame에 `broadcast`를 명시했다. Spark가 작은 데이터를 각 실행 노드에 복사하면 큰 데이터는 현재 파티션에 머문 채 해시 테이블을 조회할 수 있다.

기사×차량 후보와 후보×임계값은 업무상 필요한 `crossJoin`이다. 이 경우에도 작은 차량 목록과 임계값 목록만 Broadcast한다. 후보 행 수 자체를 줄였다고 주장하지 않고, 작은 쪽을 재분배하는 단계만 없애는 데 목적을 뒀다.

## 해결

운행 기록에 다음 정보를 붙일 때 작은 쪽을 명시적으로 Broadcast했다.

- 기사별 현재 차량 정보
- 차량 모델별 연비와 임대료
- 한 달의 일별 연료비
- 거리대별 프리미엄 요금 배수
- 모델별 재고 한도

추천 후보 생성은 기사별 한 달 운행 실적과 차량 재고 목록의 `crossJoin`으로 구현했다. 차량 목록이 비어 있으면 Join 전에 바로 실패한다. 여러 순수익 임계값을 비교할 때도 5행인 기본 임계값 목록을 Broadcast한다.

계산은 `when`, `concat_ws`, `groupBy`, `Window` 같은 Spark 내장 표현식으로 작성했다. Python UDF와 Pandas UDF는 사용하지 않았다. 연료비, 예상 순수익, 추천 사유가 Spark SQL 계획 안에서 계산되므로 Python 프로세스로 행을 넘기는 직렬화 단계가 생기지 않는다.

원본 정제 단계에서도 필요한 컬럼만 `select`한 뒤 자료형을 변환한다. Parquet가 실제로 읽을 컬럼을 Spark가 계획할 수 있도록 입력 DataFrame 전체를 그대로 전달하지 않았다.

## 검증

아래 적용 전 캡처에는 678,892행을 출력하는 `SortMergeJoin`이 보인다. Join 앞의 두 입력에는 `Exchange`와 `Sort`가 있다.

![자동 Join 선택에서 나타난 SortMergeJoin](../assets/silver_to_gold_broadcast_auto_aqe_plan.png)

Broadcast를 명시한 실행 계획에서는 같은 큰 흐름의 Join이 `BroadcastHashJoin`으로 표시된다. 작은 입력에는 `BroadcastExchange`가 있고, 큰 입력 앞에는 Join을 위한 정렬 단계가 없다.

![명시적 Broadcast 이후 실행 계획](../assets/silver_to_gold_broadcast_explicit_plan.png)

캡처에서 확인되는 입력 규모는 다음과 같다.

| 입력 | 행 수 | 캡처에 표시된 파일 크기 |
|---|---:|---:|
| 월별 운행 기록 | 678,892 | 24.0MiB |
| 기사 차량 정보 | 2,000 | 29.3KiB |
| 일별 연료비 | 31 | 화면상 파일 크기 미표시 |

코드 검색으로 추천과 집계 경로에 Python UDF, Pandas UDF, `F.udf` 사용처가 없음을 확인했다. 대용량 데이터 행을 오류 메시지로 가져올 때는 `collect()` 앞에 `limit(5)`를 둔다. 알고리즘·임계값 조합은 작은 설정 목록이라 고유 조합만 별도로 가져온다. 단일 집계 결과는 `first()`로 읽는다.

실행 시간의 적용 전후 비교는 이 문서에 넣지 않았다. 캡처가 증명하는 것은 Join 방식과 Shuffle 구조의 변화다.

## 결론

큰 운행 기록과 작은 참조 데이터의 역할을 코드에 명시했다. Spark UI에서 작은 쪽은 `BroadcastExchange`, Join은 `BroadcastHashJoin`으로 실행된 것을 확인했다.

차량 모델이나 기사 프로필이 실행 노드 메모리에 부담을 줄 정도로 커지면 Broadcast 적용 여부를 다시 확인해야 한다. 이때는 데이터 크기, Broadcast 생성 시간, Executor 메모리를 함께 비교한다.

### 관련 코드와 자료

- 참조 데이터 Join: `main/spark/jobs/silver_to_gold/transformer.py`
- 기사×차량 후보 생성: `main/spark/jobs/silver_to_gold/recommendation_algorithm/base.py`
- 임계값 Broadcast: `main/spark/jobs/silver_to_gold/recommendation_algorithm/revenue_first.py`
- Spark UI 캡처: `docs/assets/silver_to_gold_broadcast_auto_aqe_plan.png`
- Spark UI 캡처: `docs/assets/silver_to_gold_broadcast_explicit_plan.png`
