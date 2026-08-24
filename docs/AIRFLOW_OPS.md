# Airflow 운영 — 태스크 설계와 장애 알림

DAG **14개**를 EC2 위 Docker Airflow 로 돌립니다 — `sub/airflow/dags` 6개, `main/airflow/dags` 8개.
두 제품이 같은 Airflow 인스턴스를 쓰되 DAG 폴더는 나눠 마운트합니다.

- [1. 태스크 분할 기준](#1-태스크-분할-기준)
- [2. 동시 실행 차단](#2-동시-실행-차단)
- [3. 무거운 작업의 분리 실행](#3-무거운-작업의-분리-실행)
- [4. 재시도 정책](#4-재시도-정책)
- [5. 장애 알림 설계](#5-장애-알림-설계)
- [6. 스케줄](#6-스케줄)

---

## 1. 태스크 분할 기준

논리적 단위로 묶되, **5분 이상 걸리는 작업은 태스크를 분리**합니다.
한 태스크가 길면 어디서 실패했는지 로그를 파야 알 수 있고, 재시도 시 성공한 부분까지 다시 돕니다.

```
raw_to_bronze → validate_bronze → bronze_to_silver → validate_silver
```

수집·변환·검증을 나눈 덕에 **재시도 정책도 태스크별로 달리** 줄 수 있습니다.

## 2. 동시 실행 제한

지역 파티션을 받는 `main/` DAG 8개는 `max_active_runs=3`으로, 지역 축이 없는
`sub/` DAG 6개는 `max_active_runs=1`로 실행합니다. main DAG의 Bronze·Silver 경로와
Gold 자연 키가 `service_area`로 격리되어 서로 다른 세 지역은 동시에 처리할 수 있습니다.
네 번째 지역부터는 실행 중인 DagRun이 끝날 때까지 대기합니다.

`max_active_runs=3`은 실행 **상한**일 뿐 지역 DagRun을 자동 생성하지 않습니다.
지역별 트리거는 서로 다른 `service_area`를 전달해야 하고, 같은 지역·같은 월을 수동으로
중복 트리거하지 않는 운영 규약은 유지합니다. 이 동시성 값은 계약 테스트로 강제합니다.

## 3. 무거운 작업의 분리 실행

Spark job 은 Airflow 프로세스 안에서 돌리지 않고 `BashOperator` 로 별도 프로세스에 띄웁니다.
스케줄러와 메모리를 다투지 않게 하기 위해서입니다.

대신 별도 프로세스는 DAG 파싱 때의 `sys.path` 를 물려받지 않아 `PYTHONPATH` 를 명시해야 합니다 —
이 규약도 계약 테스트로 강제합니다(같은 실수를 두 번 했습니다).

LocalExecutor의 전역 `parallelism`은 기본값 32를 유지합니다. EMR와 하위 DAG 대기는
`deferrable=True`라 대기 중 worker slot을 반환하므로, 세 지역 규모에서는 별도 증설하지
않습니다. queued→running 지연과 EC2 CPU·메모리를 측정해 병목이 확인될 때만 조정합니다.

---

## 4. 재시도 정책

재시도가 의미 있는 실패와 그렇지 않은 실패를 구분합니다.

| 태스크 유형 | 재시도 | 근거 |
| --- | --- | --- |
| 외부 수집 (API·크롤링) | **2회 + exponential backoff** | 네트워크·레이트리밋·릴리스 지연은 시간이 해결 |
| 검증 (GX·계약 검사) | **0회** | 같은 입력을 다시 검증해도 같은 결과. 재시도는 알림만 늦춤 |
| 그 외 (변환·조립) | 1회 | 일시적 자원 부족만 흡수 |

검증 태스크에 `retries=0` 을 명시적으로 거는 것이 핵심입니다.
DAG 기본값을 그대로 물려받으면 **깨진 데이터를 발견하고도 30분을 더 기다린 뒤에야** 알림이 옵니다.

## 5. 장애 알림 설계

알림을 두 종류로 구분합니다.

```
⚠️  Airflow Task Alert     상태: 재시도 예정    시도: 1 / 3
🔴  Airflow Task Fail      상태: 최종 실패      시도: 3 / 3
```

둘 다 DAG · Task · **파티션** · Run ID · 시도 횟수 · **Airflow 로그 바로가기 링크**를 포함합니다.
전부 같은 문구로 오면 *지금 손봐야 할 것* 과 *자동 복구될 것* 이 구분되지 않아 알림 전체가 무시됩니다.

**파티션 항목**(`partition_key`)은 파티션 DAG에서 "어느 파티션이 문제인지"를 알림만 보고
가리게 합니다. Airflow가 이미 콜백 컨텍스트에 넣어주므로 별도 배선이 없습니다
(`airflow/sdk/execution_time/task_runner.py`). 비파티션 DAG에서는 컨텍스트에 키가
없거나 `None`이라 `-`로 표시됩니다. 지역 축이 들어가면(#674) 이 키가
`"{service_area}:{year_month}"`가 되어 **어느 지역이 죽었는지**까지 구분됩니다 —
지역마다 DAG를 새로 만들지 않는 설계라, 이 항목이 없으면 N개 지역이 한 DAG로 들어올 때
온콜이 지역을 가릴 방법이 없습니다.

EMR Serverless 잡 이름도 같은 이유로 `run_id` 기반입니다. `ds_nodash`는 날짜뿐이라 같은
날 실행이 여러 건이면 콘솔에서 구분되지 않고, `logical_date`가 없는 Asset 트리거 실행에서는
아예 비어 `UndefinedError`가 납니다(#746에서 실제 발생).

Slack provider 를 못 불러오는 환경(로컬 테스트 등)에서는 **로깅 콜백으로 자동 대체**됩니다 —
알림 설정 하나 때문에 DAG 가 뜨지 않는 상황을 막기 위해서입니다.
([slack_failure_callback.py](../shared/airflow/common/slack_failure_callback.py))

Gold DAG(`monthly_taxi_trip_silver_to_gold_pipeline`)는 입력 Asset이 준비되지 않으면
`AirflowSkipException`으로 조용히 skip되는데, Airflow에서 skip은 실패가 아니라
`on_failure_callback`이 걸리지 않습니다. 이 blind spot을 메우려고 `validate_inputs_task`가
skip 직전에 Slack 알림을 직접 호출합니다.

Asset 이벤트 자체가 오지 않는 경우는 Gold DAG 안에서 감시할 수 없으므로, 매일 실행되는
`source_api_refresh_pipeline.check_gold_staleness`가 별도로 확인합니다. Gold 검증 성공 시
`Variable("gold_staleness_state__<service_area>")`에 최신 복합 파티션 키와 UTC 성공 시각을
직접 기록하고, 일일 감시 태스크는 그 시각 이후 경과일을 계산합니다. 아직 성공 기록이
없으면 최초 감시 시각을 저장해 **Asset 이벤트가 한 번도 없었던 경우도** SLA 경과 뒤
알립니다. 상태 키가 지역별이라 NYC 성공이 다른 지역의 지연을 가리지 않습니다.

SLA는 대상월 계산이 원천의 "latest" 해석에 달려 있어 절대 날짜 대신 상대 기준(N일)을
씁니다. `source_api_refresh_pipeline`의 `gold_stale_sla_days` Param이 우선하고, 비우면
`Variable("gold_stale_sla_days")`, 둘 다 없으면 31일입니다.

---

## 6. 스케줄

```
[원천 DB 파이프라인 — sub/, DAG 8개]
매월  1일 03–05시   카탈로그 → Lyft 자격 → Uber 자격  ─┐
매월  1일 04–07시   제원 → 휘발유 → 전기 → 연료비 통합    ├→ 차량 마스터 (4종 AND)
매월 10일 00시      월별 릴리스 생성·게시

[메인 데이터 파이프라인 — main/, DAG 9개]
매월  1일 05–07시   EIA 연료비 수집·정제 (휘발유 · 전력)
매월 10일 00시      원천 API 수집 (운행 기록 · 기사-택시 마스터 · 보유 차량)
매월 12일 01시      기사 운행 이력 Silver (운행 × 리스 기간 조인)
매월 13일 03시      Gold 3종 생성
```

차량 마스터는 원천 4종이 **모두** 이번 달 것으로 갱신됐을 때만 조립합니다. 예전에는
카탈로그·자격 3종이 주간이라 제원을 AND 에 넣으면 전체가 월 1회로 묶여 배차 자격이
최대 3주 묵었고, 그래서 제원만 OR 로 빼뒀습니다. 4종을 모두 월간으로 맞추면서 그
이유가 사라졌습니다.

원천 릴리스와 수집이 같은 날인 것은 의도적입니다 — 수집은 실패하면 2회 재시도(exponential backoff)하므로
릴리스가 조금 늦어도 흡수됩니다. 그래도 못 받으면 Slack 으로 최종 실패가 옵니다.
