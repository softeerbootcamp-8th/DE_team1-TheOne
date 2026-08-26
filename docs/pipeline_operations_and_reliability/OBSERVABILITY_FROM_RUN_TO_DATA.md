# 작업 실패와 데이터 갱신 지연을 함께 감지

## 요약

- 작업 실패뿐 아니라 Asset 미발행으로 Gold가 오래 갱신되지 않는 상태도 감시한다.
- 지역·월과 실행 ID를 기준으로 알림, 데이터 상태, 인프라 지표와 로그를 연결한다.

## 문제

Asset 이벤트가 오지 않으면 DAG 실행 기록도 없어 실패 알림만으로는 데이터 지연을
발견할 수 없다. 실패가 발생해도 원인과 영향 지역·월을 찾으려면 여러 로그를 확인해야 했다.

재시도 후 성공하면 최초 실패 원인이 묻히는 문제도 있다. 최종 상태만 보면 반복되는 자원
부족이나 일시적 장애를 정상 실행으로 오해할 수 있다.

## 감시 기준

| 대상 | 감시 내용 |
| --- | --- |
| 실행 | 재시도·최종 실패, 지역·월, 원인, 실행 ID, 로그 링크 |
| 데이터 | 지역별 마지막 Gold 검증 성공 시각과 31일 지연 기준 |
| 인프라 | 서버 CPU·메모리·디스크, Lambda 실패·제한, EMR 실패·용량 |

실행 실패와 데이터 지연은 별도 신호로 수집하되 지역·월과 실행 ID를 공통으로 남긴다.
작업, 데이터, 인프라 중 어느 범위에서 문제가 시작됐는지 한 흐름으로 확인하기 위해서다.

Airflow 알림은 재시도와 최종 실패를 구분한다. 과거 월 재처리는 최신 데이터 갱신 시각을
바꾸지 않으며, 한 지역의 성공이 다른 지역의 지연을 가리지 않도록 상태를 지역별로 둔다.

Slack 알림에는 작업 단계, 실행 유형, 시도 횟수, 실제 실패 원인과 로그 링크를 넣는다.
EMR 연동 오류도 래퍼 예외가 아니라 Spark의 원래 실패 원인이 남도록 처리한다.

Airflow와 EMR 로그는 S3에 보존하고 실행·단계·시도별로 경로를 나눠 첫 실패 원인을
남긴다. 서버는 Prometheus, Lambda와 EMR은 CloudWatch, 통합 알림은 Grafana로 확인한다.

## 검증

- DAG 재시도·최종 실패 알림
- 지역 분리와 과거 월 재처리를 포함한 Gold 지연 감시
- S3 원격 로그의 암호화와 시도별 경로
- EMR 실제 실패 원인의 Airflow·Slack 전달
- 서버 지표 중단 자체에 대한 알림

## 관련 자료

- [`shared/airflow/common/slack_failure_callback.py`](../../shared/airflow/common/slack_failure_callback.py)
- [`main/airflow/common/gold_staleness.py`](../../main/airflow/common/gold_staleness.py)
- [`shared/airflow/common/emr_serverless.py`](../../shared/airflow/common/emr_serverless.py)
- [`main/airflow/tests/test_gold_staleness.py`](../../main/airflow/tests/test_gold_staleness.py)
- [모니터링 구성](../MONITORING.md)
