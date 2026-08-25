# EMR 실패 원인을 작업 알림까지 전달
- 요약
  - 문제 : EMR 작업 실패해도 Airflow provider 오류가 실제 원인을 `KeyError`로 표시
  - 접근 : 
    - 실패 이벤트의 원문을 예외로 전달
    - Slack 알림에 원인, 대상 파티션, 시도 횟수, 다음 조치, 로그 링크를 표시하도록 함.
  - 검증 : 실제 운영 실패 이벤트를 테스트 입력으로 사용해 `ExitCode: 137`과 메모리 초과 원인이 알림으로 남는지 확인
  
- 목차
  1. [문제](#문제)
  2. [접근](#접근)
  3. [해결](#해결)
  4. [검증](#검증)
  5. [참고](#참고)

## 문제
- 배경
  - Spark 작업은 Airflow가 EMR에 제출하고 완료될 때까지 대기
    - 작업 성공 : EMR 이벤트에 Application/Job 식별자 들어옴
    - 작업 실패 : 식별자 대신 실패 원인 문자열 들어옴
- 문제  
  1. Airflow Amazon Provider가 실패 이벤트에서도 성공할 때만 존재하는 식별자 읽음
  2. 1로 인해 메모리 초과로 작업이 종료되어도 Airflow와 Slack에는 원인과 관계없는 오류가 나옴 (`job_details`)
  3. 이로인해 Airflow 전체 로그에서 이벤트를 직접 찾아야 함

## 접근
1. `실제 운영에서의 이벤트를 테스트 입력으로 보존`
2. `이벤트 비교`
  - 성공 : Job 식별자 존재
  - 실패 : 원인 문자열 존재
3. `문제 해결 방식` 의사 결정
  - 성공 처리 : 그대로
  - 실패 처리 : 원인 문자열 그대로 Airflow 예외로 전달
4. Slack `알림에서 포함하는 정보` 결정
  - 실패한 파이프라인과 작업
  - 운영 지역과 대상 월
  - 정기·수동·데이터 준비 이벤트 중 어떤 실행이었는지
  - 현재 시도 횟수와 전체 시도 횟수
  - 실패 원인과 다음 조치
  - 실행 ID와 Airflow 로그 링크

## 해결
> EMR 작업 실패시 처리 방식 변경
- 변경 사항
  1. 실패 이벤트 message 읽어, Airflow 예외로 보냄
    - 종료 코드와 예외가 Airflow 작업 실패 원인으로 남음
    - 예외가 Slack까지 전달
  2. Slack 메시지 설정
    - 종료 코드와 마지막 예외 포함 400자로 제한

## 검증
- 수집한 실패/성공 이벤트 기반 재시도 진행

## 참고

- [`emr_serverless.py`](../../shared/airflow/common/emr_serverless.py): EMR 실패 이벤트의 원인 전달
- [`slack_failure_callback.py`](../../shared/airflow/common/slack_failure_callback.py): 재시도·최종 실패 알림 내용과 예외 정리
- [`test_emr_serverless_operator.py`](../../main/airflow/tests/test_emr_serverless_operator.py): 실제 실패 이벤트 회귀 테스트
- [`test_slack_callbacks.py`](../../main/airflow/tests/test_slack_callbacks.py): 알림 내용·재실행 횟수·여러 줄 예외 테스트

