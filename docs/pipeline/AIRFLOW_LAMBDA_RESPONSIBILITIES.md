# 실행 자원 격리를 위한 Lambda 원격 호출

- 요약
    - 기존 `lambda_handler_for()`는 Lambda handler를 **Airflow worker 프로세스 안에서 실행**
    - AWS Lambda 원격 호출로 데이터 처리 자원과 Airflow worker의 실행 경계 분리
    - Main 원천 3종 5개와 Sub Source→Raw 4개, 총 **9개 함수** 원격 호출 전환
    - Airflow는 스케줄·재시도·validation·Asset 공개 책임 유지
- 목차
    1. [문제](#문제)
    2. [원인](#원인)
    3. [적용](#적용)
    4. [응답과 실패 계약](#응답과-실패-계약)
    5. [적용 범위](#적용-범위)
    6. [검증](#검증)
    7. [한계](#한계)

## 문제

Lambda 폴더의 handler를 사용해도 `lambda_handler_for()`로 호출하면 실제 실행 주체는
AWS Lambda가 아니다.

```python
return lambda_handler_for(
    "driver_vehicle_monthly_snapshot_raw_to_bronze"
)(event=event)
```

이 함수는 `importlib`로 handler를 가져와 Airflow worker 프로세스에서 실행한다.

- ETL의 CPU·메모리 사용량이 Airflow worker와 분리되지 않음
- 함수별 VPC·IAM·timeout 설정이 실제 실행 경계가 되지 않음
- 한 수집 작업의 메모리 부족이 같은 worker의 다른 Task에 영향을 줄 수 있음

## 원인

초기에는 handler의 event와 응답 계약만 분리하고 실행 위치는 Airflow 프로세스로
유지했기 때문에 worker가 수집·소규모 변환을 직접 수행했다.

월별 운행의 대규모 변환은 Spark·EMR로 분리했으나 API 수집과 작은 Bronze·Silver 작업은
같은 실행 경계가 없었다.

## 적용

Main과 Sub가 공통 [`invoke_lambda()`](../../shared/airflow/common/lambda_invoke.py)를
사용하도록 통일했다.

```python
return invoke_lambda(
    "driver_vehicle_monthly_snapshot_raw_to_bronze",
    package="main.aws_lambda.functions",
    event=remote_event,
)
```

### 운영 실행

EC2 Airflow의 `LAMBDA_INVOKE=remote` 설정에서 boto3가 Lambda를 동기 호출한다.

```python
response = client.invoke(
    FunctionName=function_name,
    InvocationType="RequestResponse",
    Payload=json.dumps(event).encode("utf-8"),
)
```

AWS 인증은 별도 `aws_conn_id`가 아니라 boto3 기본 credential chain을 사용한다. 운영에서는
Airflow EC2의 IAM Role에 대상 함수의 `lambda:InvokeFunction` 권한을 부여한다.

### `LambdaInvokeFunctionOperator`를 직접 쓰지 않은 이유

현재 파이프라인은 `LambdaInvokeFunctionOperator`를 DAG에 직접 배치하지 않는다. 공통
호출 함수를 TaskFlow Task 안에서 사용해 기존 validation 입력과 XCom dict 계약을
유지한다.

### 실행 흐름

```text
Airflow Task
    → boto3 RequestResponse
    → AWS Lambda
    → dict 응답(row_count, locations, 입력 버전)
    → validation → _SUCCESS → Asset 공개
```

원격 실행으로 바뀌어도 Lambda 성공을 데이터 공개 완료로 보지 않는다. Airflow가 실제
S3 산출물을 다시 검증한 뒤에만 `_SUCCESS`와 Asset을 발행한다.

## 응답과 실패 계약

handler는 JSON object로 직렬화 가능한 dict를 반환하고 최소 `row_count`, `locations`를
포함한다.

```python
return {
    "row_count": result.write_result.row_count,
    "locations": [result.write_result.location],
    "collected_at": collected_at,
}
```

원격 호출은 다음을 확인한 뒤 JSON payload를 dict로 복원한다.

- `StatusCode == 200`
- 응답에 `FunctionError` 없음
- payload가 유효한 JSON object

Lambda handler 예외가 성공한 Airflow Task로 지나가지 않으며, 정상 응답은 dict 형태로
validation Task에 전달된다.

가장 긴 함수 timeout이 300초이므로 read timeout은 330초다. `RequestResponse`의 SDK
재시도는 같은 수집을 중복 실행할 수 있어 끄고 Airflow 재시도만 사용한다.

## 적용 범위

| 영역 | 원격 호출 함수 | 현재 방식 |
| --- | ---: | --- |
| Main 원천 API 3종 | 5개 | AWS Lambda 원격 호출 |
| Sub Source→Raw 4종 | 4개 | AWS Lambda 원격 호출 |
| Main EIA EV·Gas | 5개 | 후속 전환 대상, 현재 in-process |

Main 원천 API는 월별 운행 1개, 기사·차량 스냅샷 2개, 리스 재고 2개다. Sub는
Fuel Economy, 차량 카탈로그, Uber·Lyft 자격 목록의 Source→Raw 4개만 대상이다.
`*_raw_to_curated`와 `vehicle_master_curated_to_curated`는 이번 범위에 포함하지 않았다.

## 검증

- 공통 원격 호출: [`test_source_lambda_remote_invoke.py`](../../sub/airflow/tests/test_source_lambda_remote_invoke.py)
- Main 원천 DAG 계약: [`test_driver_vehicle_monthly_snapshot_raw_to_silver_dag.py`](../../main/airflow/tests/test_driver_vehicle_monthly_snapshot_raw_to_silver_dag.py)
- Lambda handler 응답 계약: [`functions/README.md`](../../main/aws_lambda/functions/README.md)
- 운영 모드 설정: [`docker-compose.ec2.yml`](../../docker-compose.ec2.yml)

테스트는 함수명·event·응답 계약을 확인하고, boto3 fake client로 `RequestResponse`,
`FunctionError`, timeout과 SDK 재시도 비활성화를 검증한다.

## 한계

- `RequestResponse` 동안 Airflow Task는 Lambda 완료를 기다리므로 worker slot은 사용한다.
- EIA 5개 함수는 아직 in-process라 전체 Lambda가 격리된 상태는 아니다.
- Lambda timeout을 넘는 처리나 대규모 조인은 Spark·EMR 대상이다.

Airflow는 실행 순서, 재시도, 검증과 공개를 담당하고 Lambda는 함수 단위 데이터 처리를
격리한다. 원격 호출은 책임을 옮기는 것이 아니라 이미 나눈 책임에 실행 경계를 적용한 것이다.
