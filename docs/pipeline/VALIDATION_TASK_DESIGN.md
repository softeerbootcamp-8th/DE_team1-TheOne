# 데이터 공개 안정성을 위한 Validation Task 분리

## 문제

Lambda나 Spark가 예외 없이 종료됐다고 해서 하류 파이프라인이 읽어도 되는 데이터가
만들어졌다고 보장할 수는 없다.

실행은 성공했지만 다음과 같은 상태가 남을 수 있다.

- 반환 경로와 실제 저장 경로가 다름
- 파일이 없거나 크기가 0임
- Parquet 파일이 손상되어 읽을 수 없음
- 필수 컬럼이나 타입이 예상 스키마와 다름
- 기대한 행 수와 실제 행 수가 다름
- 다른 지역·월·입력 버전 경로에 저장됨
- 일부 파일만 생성된 채 작업이 종료됨

Lambda·Spark의 성공 상태만 보고 Asset을 발행하면 이런 결과가 정상 데이터처럼 Gold로
전파될 수 있다.

## 검증 책임

검증을 Lambda·Spark에서 전혀 하지 않는 것은 아니다. 검증 시점과 책임을 두 단계로
나눴다.

| 실행 주체 | 검증 책임 |
| --- | --- |
| Lambda·Spark | 입력값, 변환 과정, 스키마와 비즈니스 규칙 검증 |
| Airflow Validation Task | 실제 적재 파일의 존재, 경로, 크기, 스키마, 행 수와 완료 상태 검증 |

Lambda·Spark는 데이터를 만드는 과정이 올바른지 확인한다. Validation Task는 만들어진
결과가 저장소에 정상적으로 남았고 하류에 공개해도 되는지 확인한다.

## Validation Task를 분리한 이유

### 실제 적재 결과를 다시 확인

Lambda handler의 `locations` 응답만 믿지 않고 해당 S3 경로를 다시 연다.
연산 중 메모리에 있던 데이터가 정상이었더라도 writer가 다른 경로에 쓰거나 불완전한
파일을 남겼다면 여기서 실패한다.

```python
parsed = parse_handler_result(result, expected_locations=1)
table = read_parquet(parsed.locations[0])

if table.schema != EXPECTED_SCHEMA:
    raise ValueError("Silver 스키마가 다릅니다")
```

### 실패 지점을 분리

ETL과 검증을 하나의 Task로 묶으면 실패 원인이 API 요청인지, 변환인지, 저장인지,
품질 조건인지 로그를 열어봐야 알 수 있다.

```text
raw_to_bronze → validate_bronze → bronze_to_silver → validate_silver
```

Task를 나누면 Airflow 화면에서 실패한 단계가 바로 드러나고, 이미 성공한 앞 단계를
불필요하게 다시 실행하지 않아도 된다.

### 데이터 공개 시점을 통제

데이터 파일이 존재하는 것과 검증된 데이터로 공개된 것은 다르다. Validation Task가
성공한 뒤에만 `_SUCCESS` marker와 Asset을 발행한다.

```text
데이터 기록
    → 파일·경로·스키마·행 수 검증
    → _SUCCESS 기록
    → Asset 발행
    → 하류 DAG 실행
```

하류 reader는 `_SUCCESS`가 있는 버전만 선택한다. 검증에 실패한 파일이 남아 있더라도
Gold 입력으로 사용되지 않는다.

### 검증 규칙을 독립적으로 변경

NULL 허용 비율이나 필수 컬럼처럼 품질 기준만 바뀌어도 Lambda·Spark 실행 코드를 다시
배포하지 않아도 된다. 실행 로직과 공개 기준을 분리해 각각의 변경 범위를 줄였다.

## 적용 흐름

### Bronze

원천을 적재한 handler가 `row_count`, `locations`, `collected_at`을 반환한다. Airflow는
응답 타입과 실제 파일을 확인하고 데이터 기준 월·지역·수집 버전이 경로와 일치하는지
검증한다.

### Silver

Silver 변환 결과도 실제 Parquet을 다시 읽는다. 스키마, 행 수, 업무 불변식과
`source_collected_at`이 사용한 Bronze 버전을 계승했는지 확인한다.

### Gold

Gold는 결과 CSV·RDS 적재의 필수 컬럼과 대상 지역·월을 검증한다. 모든 검증이 끝난 뒤
해당 파티션의 성공 상태를 기록한다.

공통 검증 코드는 [`validation.py`](../../shared/airflow/common/validation.py), marker
계약은 [`success_marker.py`](../../shared/common/success_marker.py)에 있다.

## 재시도 정책

외부 실행과 검증은 실패 원인이 다르므로 같은 재시도 횟수를 사용하지 않는다.

| Task 유형 | 재시도 | 판단 근거 |
| --- | ---: | --- |
| API 수집·Lambda 원격 호출 | 2회 + exponential backoff | 네트워크·rate limit은 시간이 해결할 수 있음 |
| Spark·일반 변환 | 1회 | 일시적인 자원 부족을 흡수 |
| Validation | 0회 | 같은 파일을 다시 검사해도 결과가 바뀌지 않음 |

```python
raw = raw_to_bronze_task.override(
    retries=2,
    retry_exponential_backoff=True,
)()
checked = validate_bronze_task.override(retries=0)(raw)
```

검증 실패에 재시도를 적용하면 같은 오류를 반복하면서 알림만 늦어진다. 필수 컬럼
누락처럼 원천이 수정됐을 가능성이 있는 일부 경우만 수집을 명시적으로 한 번 다시 수행한다.

## 검증 항목

| 구분 | 확인 내용 |
| --- | --- |
| 응답 계약 | `row_count`, `locations` 타입과 값 범위 |
| 파일 상태 | 존재 여부, 크기, Parquet metadata와 읽기 가능 여부 |
| 스키마 | 필수 컬럼, 타입, 컬럼 누락 |
| 데이터 값 | NULL, 범위 이탈, 중복과 업무 불변식 |
| 저장 경로 | `service_area`, `year_month`, 입력 버전 일치 여부 |
| 공개 상태 | 데이터 파일과 `_SUCCESS`가 함께 존재하는지 |

검증 실패 시 marker와 Asset이 생성되지 않는지, 성공 시 검증 Task 이후에만 발행되는지는
각 DAG 계약 테스트로 확인한다.
