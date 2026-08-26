# 원본 변경 가능성을 고려한 설계

## 1. 문서 목적

같은 월의 API 원본은 게시 후에도 수정될 수 있으므로 `ETag`와 `Last-Modified`로 변경을
감지해야 한다. 반대로 조건부 `HEAD`가 `304 Not Modified`를 반환해도 데이터 레이크의
Bronze·Silver가 정상이라는 보장은 없다. 이 문서는 원본 변경 가능성과 물리 저장 누락을
같은 refresh 판단에서 처리하도록 바꾼 과정을 정리한다.

- 적용 코드: [`source_api_refresh/tasks.py`](../../../main/airflow/scripts/source_api_refresh/tasks.py)
- 회귀 테스트: [`test_source_api_refresh_dag.py`](../../../main/airflow/tests/test_source_api_refresh_dag.py)

## 2. 발견한 증상

Airflow Variable에는 이전 `ETag`와 `Last-Modified`가 남아 있고 API는 정상적으로 304를
반환했다. refresh Task는 미변경으로 판단해 성공적으로 skip됐지만 다음 상태에서는
하류에 사용할 데이터가 없었다.

- 대상 월 Bronze가 삭제됨
- Bronze는 있지만 대응하는 Silver가 없음
- 최신 Bronze보다 이전 token의 Silver만 존재
- Silver 데이터만 있고 `_SUCCESS`가 없음
- 빈 `collected_at` 디렉터리만 남음

실패가 아니라 정상 skip으로 보이므로 알림이 발생하지 않는 점이 더 위험했다.

## 3. 원인

`ETag`는 외부 원천이 이전 요청 이후 바뀌었는지만 알려 준다. 우리 저장소에 파일이
존재하는지, 변환이 끝났는지, 검증 marker가 있는지는 알려 주지 않는다.

```python
refresh_required = result["changed"]
```

원천 상태와 파이프라인의 물리 완료 상태를 하나의 조건으로 오해한 것이 원인이었다.

## 4. 적용 판단

refresh 여부를 세 조건의 `OR`로 바꿨다.

```python
refresh_required = (
    source_changed
    or not bronze_exists
    or not silver_exists
)
```

Bronze가 존재한다는 것은 유효한 `collected_at` token과 데이터 파일이 있다는 뜻이다.
Silver가 존재한다는 것은 최신 Bronze token과 같은 `source_collected_at` 디렉터리에
데이터 파일과 `_SUCCESS`가 모두 있다는 뜻이다.

## 5. 적용 지점

### 5.1 최신 Bronze token 선택

S3에서 대상 지역·월의 실제 데이터 파일을 찾고 유효한 token 중 최신 값을 선택한다.
빈 디렉터리는 수집 완료로 보지 않는다.

### 5.2 대응 Silver 완료본 확인

월에 Silver가 하나라도 있는지만 보지 않는다. 최신 Bronze의 token과 같은
`source_collected_at` 버전을 찾는다.

### 5.3 `_SUCCESS` 확인

데이터 파일만 있거나 marker만 있는 버전은 미완료다. 두 조건을 모두 만족해야 skip한다.

## 6. 시나리오 대조

| API | Bronze | 대응 Silver | refresh |
|---|---|---|---|
| 200 변경 | 무관 | 무관 | 실행 |
| 304 | 없음 | 없음 | 실행 |
| 304 | 있음 | 없음 | 실행 |
| 304 | 최신 token 있음 | 이전 token만 있음 | 실행 |
| 304 | 최신 token 있음 | 데이터만 있고 marker 없음 | 실행 |
| 304 | 최신 token 있음 | 데이터 + `_SUCCESS` | skip |

## 7. 재검증 절차

1. 304 응답과 Bronze 없음 조합에서 refresh가 실행되는지 확인한다.
2. Bronze만 만들고 Silver를 비워 재실행되는지 확인한다.
3. 이전 token의 Silver만 두고 최신 Bronze가 다시 처리되는지 확인한다.
4. 데이터와 `_SUCCESS` 중 하나만 둔 Silver가 미완료로 판정되는지 확인한다.
5. S3에 같은 시나리오를 적용해 결과가 같은지 확인한다.
6. 최신 Bronze와 대응 Silver 완료본이 있을 때만 skip하는지 확인한다.

## 8. 결론

304는 원천 미변경 신호이지 파이프라인 완료 신호가 아니다. refresh 판단에 Bronze 원본과
그 token을 계승한 Silver 완료본을 함께 확인함으로써 원천이 그대로여도 삭제·실패한
데이터를 자동 복구하도록 바꿨다.
