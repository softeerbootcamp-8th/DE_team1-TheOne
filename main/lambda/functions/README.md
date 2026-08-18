# lambda/functions

데이터셋 하나당 폴더 하나:

```
<dataset>/
  extractor.py   # class <Dataset>Extractor(Extractor) — 상단 docstring에 출처 URL/수집
                 # 주기/응답 형태/특이사항을 적습니다. API URL, 헤더 등 설정값도 여기 모듈
                 # 상수로 둡니다.
  handler.py     # lambda_handler — Extractor + Loader(lambda/common) 를 Pipeline 으로 조립.
                 # 파티션 스키마(pyarrow schema)도 보통 여기 상수로 둡니다.
```

## 핸들러 응답 스키마 (계약)

`lambda_handler` 는 항상 아래 **두 키를 먼저** 반환합니다. 나머지는 데이터셋별
도메인 필드로 자유롭게 붙이세요.

| 키 | 타입 | 뜻 |
|---|---|---|
| `row_count` | `int` | 이번 실행이 **쓴** 행 수. 0 이면 쓸 게 없었다는 뜻(정상)입니다 |
| `locations` | `list[str]` | 이번 실행이 쓴 산출물 경로. **파일이 하나여도 리스트**입니다 |

```python
return {
    "row_count": result.write_result.row_count,
    "locations": [result.write_result.location],
    "collected_date": f"{collected_at:%Y-%m-%d}",   # 이하 도메인 필드
}
```

- 왜 항상 리스트인가 — 업체/도시/출처별로 파일을 나눠 쓰는 핸들러가 있어
  `path` 와 `paths` 가 갈렸습니다. 하나로 고정해야 소비자가 분기하지 않습니다.
  개수가 필요하면 `len(locations)` 를 쓰세요 (`vendor_count` 류는 없앴습니다).
- **성공/실패 필드는 두지 않습니다.** 실패는 예외로 알립니다 — Airflow 는 태스크
  실패로, 원격 Lambda 호출은 `FunctionError` 로 잡습니다.
- 이 dict 는 지금은 XCom 으로 흐르지만, `LambdaInvokeFunctionOperator` 로
  바뀌면 **그대로 Lambda 응답 payload** 가 됩니다 (`docs/decision_making/0811.md` 2번).
  그래서 JSON 직렬화 가능한 값만 넣으세요 (`Path` 는 `str()` 로 변환).

---

`common/` 은 데이터셋이 아니라 **핸들러들이 같이 쓰는 코드**입니다 (`logging_setup.py` 등).
`lambda/common/` 이 아니라 여기 있는 이유는 `airflow/dags/common/` 이 Airflow 프로세스에서
최상위 이름 `common` 을 먼저 점유해 `lambda/common` 하위 모듈을 import 할 수 없기 때문입니다.
핸들러에서는 `from ..common.logging_setup import ...` 처럼 상대 경로로 부르세요 —
이러면 Lambda 이미지(`functions.*`), Airflow in-process(`lambda.functions.*`), pytest 에서
모두 같은 경로로 풀립니다.