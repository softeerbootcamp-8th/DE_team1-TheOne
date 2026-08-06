# Airflow constraints (vendored)

Airflow 는 (airflow 버전 × python 버전) 조합으로 검증된 의존성 집합을
constraints 파일로 배포합니다. 이 파일을 vendoring 해서 재현성을 확보합니다.

```bash
# 리포 루트에서
make airflow-constraints
# -> constraints/constraints-3.3.0-py3.11.txt 생성
```

이 파일은 **git 에 커밋**합니다.

`../pyproject.toml` 의 `==` 핀 값이 바로 여기서 나온 것입니다. Airflow 를
다음 버전으로 올릴 때는 이 파일을 다시 받아서, 새 값으로 `pyproject.toml` 을
고친 뒤 `uv lock` 을 돌리세요.
