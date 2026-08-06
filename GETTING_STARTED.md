# 개발 환경 셋업 — 팀원 전원 같은 버전 쓰기

**개발은 Docker 에서 합니다.** 로컬에 Python 을 깔 필요가 없습니다.

---

## 1. 최초 1회 (팀원 각자)

```bash
# 1) Docker Desktop 설치 후 실행
# 2) 저장소 받기
git clone <repo-url> && cd DE_team1-Even1TrillionNoSee

# 3) 띄우기 (첫 실행은 이미지 빌드로 몇 분 걸립니다)
docker compose up -d
```

브라우저에서 <http://localhost:8080> 접속. 로그인 정보는:

```bash
docker compose logs airflow | grep "Password for user"
```

`hello_test` DAG 가 보이면 정상입니다. (확인 후 `airflow/dags/hello_test.py` 는 지우세요.)

### 일상 명령어

```bash
docker compose up -d        # 띄우기
docker compose logs -f      # 로그 보기
docker compose down         # 내리기
docker compose down -v      # 내리면서 DB 초기화 (꼬였을 때)
docker compose build        # 라이브러리가 바뀐 뒤 이미지 다시 굽기
```

---

## 2. 버전이 고정되는 원리

이게 핵심입니다. **`uv.lock` 하나가 로컬과 Docker 양쪽의 원천입니다.**

```
pyproject.toml    "boto3 쓸래"                    ← 사람이 적음 (느슨)
      ↓  uv lock
uv.lock           "boto3 1.43.65 + 해시 abc123…"  ← ★ git 에 커밋됨 (정확)
      │
      ├─→ docker build      이미지 안에 이 버전 그대로 설치
      └─→ uv sync --frozen  내 노트북 .venv (IDE 자동완성용, 선택)
```

`pip install boto3` 는 **설치하는 날마다 최신 버전**을 가져옵니다. 그래서 어제 받은
사람과 오늘 받은 사람이 달라집니다. `uv.lock` 을 쓰면 거기 적힌 그 버전만, 해시까지
대조해서 깔립니다. **그래서 갈릴 수가 없습니다.**

Dockerfile 안을 보시면 lock 을 이미지로 복사해서 그대로 설치합니다:

```dockerfile
COPY pyproject.toml uv.lock /tmp/build/
RUN uv export --frozen ... -o requirements.txt && pip install -r requirements.txt
```

실제로 대조해서 이미지와 lock 이 일치하는 걸 확인했습니다
(airflow 3.3.0 / pandas 2.1.4 / numpy 1.26.4 / boto3 1.43.65 …).

---

## 3. 구조

```
.github/workflows/   CI — PR 마다 버전 잠금 검증
docker-compose.yml   로컬 개발 환경 (Airflow + Postgres)
versions.toml        팀 버전표 — "우리 뭐 쓰지?" 한눈에
Makefile             lock / check / sync / build

airflow/    Airflow 3.3.0   → EC2          (compose 로 로컬 실행)
spark/      Spark 3.5.6     → EMR 7.13.0
lambda/     Python 3.11     → AWS Lambda
  ├─ pyproject.toml    무슨 라이브러리를 쓸지 (사람이 적음)
  ├─ uv.lock           ★ 실제 고정 — 커밋 필수
  ├─ .python-version   3.11
  ├─ Dockerfile        이미지 (uv.lock 그대로 설치)
  └─ dags/             airflow 만. 로컬에서 고치면 컨테이너에 바로 반영

config/              가정 파라미터 (기획서 11장). 버전 관리와 무관
```

### 왜 런타임을 3개로 나눴나

**의존성이 서로 충돌하기 때문입니다.**

| | Airflow | Spark |
|---|---|---|
| pandas | 2.1.4 | 3.0.5 |
| numpy | 1.26.4 | 2.4.6 |

한 환경에 다 넣으면 둘 중 하나가 끌려 내려가 깨집니다.

---

## 4. 라이브러리를 추가·변경할 때

`uv.lock` 갱신은 **로컬에서** 해야 합니다. 그래서 uv 는 깔아두세요.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

작업 순서:

```bash
cd airflow
# 1) pyproject.toml 의 dependencies 수정
uv lock                    # 2) 잠금 갱신 ← 빼먹으면 팀원과 버전이 갈립니다
make check                 # 3) 확인 (루트에서)
cd .. && docker compose build && docker compose up -d   # 4) 이미지 다시 굽기
# 5) pyproject.toml 과 uv.lock 을 "같이" 커밋 → PR
```

**PR 올리기 전에 `make check` 한 번.** `pyproject.toml` 만 고치고 `uv lock` 을 안
돌렸다면 여기서 FAIL 이 납니다.

깜빡해도 **PR 을 올리면 CI 가 같은 검사를 자동으로 돌려서 빨간불**이 뜹니다
(`.github/workflows/ci.yml`). 로컬에서 미리 돌려보면 왕복을 줄일 수 있을 뿐입니다.

> CI 가 잡는 것: "`uv lock` 을 깜빡한" 실수.
> CI 가 못 잡는 것: `uv lock` 까지 돌려서 **잘못된 버전을 제대로 잠근** 경우.
> 예를 들어 `pandas==2.1.4` 를 `2.2.0` 으로 바꾸고 `uv lock` 을 돌리면 CI 는
> 통과합니다. Airflow 검증 조합에서 벗어나는지는 **리뷰어가 봐야 합니다.**

### 남이 올린 변경을 받았을 때

```bash
git pull
docker compose build && docker compose up -d    # lock 이 바뀌었으면 이미지도 다시
```

### Airflow 만 주의

`airflow/pyproject.toml` 의 버전은 `==` 로 박혀 있습니다. Airflow 공식 constraints 가
검증한 조합이라 임의로 올리면 깨집니다. 핀을 빼면 pandas 3.x / numpy 2.x 처럼
Airflow 가 검증한 적 없는 조합이 잠깁니다.

```bash
make airflow-constraints          # constraints 새로 받기
# 받은 파일에서 값 확인 → airflow/pyproject.toml 수정
cd airflow && uv lock
```

---

## 5. 하지 말 것

| 하지 말 것 | 왜 |
|---|---|
| 컨테이너 안에서 `pip install` | 컨테이너 지우면 사라짐. pyproject 고치고 `uv lock` 할 것 |
| `uv.lock` 을 `.gitignore` 에 | 이걸 커밋 안 하면 버전 고정이 아예 동작 안 함 |
| `uv sync` (`--frozen` 없이) | lock 을 멋대로 갱신함 |
| `uv.lock` 손으로 편집 | 해시가 깨짐. 항상 `uv lock` 으로 재생성 |

`uv.lock` 이 **머지 충돌**나면 손으로 고치지 마세요:

```bash
git checkout --theirs airflow/uv.lock   # 아무 쪽이나 택하고
cd airflow && uv lock                   # pyproject 기준으로 재계산
```

---

## 6. 로컬 .venv 는 선택

IDE 자동완성·린트를 원하면:

```bash
make sync        # 또는 cd airflow && uv sync --frozen
```

Docker 안에서 도는 것과 **같은 버전**이 깔립니다. 실행은 Docker 에서 하니
필수는 아닙니다. 용량이 크니(airflow 만 수백 MB) 안 쓰면 `rm -rf */.venv`.

---

## 7. 아직 안 된 것

- **Renovate 앱 미연결.** `renovate.json` 은 있지만 GitHub 앱 연결이 필요합니다.
- **EMR 이미지가 `:latest`.** 떠다니는 태그라 운영 배포 전에 digest 고정 필요.
- **spark 이미지는 빌드 검증 안 함.** EMR 베이스 이미지가 수 GB 라 받지 않았습니다.
  airflow / lambda 이미지는 빌드 + 버전 대조까지 확인했습니다.
- **compose 는 로컬 개발 전용.** 운영은 EC2/EMR/Lambda 로 따로 배포합니다.
  `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10` 같은 설정도 개발용입니다.
