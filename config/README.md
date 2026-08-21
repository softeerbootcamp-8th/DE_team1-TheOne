# config/

버전 관리와 무관하게 값이 바뀔 수 있어 코드에서 분리한 설정을 둡니다.

- `generation.json` — 합성 데이터 생성의 실험 파라미터 (아래 참고)
- 순수익/추천 계산에 쓰이는 비즈니스 가정치(렌탈비, 연비 가정 등)
- 크롤러의 API URL, 헤더, 수집 대상 도시 목록 같은 **데이터셋별 수집 설정은 이 폴더가
  아니라 해당 데이터셋 폴더 안의 config 파일**로 작성합니다

---

## generation.json

합성 기사·운행 원천 데이터 생성의 단일 소스입니다. 읽는 쪽은
[`sub/config.py`](../sub/config.py) 의 `load_config` 하나뿐이고, 검증에 실패하면
조용히 넘어가지 않고 즉시 예외로 죽습니다.

### 무엇이 여기 오고 무엇이 오지 않는가

여기 오는 것은 **실험 파라미터** — 바꿔가며 돌려볼 값 — 만입니다. 나머지 세 종류는
오지 않습니다. 분류 근거와 전체 목록은
[`docs/config_inventory.md`](../docs/config_inventory.md) 에 있습니다.

- **실측 상수**는 코드가 소유합니다. 요일·시간대 비중과 거리 tertile(1.93/4.75mi)은
  HVFHV 실측값이라 `analysis.md` 를 다시 돌려서 갱신하는 값입니다. 손으로 고치는 것은
  설정 조정이 아니라 관측 왜곡이므로, 손잡이와 같은 파일에 두지 않습니다.
- **가정 파라미터**(분포 모수, `GROUP_COUNTS`, 각종 범위)도 지금은 코드가 소유합니다.
  튜닝 대상이지만 아직 올리지 않았습니다 — 후속 작업에서 옮깁니다.
- **경로·환경**(입출력 디렉터리, 외부 URL, `test_row_limit`)은 설정이 아니라 실행
  인자입니다. CLI 인자와 Airflow Param 이 소유합니다.

`target_month` 도 여기 없습니다. 대상 월은 TLC 공개 지연 때문에 런타임에 발견되고,
설정에 넣으면 같은 설정으로 다른 달을 돌릴 수 없게 됩니다(`config_hash` 가 달라짐).
`RunContext.create(target_month, config)` 의 인자입니다.

### 우선순위

```
CLI 인자  >  generation.json
```

두 계층입니다. Airflow Param 은 별도 계층이 아닙니다 — BashOperator 가 Param 을
**CLI 인자로 렌더링해서** 넘기기 때문입니다. 그래서 Param 기본값을 비워 두고, 비어
있으면 플래그 자체를 생략해 이 파일의 값이 쓰이게 합니다. Param 에 값을 박아 두면
그 값이 항상 CLI 로 실려서 이 파일이 영원히 가려집니다.

`--seed` 처럼 이 파일의 값을 덮는 인자를 주면, 덮은 값으로 **유효 설정**을 만들어 그것을
해싱합니다. 그러지 않으면 `run_id` 가 실제로 쓰이지 않은 설정을 가리킵니다.

### 계보 (`run_id` / `config_hash`)

`config_hash` 는 이 파일 전체를 정렬 직렬화해 sha256 한 값의 앞 12자이고,
`run_id` 는 `{target_month}_{config_hash}` 입니다. 발행한 릴리스의 `manifest.json` 에
`run_id`·`config_hash`·`created_at` 이 실리고, 재발행 멱등성 판정도 `run_id` 로 합니다
— `seed` 만 보면 설정을 바꿔도 낡은 릴리스를 재사용해서 "설정을 바꿨는데 결과가 안
바뀐다" 가 됩니다.

해시는 파일 **전체**를 덮습니다. 그래서 `bucket_size` 처럼 산출물에 영향이 없는 성능
노브를 바꿔도 `run_id` 가 바뀌고 한 번 재생성이 일어납니다. 그 비용을 받아들이는 대신
"설정 값이 하나라도 다르면 다른 실행" 이라는 규칙을 예외 없이 지킵니다.

### 미배선 키 표기 규칙

키를 먼저 두고 소비자를 나중에 붙이는 경우가 있습니다. 그런 키는 **로더가 검증은
하지만 결과에 영향을 주지 않습니다.** 조용히 두면 "설정을 바꿨는데 결과가 안 바뀐다"
가 되므로, 반드시 두 곳에 표기합니다.

1. `docs/config_inventory.md` 의 `배선` 열에 `검증만` 으로 적습니다.
2. `sub/config.py` 의 해당 dataclass docstring 에 무엇이 소비하지 않는지와 어느
   작업에서 붙일지를 적습니다.

**현재 미배선 키** — `driver.join_rate`, `driver.exit_rate`,
`driver.vehicle_change_rate`. 현재 생성기는 유입 수와 유출 수가 정의상 같은
`change_rate` 하나로만 돌기 때문입니다. 후속 lifecycle 작업에서 소비됩니다.

### 값을 바꿀 때

- 테스트가 이 파일을 읽지 않습니다. 테스트 전용 리터럴은
  `sub/spark/tests/conftest.py` 에 있고, 이 파일을 읽는 테스트는 로더를 검증하는
  `test_config.py` 하나뿐이며 값이 아니라 로드 가능성만 봅니다. 그래서 값을 튜닝해도
  무관한 테스트가 깨지지 않습니다.
- `bootstrap.snapshot_date` 는 취향이 아니라 **데이터를 결정하는 값**입니다. 리스
  시작일이 `[LEASE_START_MIN, snapshot_date]` 에서 추첨되고, 생성기는 여기서부터 한
  달씩만 전진합니다(`sub/generators/synthetic_driver_trip_source/monthly.py`). 즉 이
  값이 곧 그 로컬에서 만들 수 있는 첫 달입니다. 실행일(`date.today()`)을 쓰지 않는
  이유는 이것이 수집한 데이터가 아니라 회사 DB 를 대신해 생성한 **픽스처**이고, 픽스처는
  고정되어야 팀원 사이에 결과를 비교할 수 있기 때문입니다. 매월 1일만 허용합니다.
- `driver.initial_count` 를 바꾸면 `snapshot.py` 의 `GROUP_COUNTS` 합도 함께 맞춰야
  합니다. 총원은 이 파일이, 자격 구성비는 코드가 소유하기 때문입니다. 어긋나면
  두 출처를 함께 지목하며 즉시 실패합니다.
- `allocation.score_weights` 는 합이 1.0 이어야 합니다. 로더가 확인합니다.
