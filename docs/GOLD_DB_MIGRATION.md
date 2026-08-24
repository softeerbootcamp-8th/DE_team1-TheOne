# Gold DB 스키마 변경 런북

Gold 2종(`driver_aggregation`, `driver_car_suggestion`)은 RDS
PostgreSQL에 적재됩니다. 이 문서는 **그 테이블의 스키마를 바꿀 때** 무엇을 해야 하는지를
적습니다.

---

## 왜 런북이 필요한가 — 자동 마이그레이션이 없습니다

`main/spark/jobs/silver_to_gold/postgres_loader.py`의 `_create_table_sql()`이 DDL을
소유하는데, 실행하는 문장은 **`CREATE TABLE IF NOT EXISTS`** 입니다.

```python
f"CREATE TABLE IF NOT EXISTS {table} (\n    " + ... + f",\n    PRIMARY KEY ({primary_key})\n)"
```

즉 **이미 배포된 테이블에는 아무 일도 하지 않습니다.**

| 바꾼 것 | 실제로 일어나는 일 |
|---|---|
| dataclass에 필드 추가 | 테이블에 컬럼이 **안 생김**. 다음 적재의 `INSERT`가 `psycopg2.errors.UndefinedColumn`으로 죽음 |
| `_PRIMARY_KEYS` 변경 | 제약이 **안 바뀜**. 옛 PK가 그대로 남아 새 조합이 충돌하거나 중복을 못 막음 |
| 컬럼 타입 변경 | 반영 안 됨 |

이 저장소에는 Alembic(Airflow 자체 메타DB용 제외)도, 마이그레이션 러너도 없습니다.
`ALTER TABLE`이 코드 어디에도 없다는 것이 그 증거입니다. **그래서 스키마 변경은 손으로
쓴 SQL을 배포 전에 수동 실행하는 것이 유일한 경로입니다.**

> 다행히 실패는 **요란합니다** — 마이그레이션을 잊으면 적재가 죽고, 틀린 값이 조용히
> 들어가지는 않습니다. 트랜잭션(`with conn:`)으로 묶여 있어 부분 반영도 없습니다.

## 순서 — 반드시 이 순서

```
1. 마이그레이션 SQL 실행   ← 배포 전
2. 코드 배포 (Spark 이미지 / Airflow DAG)
3. 첫 실행 확인
```

거꾸로 하면 1번과 2번 사이에 도는 잡이 전부 실패합니다. 반대로 1번을 먼저 하면,
일반적인 컬럼 추가는 옛 코드도 계속 동작하도록 구성해야 합니다.

그래서 **`NOT NULL` 컬럼을 추가할 때는 1번과 2번 사이의 창을 짧게** 하거나,
nullable로 넣고 배포 후 `SET NOT NULL`을 따로 거는 2단계로 나눕니다. 아래 스크립트는
한 트랜잭션 안에서 nullable 추가 → 백필 → `SET NOT NULL`을 하므로, **배포 직전에
실행**하는 것을 전제로 합니다.

## 2026-08-24 재고 테이블 전환 스크립트 실행 방법

```bash
# DSN 은 Airflow Variable/환경변수 GOLD_DATABASE_URL 과 같은 값입니다.
psql "$GOLD_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f main/spark/jobs/silver_to_gold/migrations/2026-08-24_replace_monthly_report_with_inventory.sql
```

이 스크립트는 더 이상 발행하지 않는 `monthly_report`와 추천 뷰를 삭제합니다. 새 코드
배포 직전에 실행하고, 보존이 필요한 기존 리포트는 실행 전에 별도로 백업합니다.

`ON_ERROR_STOP=1`을 빼면 중간 문장이 실패해도 계속 진행해 **일부만 반영된 상태**가
됩니다. 스크립트 자체는 `BEGIN`/`COMMIT`으로 묶여 있지만, 이 플래그가 없으면 psql이
에러를 무시하고 다음 문장으로 넘어갑니다.

실행 후 확인:

```sql
\d driver_aggregation
\d driver_car_suggestion
```

두 테이블 모두 `service_area` 컬럼이 `not null`이고, PK가
`(service_area, year_month, version, driver_id)`인지 확인합니다.

#927은 기존 `driver_car_suggestion` 물리 테이블을 그대로 재사용하고, 없는 환경에서는
적재기가 생성하므로 추가 스키마 마이그레이션이 없습니다. 기존 시뮬레이션·재고 테이블은
새 Gold 실행부터 갱신하지 않으며 삭제는 별도 데이터 보존 결정 뒤 진행합니다.

## 이력

| 날짜 | 스크립트 | 내용 | 관련 |
|---|---|---|---|
| 2026-08-23 | `2026-08-23_add_service_area.sql` | 3종에 `service_area` 추가, PK를 `(service_area, ...)`로 확장. 기존 행은 `'NYC'` 백필 | #809, #674, #805 |
| 2026-08-24 | `2026-08-24_replace_monthly_report_with_inventory.sql` | 추천 뷰·`monthly_report`·`candidate_stock`을 제거하고 Gold 재고 테이블 생성 | #915 |
| 2026-08-24 | 추가 SQL 없음 | 기존 추천 테이블을 다시 적재하고 Gold 출력을 집계·추천 2종으로 축소 | #927 |

### 추천 후보 확장 이력

- #927부터 기사 수 × 차량 모델 수 후보는 Spark 내부 계산으로만 사용합니다.
- 현재 차량 점유분과 남은 재고로 배정한 `driver_car_suggestion`만 적재합니다.

### 2026-08-23 주의사항

- **기존 행을 `'NYC'`로 백필하는 근거**는 "지금 서비스 지역이 뉴욕 하나"입니다.
  **두 번째 지역이 들어온 뒤에 이 스크립트를 돌리면 안 됩니다** — 다른 지역 데이터가
  NYC로 라벨링됩니다.
- PK에 `service_area`를 넣는 이유는 **`driver_id`가 지역 간 유니크하지 않기**
  때문입니다(#805 — `build_driver_ids()`가 `SD0000`~`SD1999`를 지역 성분 없이 생성).
  빼면 두 지역의 같은 기사 ID가 한 행으로 취급됩니다.
- 대시보드의 "최신 버전만" 쿼리(`main/dashboard/datasource.py`)도 지역으로 좁혀야
  합니다 — 안 하면 버전이 낮은 지역 행이 **조용히 사라집니다.** #810에서 처리합니다.

## 새 마이그레이션을 추가할 때

1. `main/spark/jobs/silver_to_gold/migrations/<YYYY-MM-DD>_<설명>.sql` 로 만듭니다
2. `BEGIN`/`COMMIT`으로 감쌉니다
3. **왜 손으로 실행해야 하는지**와 **백필 가정**을 파일 맨 위 주석에 적습니다
4. 위 "이력" 표에 한 줄 추가하고, 주의사항이 있으면 소절로 남깁니다
5. `postgres_loader.py`의 `_PRIMARY_KEYS`/dataclass 변경과 **같은 PR**에 넣습니다 —
   따로 올리면 어느 쪽이 먼저 머지되는지에 따라 운영이 깨집니다
