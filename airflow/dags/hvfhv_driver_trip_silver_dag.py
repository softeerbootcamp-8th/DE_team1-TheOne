"""월별 HVFHV 기사 배정 Silver 생성과 결과 검증 DAG."""

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator
try:
    from common.slack_failure_callback import slack_failure_callback
except Exception:
    def slack_failure_callback(context):
        return None

ROOT = Path("/opt/airflow/project-root")
if not ROOT.exists():
    ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "trips_path": str(SILVER / "hvfhv"),
    "preferences_path": str(BRONZE / "driver_preferences.parquet"),
    "company_path": str(ROOT / "data" / "source" / "company"),
    "travel_times_path": str(SILVER / "taxi_zone_travel_times"),
    "output_path": str(SILVER / "hvfhv_driver_trip"),
}
REQUIRED_COLUMNS = {
    "trip_key", "driver_id", "customer_id", "lease_id", "taxi_id",
    "pickup_datetime", "lease_started_on", "lease_ended_on", "year_month",
    "snapshot_date", "assignment_seed", "assignment_version", "trip_sequence",
    "deadhead_minutes", "preference_score", "make_key", "model_key", "model_year",
}


def resolve_target_year_month(logical_date: datetime, params: dict) -> str:
    if params.get("year") and params.get("month"):
        return f"{str(params['year']).strip()}-{str(params['month']).strip().zfill(2)}"
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    return (logical_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def validate_input_paths(year_month: str, snapshot_date: str, paths: dict) -> dict:
    date.fromisoformat(snapshot_date)
    datetime.strptime(year_month, "%Y-%m")
    for name in DEFAULT_PATHS:
        if name == "output_path":
            continue
        path = Path(paths[name])
        if name == "trips_path":
            path = path / f"year_month={year_month}"
        elif name == "company_path":
            path = path / f"snapshot_date={snapshot_date}"
        if not path.exists():
            raise FileNotFoundError(f"기사 배정 입력 경로가 없습니다: {name}={path}")
    company = Path(paths["company_path"]) / f"snapshot_date={snapshot_date}"
    for filename in ("customer.parquet", "lease_contract.parquet", "taxi.parquet"):
        if not (company / filename).is_file():
            raise FileNotFoundError(f"회사 스냅샷 파일이 없습니다: {company / filename}")
    return {"year_month": year_month, "snapshot_date": snapshot_date}


def validate_silver_partition(output_dir: str | Path, year_month: str) -> None:
    partition = Path(output_dir) / f"year_month={year_month}"
    files = sorted(partition.glob("*.parquet"))
    if not files:
        raise ValueError(f"기사 배정 Silver 파일이 없습니다: {partition}")
    tables = [pq.ParquetFile(path).read() for path in files]
    table = pa.concat_tables(tables)
    if table.num_rows == 0:
        raise ValueError(f"기사 배정 Silver 행 수가 0입니다: {partition}")
    missing = REQUIRED_COLUMNS - set(table.column_names)
    if missing:
        raise ValueError(f"기사 배정 Silver 필수 컬럼 누락: {sorted(missing)}")
    frame = table.to_pandas()
    keys = ["trip_key", "driver_id", "customer_id", "lease_id", "taxi_id"]
    if frame[keys].isna().any().any() or frame["trip_key"].duplicated().any():
        raise ValueError("기사 배정 Silver 키가 null이거나 trip_key가 중복됩니다")
    if set(frame["year_month"]) != {year_month}:
        raise ValueError("기사 배정 Silver 행의 year_month가 파티션과 다릅니다")
    pickup_date = frame["pickup_datetime"].dt.date
    started = frame["lease_started_on"]
    ended = frame["lease_ended_on"]
    if ((pickup_date < started) | (ended.notna() & (pickup_date >= ended))).any():
        raise ValueError("운행일이 대여 계약 기간 밖입니다")


default_args = {
    "owner": "DE_team1", "retries": 1, "retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="hvfhv_driver_trip_silver_pipeline", default_args=default_args,
    schedule="0 1 12 * *", start_date=datetime(2024, 1, 1), catchup=False,
    max_active_runs=1, tags=["hvfhv", "driver", "silver", "spark"],
    params={
        "year": Param(None, type=["string", "null"]),
        "month": Param(None, type=["string", "null"]),
        "snapshot_date": Param(None, type=["string", "null"]),
        "seed": Param(42, type="integer"),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
    },
)
def driver_trip_pipeline():
    @task(task_id="validate_inputs")
    def validate_inputs(**context):
        params = context["params"]
        logical_date = context.get("logical_date") or datetime.now(timezone.utc)
        year_month = resolve_target_year_month(logical_date, params)
        snapshot_date = params.get("snapshot_date") or f"{year_month}-01"
        return validate_input_paths(year_month, snapshot_date, params)

    build = BashOperator(
        task_id="build_driver_trip_silver",
        bash_command=(
            f"python {ROOT}/spark/jobs/driver_assignment/silver_job.py "
            + "--trips_path {{ params.trips_path }}/year_month="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }} "
            + "--preferences_path {{ params.preferences_path }} "
            + "--customers_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/customer.parquet "
            + "--leases_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/lease_contract.parquet "
            + "--taxis_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/taxi.parquet "
            + "--travel_times_path {{ params.travel_times_path }} --output_path {{ params.output_path }}"
            + " --year_month {{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }}"
            + " --snapshot_date {{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}"
            + " --seed {{ params.seed }}"
        ),
        env={**os.environ, "PYTHONPATH": f"{ROOT}:{ROOT}/spark:{os.getenv('PYTHONPATH', '')}"},
    )

    @task(task_id="validate_silver")
    def validate_silver(**context):
        result = context["task_instance"].xcom_pull(task_ids="validate_inputs")
        validate_silver_partition(context["params"]["output_path"], result["year_month"])

    result = validate_inputs()
    result >> build >> validate_silver()


hvfhv_driver_trip_silver_dag = driver_trip_pipeline()
