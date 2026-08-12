"""회사 고객·택시·리스 원천 DB 스냅샷을 읽고 관계 무결성을 검증합니다."""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

TABLES = ("customer", "taxi", "lease_contract")
PRIMARY_KEYS = {
    "customer": "customer_id",
    "taxi": "taxi_id",
    "lease_contract": "lease_id",
}


class CompanySnapshotExtractor(Extractor):
    """한 날짜의 세 원천 Parquet을 변형 없이 읽습니다."""

    name = "company_snapshot"

    def __init__(self, source_dir: str, snapshot_date: str):
        try:
            date.fromisoformat(snapshot_date)
        except ValueError as exc:
            raise ValueError("snapshot_date는 유효한 YYYY-MM-DD 형식이어야 합니다.") from exc
        self._partition = Path(source_dir) / f"snapshot_date={snapshot_date}"
        self._snapshot_date = snapshot_date

    def extract(self) -> dict[str, pa.Table]:
        tables: dict[str, pa.Table] = {}
        for name in TABLES:
            path = self._partition / f"{name}.parquet"
            if not path.is_file():
                raise FileNotFoundError(f"회사 원천 스냅샷 파일이 없습니다: {path}")
            try:
                table = pq.ParquetFile(path).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"회사 원천 스냅샷을 읽지 못했습니다: {path}") from exc
            if table.num_rows == 0:
                raise ValueError(f"회사 원천 스냅샷이 비어 있습니다: {path}")
            tables[name] = table

        self._validate(tables)
        return tables

    def _validate(self, tables: dict[str, pa.Table]) -> None:
        for name, table in tables.items():
            required = {PRIMARY_KEYS[name], "snapshot_date"}
            if name == "lease_contract":
                required |= {"customer_id", "taxi_id"}
            missing = required - set(table.column_names)
            if missing:
                raise ValueError(f"{name} 필수 컬럼 누락: {sorted(missing)}")

            rows = table.to_pylist()
            primary_key = PRIMARY_KEYS[name]
            ids = [row[primary_key] for row in rows]
            if any(value is None for value in ids) or len(ids) != len(set(ids)):
                raise ValueError(f"{name}.{primary_key}는 null 없이 고유해야 합니다")
            dates = {str(row["snapshot_date"]) for row in rows}
            if dates != {self._snapshot_date}:
                raise ValueError(
                    f"{name} snapshot_date 불일치: 요청={self._snapshot_date}, 데이터={sorted(dates)}"
                )

        customers = set(tables["customer"]["customer_id"].to_pylist())
        taxis = set(tables["taxi"]["taxi_id"].to_pylist())
        contracts = tables["lease_contract"].to_pylist()
        missing_customers = {row["customer_id"] for row in contracts} - customers
        missing_taxis = {row["taxi_id"] for row in contracts} - taxis
        if missing_customers or missing_taxis:
            raise ValueError(
                "lease_contract FK 위반: "
                f"customer_id={sorted(missing_customers)}, taxi_id={sorted(missing_taxis)}"
            )
