from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

DATASET_ID = "datarspectrum/retail-data-warehouse-12-table-1m-rows-dataset"
DEFAULT_DB_NAME = "benchmark_db"
DEFAULT_ORDERS_TARGET_ROWS = 9_000_000


def ensure_supported_python() -> None:
    if sys.version_info >= (3, 14):
        raise RuntimeError(
            "Python 3.14 detected. Use Python 3.11 or 3.12 for this project (numpy/pandas may crash or fail to install on 3.14 on Windows)."
        )


_PANDAS = None


def _pd():
    global _PANDAS
    if _PANDAS is None:
        ensure_supported_python()
        import pandas as pd

        _PANDAS = pd
    return _PANDAS


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def download_dataset(dataset_id: str = DATASET_ID) -> Path:
    try:
        import kagglehub
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'kagglehub'. Run: pip install -r requirements.txt"
        ) from exc

    path_str = kagglehub.dataset_download(dataset_id)
    return Path(path_str)


def discover_csv_tables(dataset_path: Path) -> dict[str, Path]:
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset path does not exist: {dataset_path}")

    csv_files = list(dataset_path.rglob("*.csv"))
    if not csv_files:
        raise RuntimeError(
            f"No CSV files found under: {dataset_path}. Contents: {list(dataset_path.iterdir())[:20]}"
        )

    tables: dict[str, Path] = {}
    for p in csv_files:
        table = normalize_table_name(p.stem)
        tables[table] = p

    return tables


_identifier_re = re.compile(r"[^a-zA-Z0-9_]")


def normalize_table_name(name: str) -> str:
    cleaned = name.strip().lower().replace("-", "_").replace(" ", "_")
    cleaned = _identifier_re.sub("_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def normalize_column_name(name: str) -> str:
    return normalize_table_name(name)


@dataclass(frozen=True)
class ForeignKeyDef:
    column: str
    ref_table: str
    ref_column: str
    name: str


@dataclass(frozen=True)
class TableDef:
    name: str
    csv_path: Path
    columns: list[str]
    primary_key: str
    foreign_keys: list[ForeignKeyDef]


def read_csv_header(csv_path: Path) -> list[str]:
    pd = _pd()
    df = pd.read_csv(csv_path, nrows=0)
    return [normalize_column_name(c) for c in df.columns.tolist()]


def guess_primary_key(table: str, columns: list[str]) -> str:
    table_singular = table[:-1] if table.endswith("s") else table
    candidates = [
        "id",
        f"{table}_id",
        f"{table_singular}_id",
        f"{table}_key",
        f"{table_singular}_key",
    ]
    for cand in candidates:
        if cand in columns:
            return cand

    id_cols = [c for c in columns if c.endswith("_id")]
    if len(id_cols) == 1:
        return id_cols[0]

    return "__row_id"


def _pluralize(name: str) -> str:
    if name.endswith("s"):
        return name
    return f"{name}s"


def _singularize(name: str) -> str:
    if name.endswith("s") and len(name) > 1:
        return name[:-1]
    return name


def guess_foreign_keys(
    table: str,
    columns: list[str],
    known_tables: set[str],
    pk_by_table: dict[str, str],
) -> list[ForeignKeyDef]:
    fks: list[ForeignKeyDef] = []
    for col in columns:
        if not col.endswith("_id"):
            continue
        if col == pk_by_table.get(table):
            continue

        base = col[: -len("_id")]
        candidates = [
            base,
            _pluralize(base),
            _singularize(base),
            _pluralize(_singularize(base)),
        ]
        ref_table = next((t for t in candidates if t in known_tables), None)
        if not ref_table:
            continue

        ref_col = pk_by_table.get(ref_table, "id")
        fk_name = normalize_table_name(f"fk_{table}_{col}_{ref_table}_{ref_col}")
        fks.append(
            ForeignKeyDef(
                column=col,
                ref_table=ref_table,
                ref_column=ref_col,
                name=fk_name,
            )
        )

    return fks


def build_table_defs(dataset_path: Path) -> list[TableDef]:
    tables = discover_csv_tables(dataset_path)

    headers: dict[str, list[str]] = {
        t: read_csv_header(path) for t, path in tables.items()
    }

    pk_by_table: dict[str, str] = {
        t: guess_primary_key(t, cols) for t, cols in headers.items()
    }

    defs: list[TableDef] = []
    known = set(headers.keys())
    for t, cols in headers.items():
        fks = guess_foreign_keys(t, cols, known, pk_by_table)
        defs.append(
            TableDef(
                name=t,
                csv_path=tables[t],
                columns=cols,
                primary_key=pk_by_table[t],
                foreign_keys=fks,
            )
        )

    return sorted(defs, key=lambda d: d.name)


def is_orders_table(table_name: str) -> bool:
    return normalize_table_name(table_name) == "orders"


def guess_column_type_sql(col: str) -> str:
    col_n = normalize_column_name(col)
    if col_n == "__row_id":
        return "BIGINT"
    if col_n.endswith("_id") or col_n.endswith("_key"):
        return "BIGINT"
    if any(token in col_n for token in ("date", "time", "created", "updated")):
        return "TIMESTAMP"
    return "TEXT"


def guess_column_type_mysql(col: str) -> str:
    col_n = normalize_column_name(col)
    if col_n == "__row_id":
        return "BIGINT"
    if col_n.endswith("_id") or col_n.endswith("_key"):
        return "BIGINT"
    if any(token in col_n for token in ("date", "time", "created", "updated")):
        return "DATETIME"
    return "TEXT"


def chunked_iterable(items: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def dataframe_rows_as_tuples(df: Any, columns: list[str]) -> list[tuple[Any, ...]]:
    pd = _pd()
    subset = df[columns]
    subset = subset.where(pd.notna(subset), None)
    return [tuple(row) for row in subset.itertuples(index=False, name=None)]
