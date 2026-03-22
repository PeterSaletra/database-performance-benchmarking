from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import mysql.connector

from retail_import_common import (
    DEFAULT_ORDERS_TARGET_ROWS,
    build_table_defs,
    download_dataset,
    ensure_supported_python,
    guess_column_type_mysql,
    is_orders_table,
    load_env_file,
    normalize_column_name,
    normalize_table_name,
)


def my_ident(name: str) -> str:
    safe = normalize_table_name(name)
    safe = safe.replace("`", "``")
    return f"`{safe}`"


def my_col_ident(name: str) -> str:
    safe = normalize_column_name(name)
    safe = safe.replace("`", "``")
    return f"`{safe}`"


def create_table_mysql(conn, table_def) -> None:
    table = my_ident(table_def.name)
    pk = normalize_column_name(table_def.primary_key)

    column_defs: list[str] = []
    for col in table_def.columns:
        col_n = normalize_column_name(col)
        col_type = guess_column_type_mysql(col_n)
        if col_n == pk and pk != "__row_id":
            column_defs.append(f"{my_col_ident(col_n)} {col_type} AUTO_INCREMENT")
        else:
            column_defs.append(f"{my_col_ident(col_n)} {col_type}")

    if pk == "__row_id":
        column_defs.insert(0, "`__row_id` BIGINT AUTO_INCREMENT PRIMARY KEY")
        pk_clause = ""
    else:
        pk_clause = f", PRIMARY KEY ({my_col_ident(pk)})"

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        + ", ".join(column_defs)
        + pk_clause
        + ") ENGINE=InnoDB;"
    )

    cur = conn.cursor()
    cur.execute(ddl)
    cur.close()


def add_foreign_keys_mysql(conn, table_def) -> None:
    if not table_def.foreign_keys:
        return

    table = my_ident(table_def.name)
    cur = conn.cursor()
    for fk in table_def.foreign_keys:
        try:
            idx_name = normalize_table_name(f"idx_{table_def.name}_{fk.column}")
            cur.execute(
                f"CREATE INDEX {my_ident(idx_name)} ON {table} ({my_col_ident(fk.column)});"
            )
        except Exception:
            conn.rollback()

        try:
            cur.execute(
                f"""
                ALTER TABLE {table}
                ADD CONSTRAINT {my_ident(fk.name)}
                FOREIGN KEY ({my_col_ident(fk.column)})
                REFERENCES {my_ident(fk.ref_table)} ({my_col_ident(fk.ref_column)});
                """
            )
        except Exception:
            conn.rollback()
            continue

    cur.close()


def import_csv_mysql(conn, table_def, batch_size: int) -> None:
    import pandas as pd

    table = my_ident(table_def.name)
    pk = normalize_column_name(table_def.primary_key)

    insert_columns = [normalize_column_name(c) for c in table_def.columns]
    if pk == "__row_id":
        insert_columns = [c for c in insert_columns if c != "__row_id"]

    placeholders = ", ".join(["%s"] * len(insert_columns))
    cols_sql = ", ".join(my_col_ident(c) for c in insert_columns)

    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders});"

    cur = conn.cursor()
    chunk_idx = 0
    imported_rows = 0
    t0 = time.perf_counter()
    for chunk in pd.read_csv(table_def.csv_path, chunksize=batch_size, dtype=str):
        chunk_idx += 1
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        rows = chunk.where(pd.notna(chunk), None)
        values = [tuple(r) for r in rows[insert_columns].itertuples(index=False, name=None)]
        cur.executemany(sql, values)
        conn.commit()

        imported_rows += len(values)
        if chunk_idx == 1 or chunk_idx % 25 == 0:
            elapsed = time.perf_counter() - t0
            print(
                f"  - {table_def.name}: {imported_rows:,} rows inserted ({chunk_idx} chunks) in {elapsed:.1f}s",
                flush=True,
            )

    cur.close()


def ensure_orders_size_mysql(conn, orders_pk: str, target_rows: int) -> None:
    pk = normalize_column_name(orders_pk)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM `orders`;")
    current = int(cur.fetchone()[0])

    if current >= target_rows:
        cur.close()
        return

    if pk != "__row_id":
        cur.execute(f"SELECT COALESCE(MAX({my_col_ident(pk)}), 0) FROM `orders`;")
        max_pk = int(cur.fetchone()[0])
        cur.execute(f"ALTER TABLE `orders` AUTO_INCREMENT = {max_pk + 1};")
        conn.commit()

    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'orders'
        ORDER BY ORDINAL_POSITION;
        """
    )
    cols = [r[0] for r in cur.fetchall() if r[0] != pk]
    cols_sql = ", ".join(my_col_ident(c) for c in cols)

    batch_limit = int(os.getenv("ORDERS_EXPAND_BATCH", "50000"))
    if batch_limit <= 0:
        batch_limit = 50000

    print(
        f"  - orders currently {current:,}, target {target_rows:,}, expand batch {batch_limit:,}",
        flush=True,
    )

    while current < target_rows:
        to_add = min(target_rows - current, batch_limit)
        t0 = time.perf_counter()
        cur.execute(
            f"INSERT INTO `orders` ({cols_sql}) SELECT {cols_sql} FROM `orders` LIMIT %s;",
            (to_add,),
        )
        conn.commit()
        added = cur.rowcount if cur.rowcount is not None else to_add
        current += added
        elapsed = time.perf_counter() - t0
        print(
            f"  - orders expanded: +{added:,} -> {current:,}/{target_rows:,} ({elapsed:.2f}s)",
            flush=True,
        )

    cur.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import the Kaggle Retail Data Warehouse dataset into MySQL."
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("KAGGLE_DATASET_ID", None) or None,
        help="Kaggle dataset id (default: retail DWH 12-table dataset).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate tables before import.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("IMPORT_BATCH_SIZE", "5000")),
        help="CSV chunk size / insert batch size (default: 5000).",
    )
    parser.add_argument(
        "--orders-target-rows",
        type=int,
        default=int(os.getenv("ORDERS_TARGET_ROWS", str(DEFAULT_ORDERS_TARGET_ROWS))),
        help=f"Target number of rows in orders table (default: {DEFAULT_ORDERS_TARGET_ROWS}).",
    )
    return parser.parse_args()


def main() -> int:
    load_env_file(Path(".env"))
    args = parse_args()

    ensure_supported_python()

    dataset_id = args.dataset_id or os.getenv("KAGGLE_DATASET_ID")
    print("Downloading dataset via kagglehub...", flush=True)
    dataset_path = download_dataset(dataset_id) if dataset_id else download_dataset()
    print(f"Dataset ready at: {dataset_path}", flush=True)

    print("Discovering CSV tables...", flush=True)
    table_defs = build_table_defs(dataset_path)
    print(f"Discovered {len(table_defs)} tables.", flush=True)

    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "benchmark_db"),
        user=os.getenv("MYSQL_USER", "benchmark_user"),
        password=os.getenv("MYSQL_PASSWORD", "benchmark_pass"),
        autocommit=False,
    )

    try:
        if args.reset:
            cur = conn.cursor()
            print("Reset enabled: dropping existing tables...", flush=True)
            cur.execute("SET FOREIGN_KEY_CHECKS=0;")
            for td in reversed(table_defs):
                cur.execute(f"DROP TABLE IF EXISTS {my_ident(td.name)};")
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()
            cur.close()

        for td in table_defs:
            create_table_mysql(conn, td)
        conn.commit()

        for td in table_defs:
            start = time.perf_counter()
            print(f"Importing {td.name}...", flush=True)
            import_csv_mysql(conn, td, args.batch_size)
            print(
                f"[OK] Imported {td.name} in {time.perf_counter() - start:.2f}s",
                flush=True,
            )

        for td in table_defs:
            add_foreign_keys_mysql(conn, td)
        conn.commit()

        orders_def = next((d for d in table_defs if is_orders_table(d.name)), None)
        if orders_def:
            print(
                f"Expanding orders to {args.orders_target_rows:,} rows (database-side duplication)...",
                flush=True,
            )
            start = time.perf_counter()
            ensure_orders_size_mysql(conn, orders_def.primary_key, args.orders_target_rows)
            print(
                f"[OK] Orders expanded in {time.perf_counter() - start:.2f}s",
                flush=True,
            )

    finally:
        conn.close()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
