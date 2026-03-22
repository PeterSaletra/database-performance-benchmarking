from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def _require_cassandra_driver():
    try:
        from cassandra.concurrent import execute_concurrent_with_args
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "ScyllaDB import requires dependency 'cassandra-driver'. On Windows, use Python 3.11 or 3.12 (Python 3.13+ may not be supported by cassandra-driver yet)."
        ) from exc
    except Exception as exc:
        msg = str(exc)
        if "Unable to load a default connection class" in msg and sys.version_info >= (3, 12):
            raise RuntimeError(
                "cassandra-driver failed to import due to reactor selection on Python 3.12+. "
                "Upgrade the driver (recommended: cassandra-driver==3.29.3) and retry. "
                "If you still hit this, use Python 3.11 for ScyllaDB. Original error: "
                + msg
            ) from exc
        raise

    connection_class = None
    if sys.platform.startswith("win") and sys.version_info >= (3, 12):
        try:
            import asyncio

            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

    if sys.version_info >= (3, 12):
        try:
            from cassandra.io.asyncioreactor import AsyncioConnection

            connection_class = AsyncioConnection
        except Exception as exc:
            raise RuntimeError(
                "Python 3.12+ requires cassandra-driver asyncio reactor, but it could not be imported. "
                "Try reinstalling cassandra-driver in this venv. Original error: "
                + repr(exc)
            ) from exc

    from cassandra.cluster import Cluster
    return Cluster, execute_concurrent_with_args


_CASSANDRA_DRIVER: tuple[Any, Any] | None = None


def _cassandra_driver() -> tuple[Any, Any]:
    global _CASSANDRA_DRIVER
    if _CASSANDRA_DRIVER is None:
        _CASSANDRA_DRIVER = _require_cassandra_driver()
    return _CASSANDRA_DRIVER


def _execute_concurrent_with_args(session, prepared, args, concurrency: int) -> None:
    _, execute_concurrent_with_args = _cassandra_driver()
    execute_concurrent_with_args(session, prepared, args, concurrency=concurrency)

from retail_denormalize import (
    build_inventory_docs,
    build_optional_order_object_cache_rows,
    build_order_items_cache_rows,
    build_table_ref_map,
    compute_csv_max_int,
    default_cache_path,
    denorm_load_customers,
    denorm_load_products,
    denorm_load_stores,
    fetch_items_for_orders,
    fetch_optional_by_order_id,
    iter_csv_rows,
    pick_table,
    rebuild_sqlite_cache,
    stable_object_id_hex,
)
from retail_import_common import (
    DEFAULT_ORDERS_TARGET_ROWS,
    build_table_defs,
    download_dataset,
    ensure_supported_python,
    is_orders_table,
    load_env_file,
    normalize_column_name,
    normalize_table_name,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import the Kaggle Retail Data Warehouse dataset into ScyllaDB (Cassandra-compatible)."
    )
    parser.add_argument(
        "--mode",
        choices=["tables", "denormalized"],
        default=os.getenv("NOSQL_MODE", "denormalized"),
        help="Import mode: 'tables' (1 CSV -> table with (id,data)) or 'denormalized' (ERD-like JSON docs).",
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("KAGGLE_DATASET_ID", None) or None,
        help="Kaggle dataset id (default: retail DWH 12-table dataset).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop keyspace contents before import.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("IMPORT_BATCH_SIZE", "10000")),
        help="CSV chunk size (default: 10000).",
    )
    parser.add_argument(
        "--orders-target-rows",
        type=int,
        default=int(os.getenv("ORDERS_TARGET_ROWS", str(DEFAULT_ORDERS_TARGET_ROWS))),
        help=f"Target number of orders (default: {DEFAULT_ORDERS_TARGET_ROWS}).",
    )
    return parser.parse_args()


def _scylla_session() -> tuple[Any, Any, str]:
    host = os.getenv("SCYLLA_HOST", "localhost")
    port = int(os.getenv("SCYLLA_PORT", "9042"))
    keyspace = os.getenv("SCYLLA_KEYSPACE", "benchmark_db")

    Cluster, _ = _cassandra_driver()

    kwargs = {}
    if sys.version_info >= (3, 12):
        if sys.platform.startswith("win"):
            try:
                import asyncio

                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except Exception:
                pass

        try:
            from cassandra.io.asyncioreactor import AsyncioConnection
        except Exception as exc:
            raise RuntimeError(
                "Python 3.12+ requires cassandra-driver asyncio reactor, but it could not be imported. "
                "Try reinstalling cassandra-driver in this venv. Original error: "
                + repr(exc)
            ) from exc

        kwargs["connection_class"] = AsyncioConnection

    cluster = Cluster([host], port=port, **kwargs)
    session = cluster.connect()

    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"
    )
    session.set_keyspace(keyspace)
    return cluster, session, keyspace


def _sanitize_cql_ident(name: str) -> str:
    n = normalize_table_name(name)
    n = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in n)
    if not n or n[0].isdigit():
        n = f"t_{n}"
    return n


def _ensure_kv_table(session, table: str) -> None:
    session.execute(f"CREATE TABLE IF NOT EXISTS {table} (id text PRIMARY KEY, data text);")


def _truncate_if_exists(session, table: str) -> None:
    try:
        session.execute(f"TRUNCATE {table};")
    except Exception:
        pass


def import_table_scylla(session, table_def, batch_size: int, reset: bool) -> None:
    import pandas as pd

    src_table = table_def.name
    table = f"retail_{_sanitize_cql_ident(src_table)}"
    pk = normalize_column_name(table_def.primary_key)

    _ensure_kv_table(session, table)
    if reset:
        _truncate_if_exists(session, table)

    prepared = session.prepare(f"INSERT INTO {table} (id, data) VALUES (?, ?)")

    inserted = 0
    t0 = time.perf_counter()

    for chunk in pd.read_csv(table_def.csv_path, chunksize=batch_size, dtype=str):
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        chunk = chunk.where(pd.notna(chunk), None)

        rows: list[tuple[str, str]] = []
        for i, row in enumerate(chunk.to_dict(orient="records")):
            rid = row.get(pk)
            if rid is None:
                rid = f"{inserted + i + 1}"
            payload = json.dumps({k: v for k, v in row.items() if v is not None}, ensure_ascii=False)
            rows.append((str(rid), payload))

        if rows:
            _execute_concurrent_with_args(session, prepared, rows, concurrency=200)
            inserted += len(rows)

        if inserted and (inserted == len(rows) or inserted % 200_000 == 0):
            print(f"  - {src_table}: inserted {inserted:,} rows in {time.perf_counter() - t0:.1f}s", flush=True)

    print(f"  - {src_table}: done ({inserted:,} rows) in {time.perf_counter() - t0:.1f}s", flush=True)


def expand_orders_scylla(session, orders_def, target_rows: int, batch_size: int) -> None:
    import pandas as pd

    table = f"retail_{_sanitize_cql_ident(orders_def.name)}"
    pk = normalize_column_name(orders_def.primary_key)

    _ensure_kv_table(session, table)
    prepared = session.prepare(f"INSERT INTO {table} (id, data) VALUES (?, ?)")

    max_pk_int = compute_csv_max_int(orders_def.csv_path, pk, chunksize=batch_size)
    if max_pk_int is None:
        max_pk_int = 0

    inserted_total = 0
    pass_idx = 0
    t0 = time.perf_counter()

    while inserted_total < target_rows:
        offset = max_pk_int * pass_idx
        print(f"  - pass {pass_idx + 1}: offset={offset:,}, inserted_total={inserted_total:,}", flush=True)

        for chunk in pd.read_csv(orders_def.csv_path, chunksize=batch_size, dtype=str):
            if inserted_total >= target_rows:
                break

            chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
            chunk = chunk.where(pd.notna(chunk), None)

            rows: list[tuple[str, str]] = []
            for row in chunk.to_dict(orient="records"):
                if inserted_total + len(rows) >= target_rows:
                    break

                base = row.get(pk)
                if base is None:
                    continue
                try:
                    new_id = str(int(str(base)) + offset) if pass_idx > 0 else str(base)
                except Exception:
                    new_id = f"{base}_{pass_idx}" if pass_idx > 0 else str(base)

                payload = json.dumps({k: v for k, v in row.items() if v is not None}, ensure_ascii=False)
                rows.append((new_id, payload))

            if rows:
                _execute_concurrent_with_args(session, prepared, rows, concurrency=200)
                inserted_total += len(rows)

            if inserted_total and (inserted_total == len(rows) or inserted_total % 200_000 == 0):
                print(
                    f"  - orders inserted: {inserted_total:,}/{target_rows:,} in {time.perf_counter() - t0:.1f}s",
                    flush=True,
                )

        pass_idx += 1


def _ensure_doc_table(session, table: str) -> None:
    _ensure_kv_table(session, table)


def _insert_doc(session, prepared, doc_id: str, doc: dict[str, Any]) -> None:
    _execute_concurrent_with_args(
        session,
        prepared,
        [(doc_id, json.dumps(doc, ensure_ascii=False))],
        concurrency=1,
    )


def import_denormalized_scylla(session, table_defs, batch_size: int, reset: bool, orders_target_rows: int) -> None:
    table_refs = build_table_ref_map(table_defs)

    customers_ref = pick_table(table_refs, "customers", "customer")
    stores_ref = pick_table(table_refs, "stores", "store")
    products_ref = pick_table(table_refs, "products", "product")
    orders_ref = pick_table(table_refs, "orders", "order")
    order_items_ref = pick_table(
        table_refs,
        "order_items",
        "order_item",
        "order_details",
        "order_lines",
        "order_line_items",
    )
    inventory_ref = pick_table(table_refs, "inventory", "stock")
    employees_ref = pick_table(table_refs, "employees", "staff")
    categories_ref = pick_table(table_refs, "categories", "category")
    suppliers_ref = pick_table(table_refs, "suppliers", "supplier")
    payments_ref = pick_table(table_refs, "payments", "payment")
    shipments_ref = pick_table(table_refs, "shipments", "shipment")

    missing = [
        name
        for name, ref in [
            ("customers", customers_ref),
            ("stores", stores_ref),
            ("products", products_ref),
            ("orders", orders_ref),
            ("order_items", order_items_ref),
        ]
        if ref is None
    ]
    if missing:
        raise RuntimeError(
            "Denormalized mode requires at least these tables in the dataset: " + ", ".join(missing)
        )

    t_customers = "retail_customers"
    t_stores = "retail_stores"
    t_products = "retail_products"
    t_inventory = "retail_inventory"
    t_orders = "retail_orders"

    for t in (t_customers, t_stores, t_products, t_inventory, t_orders):
        _ensure_doc_table(session, t)
        if reset:
            _truncate_if_exists(session, t)

    prep_customers = session.prepare(f"INSERT INTO {t_customers} (id, data) VALUES (?, ?)")
    prep_stores = session.prepare(f"INSERT INTO {t_stores} (id, data) VALUES (?, ?)")
    prep_products = session.prepare(f"INSERT INTO {t_products} (id, data) VALUES (?, ?)")
    prep_inventory = session.prepare(f"INSERT INTO {t_inventory} (id, data) VALUES (?, ?)")
    prep_orders = session.prepare(f"INSERT INTO {t_orders} (id, data) VALUES (?, ?)")

    print("Loading base entities (customers/stores/products)...", flush=True)
    t0 = time.perf_counter()

    customers_by_src = denorm_load_customers(customers_ref, chunksize=batch_size)
    stores_by_src = denorm_load_stores(stores_ref, employees_ref, chunksize=batch_size)
    products_by_src = denorm_load_products(products_ref, categories_ref, suppliers_ref, chunksize=batch_size)

    print(
        f"  - loaded customers={len(customers_by_src):,}, stores={len(stores_by_src):,}, products={len(products_by_src):,} in {time.perf_counter() - t0:.2f}s",
        flush=True,
    )

    print("Writing base documents to ScyllaDB...", flush=True)
    t0 = time.perf_counter()

    _execute_concurrent_with_args(
        session,
        prep_customers,
        [(doc["_id"], json.dumps(doc, ensure_ascii=False)) for doc in customers_by_src.values()],
        concurrency=200,
    )
    _execute_concurrent_with_args(
        session,
        prep_stores,
        [(doc["_id"], json.dumps(doc, ensure_ascii=False)) for doc in stores_by_src.values()],
        concurrency=200,
    )
    _execute_concurrent_with_args(
        session,
        prep_products,
        [(doc["_id"], json.dumps(doc, ensure_ascii=False)) for doc in products_by_src.values()],
        concurrency=200,
    )

    print(f"  - base docs written in {time.perf_counter() - t0:.2f}s", flush=True)

    if inventory_ref is not None:
        print("Building + writing inventory documents...", flush=True)
        t0 = time.perf_counter()
        inv_docs = build_inventory_docs(inventory_ref, stores_by_src, products_by_src, chunksize=batch_size)
        _execute_concurrent_with_args(
            session,
            prep_inventory,
            [(d["_id"], json.dumps(d, ensure_ascii=False)) for d in inv_docs],
            concurrency=200,
        )
        print(f"  - inventory written in {time.perf_counter() - t0:.2f}s", flush=True)

    cache_path = default_cache_path("retail_denorm.sqlite")

    print("Building SQLite cache for order_items (+ optional payments/shipments)...", flush=True)
    t0 = time.perf_counter()

    order_items_rows = build_order_items_cache_rows(order_items_ref, products_by_src, chunksize=batch_size)
    payments_rows = (
        build_optional_order_object_cache_rows(payments_ref, chunksize=batch_size) if payments_ref is not None else None
    )
    shipments_rows = (
        build_optional_order_object_cache_rows(shipments_ref, chunksize=batch_size) if shipments_ref is not None else None
    )

    rebuild_sqlite_cache(cache_path, order_items_rows, payments_rows, shipments_rows)

    print(f"  - SQLite cache ready in {time.perf_counter() - t0:.2f}s ({cache_path})", flush=True)

    orders_pk = normalize_column_name(orders_ref.primary_key)
    customer_fk = "customer_id" if "customer_id" in orders_ref.columns else None
    store_fk = "store_id" if "store_id" in orders_ref.columns else None
    date_col = "order_date" if "order_date" in orders_ref.columns else None
    status_col = "status" if "status" in orders_ref.columns else None

    max_order_id = compute_csv_max_int(orders_ref.csv_path, orders_pk, chunksize=batch_size)
    pass_idx = 0
    inserted_total = 0

    print(f"Writing denormalized orders to ScyllaDB (target {orders_target_rows:,})...", flush=True)
    if max_order_id is not None:
        print(f"  - max {orders_pk} in CSV: {max_order_id:,}", flush=True)

    keep_cache = str(os.environ.get("RETAIL_KEEP_CACHE", "")).strip().lower() in {"1", "true", "yes"}
    try:
        while inserted_total < orders_target_rows:
            offset = (max_order_id or 0) * pass_idx

            print(
                f"  - pass {pass_idx + 1}: offset={offset:,}, inserted_total={inserted_total:,}",
                flush=True,
            )

            for rows in iter_csv_rows(orders_ref.csv_path, chunksize=batch_size):
                if inserted_total >= orders_target_rows:
                    break

                base_order_ids: list[str] = []
                base_rows: list[dict[str, object]] = []
                for row in rows:
                    if inserted_total + len(base_rows) >= orders_target_rows:
                        break
                    if row.get(orders_pk) is None:
                        continue
                    base_order_ids.append(str(row[orders_pk]))
                    base_rows.append(row)

                items_by_order = fetch_items_for_orders(cache_path, base_order_ids)
                payments_by_order = (
                    fetch_optional_by_order_id(cache_path, "payments", base_order_ids) if payments_ref is not None else {}
                )
                shipments_by_order = (
                    fetch_optional_by_order_id(cache_path, "shipments", base_order_ids)
                    if shipments_ref is not None
                    else {}
                )

                docs_args: list[tuple[str, str]] = []
                for row in base_rows:
                    base_id = str(row[orders_pk])
                    new_id = base_id
                    if pass_idx > 0 and max_order_id is not None:
                        try:
                            new_id = str(int(base_id) + offset)
                        except Exception:
                            new_id = f"{base_id}_{pass_idx}"

                    customer_obj = None
                    if customer_fk and row.get(customer_fk) is not None:
                        customer_obj = customers_by_src.get(str(row[customer_fk]))
                    store_obj = None
                    if store_fk and row.get(store_fk) is not None:
                        store_obj = stores_by_src.get(str(row[store_fk]))

                    order_doc = {
                        "_id": stable_object_id_hex("orders", new_id),
                        "customer": customer_obj,
                        "store_id": store_obj.get("_id") if isinstance(store_obj, dict) else None,
                        "order_date": row.get(date_col) if date_col else None,
                        "status": row.get(status_col) if status_col else None,
                        "items": items_by_order.get(base_id, []),
                        "payment": payments_by_order.get(base_id, {}),
                        "shipment": shipments_by_order.get(base_id, {}),
                    }
                    order_doc = {k: v for k, v in order_doc.items() if v not in (None, {}, [])}

                    docs_args.append((order_doc["_id"], json.dumps(order_doc, ensure_ascii=False)))

                if docs_args:
                    _execute_concurrent_with_args(session, prep_orders, docs_args, concurrency=200)
                    inserted_total += len(docs_args)

                if inserted_total and (inserted_total == len(docs_args) or inserted_total % 100_000 == 0):
                    print(f"  - orders inserted: {inserted_total:,}/{orders_target_rows:,}", flush=True)

            pass_idx += 1
    finally:
        if not keep_cache:
            try:
                cache_path.unlink(missing_ok=True)
                print(f"Deleted SQLite cache: {cache_path}", flush=True)
            except Exception:
                pass


def main() -> int:
    load_env_file(Path(".env"))
    args = parse_args()

    ensure_supported_python()

    cluster, session, keyspace = _scylla_session()
    try:
        if args.reset:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace};")
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"
            )
            session.set_keyspace(keyspace)

        dataset_id = args.dataset_id or os.getenv("KAGGLE_DATASET_ID")
        print("Downloading dataset via kagglehub...", flush=True)
        dataset_path = download_dataset(dataset_id) if dataset_id else download_dataset()
        print(f"Dataset ready at: {dataset_path}", flush=True)

        print("Discovering CSV tables...", flush=True)
        table_defs = build_table_defs(dataset_path)
        print(f"Discovered {len(table_defs)} tables.", flush=True)

        if args.mode == "tables":
            for td in table_defs:
                start = time.perf_counter()
                print(f"Importing {td.name}...", flush=True)
                import_table_scylla(session, td, args.batch_size, reset=False)
                print(f"[OK] Imported {td.name} in {time.perf_counter() - start:.2f}s", flush=True)

            orders_def = next((d for d in table_defs if is_orders_table(d.name)), None)
            if orders_def:
                print(f"Expanding orders to {args.orders_target_rows:,} rows...", flush=True)
                start = time.perf_counter()
                expand_orders_scylla(session, orders_def, args.orders_target_rows, args.batch_size)
                print(f"[OK] Orders expanded in {time.perf_counter() - start:.2f}s", flush=True)
        else:
            start = time.perf_counter()
            import_denormalized_scylla(session, table_defs, args.batch_size, reset=False, orders_target_rows=args.orders_target_rows)
            print(f"[OK] Denormalized import finished in {time.perf_counter() - start:.2f}s", flush=True)

    finally:
        try:
            session.shutdown()
        except Exception:
            pass
        cluster.shutdown()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
