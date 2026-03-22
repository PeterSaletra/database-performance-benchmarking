from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import redis

from retail_import_common import (
    DEFAULT_ORDERS_TARGET_ROWS,
    build_table_defs,
    download_dataset,
    ensure_supported_python,
    is_orders_table,
    load_env_file,
    normalize_column_name,
)

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


def _try_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except Exception:
        return None


def _compute_csv_max_int(csv_path: Path, column: str, chunksize: int) -> int | None:
    import pandas as pd

    max_value: int | None = None
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, dtype=str):
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        if column not in chunk.columns:
            return None
        series = pd.to_numeric(chunk[column], errors="coerce")
        v = series.max(skipna=True)
        if pd.isna(v):
            continue
        v_int = int(v)
        max_value = v_int if max_value is None else max(max_value, v_int)
    return max_value


def _delete_prefix(client: redis.Redis, prefix: str) -> None:
    keys = []
    for key in client.scan_iter(match=f"{prefix}*", count=5000):
        keys.append(key)
        if len(keys) >= 5000:
            client.delete(*keys)
            keys.clear()
    if keys:
        client.delete(*keys)


def import_table_redis(client: redis.Redis, table_def, batch_size: int, reset: bool) -> None:
    import pandas as pd

    table = table_def.name
    pk = normalize_column_name(table_def.primary_key)

    prefix = f"retail:{table}:"
    count_key = f"retail:count:{table}"

    if reset:
        _delete_prefix(client, prefix)
        client.delete(count_key)

    next_row_id = int(client.get(count_key) or 0) + 1

    chunk_idx = 0
    inserted_total = 0
    t0 = time.perf_counter()
    for chunk in pd.read_csv(table_def.csv_path, chunksize=batch_size, dtype=str):
        chunk_idx += 1
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        chunk = chunk.where(pd.notna(chunk), None)

        pipe = client.pipeline(transaction=False)

        inserted = 0
        for row in chunk.to_dict(orient="records"):
            row = {k: v for k, v in row.items() if v is not None}

            if pk != "__row_id" and pk in row:
                row_id = row[pk]
            else:
                row_id = str(next_row_id)
                next_row_id += 1

            key = f"{prefix}{row_id}"
            if row:
                pipe.hset(key, mapping={k: str(v) for k, v in row.items()})
            else:
                pipe.hset(key, mapping={})
            inserted += 1

        if inserted:
            pipe.incrby(count_key, inserted)
        pipe.execute()

        inserted_total += inserted
        if chunk_idx == 1 or chunk_idx % 25 == 0:
            elapsed = time.perf_counter() - t0
            print(
                f"  - {table}: {inserted_total:,} rows inserted ({chunk_idx} chunks) in {elapsed:.1f}s",
                flush=True,
            )


def expand_orders_redis(client: redis.Redis, orders_def, target_rows: int, batch_size: int) -> None:
    import pandas as pd

    table = "orders"
    pk = normalize_column_name(orders_def.primary_key)
    prefix = f"retail:{table}:"
    count_key = f"retail:count:{table}"

    current = int(client.get(count_key) or 0)
    if current >= target_rows:
        return

    max_pk_int = None
    if pk != "__row_id":
        max_pk_int = _compute_csv_max_int(orders_def.csv_path, pk, chunksize=batch_size)

    pass_idx = 1
    while current < target_rows:
        offset = (max_pk_int or 0) * pass_idx
        inserted_this_pass = 0

        print(
            f"  - pass {pass_idx}: current={current:,}, target={target_rows:,}, offset={offset:,}",
            flush=True,
        )

        for chunk in pd.read_csv(orders_def.csv_path, chunksize=batch_size, dtype=str):
            if current + inserted_this_pass >= target_rows:
                break

            chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
            chunk = chunk.where(pd.notna(chunk), None)

            pipe = client.pipeline(transaction=False)
            for row in chunk.to_dict(orient="records"):
                if current + inserted_this_pass >= target_rows:
                    break

                row = {k: v for k, v in row.items() if v is not None}

                if pk != "__row_id" and pk in row:
                    old = row.get(pk)
                    old_int = _try_int(old)
                    if old_int is not None and max_pk_int is not None:
                        new_id = str(old_int + offset)
                    else:
                        new_id = f"{old}_{pass_idx}_{inserted_this_pass}"
                    row[pk] = new_id
                    row_id = new_id
                else:
                    row_id = str(current + inserted_this_pass + 1)

                key = f"{prefix}{row_id}"
                pipe.hset(key, mapping={k: str(v) for k, v in row.items()})
                inserted_this_pass += 1

            if inserted_this_pass:
                pipe.incrby(count_key, inserted_this_pass)
            pipe.execute()

            if inserted_this_pass and inserted_this_pass % 100_000 == 0:
                print(
                    f"  - orders expanded: +{inserted_this_pass:,} -> {current + inserted_this_pass:,}/{target_rows:,}",
                    flush=True,
                )

        current += inserted_this_pass
        pass_idx += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import the Kaggle Retail Data Warehouse dataset into Redis (hashes per row)."
    )
    parser.add_argument(
        "--mode",
        choices=["tables", "denormalized"],
        default=os.getenv("NOSQL_MODE", "denormalized"),
        help="Import mode: 'tables' (1 CSV -> hashes per row) or 'denormalized' (ERD-like JSON documents).",
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("KAGGLE_DATASET_ID", None) or None,
        help="Kaggle dataset id (default: retail DWH 12-table dataset).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing keys with prefix retail:* before import.",
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
        help=f"Target number of rows in orders table (default: {DEFAULT_ORDERS_TARGET_ROWS}).",
    )
    return parser.parse_args()


def _delete_prefixes(client: redis.Redis, prefixes: list[str]) -> None:
    for p in prefixes:
        _delete_prefix(client, p)


def _set_json(client: redis.Redis, key: str, obj: dict[str, object]) -> None:
    client.set(key, json.dumps(obj, ensure_ascii=False))


def import_denormalized_redis(
    client: redis.Redis,
    table_defs,
    batch_size: int,
    reset: bool,
    orders_target_rows: int,
) -> None:
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
    shipments_ref = pick_table(table_refs, "shipments", "shipment", "shipping")

    if not (customers_ref and stores_ref and products_ref and orders_ref and order_items_ref):
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
        raise RuntimeError(
            "Denormalized mode requires at least these tables in the dataset: "
            + ", ".join(missing)
        )

    if reset:
        _delete_prefixes(
            client,
            [
                "retail:customers:",
                "retail:stores:",
                "retail:products:",
                "retail:orders:",
                "retail:inventory:",
            ],
        )

    print("Loading base entities (customers/stores/products)...", flush=True)
    t0 = time.perf_counter()

    customers_by_src = denorm_load_customers(customers_ref, chunksize=batch_size)
    stores_by_src = denorm_load_stores(stores_ref, employees_ref, chunksize=batch_size)
    products_by_src = denorm_load_products(products_ref, categories_ref, suppliers_ref, chunksize=batch_size)

    print(
        f"  - loaded customers={len(customers_by_src):,}, stores={len(stores_by_src):,}, products={len(products_by_src):,} in {time.perf_counter() - t0:.2f}s",
        flush=True,
    )

    print("Writing base documents to Redis...", flush=True)
    t0 = time.perf_counter()
    for doc in customers_by_src.values():
        _set_json(client, f"retail:customers:{doc['_id']}", doc)
    for doc in stores_by_src.values():
        _set_json(client, f"retail:stores:{doc['_id']}", doc)
    for doc in products_by_src.values():
        _set_json(client, f"retail:products:{doc['_id']}", doc)

    print(f"  - base docs written in {time.perf_counter() - t0:.2f}s", flush=True)

    if inventory_ref is not None:
        print("Building + writing inventory documents...", flush=True)
        t0 = time.perf_counter()
        inv_docs = build_inventory_docs(inventory_ref, stores_by_src, products_by_src, chunksize=batch_size)
        for doc in inv_docs:
            _set_json(client, f"retail:inventory:{doc['_id']}", doc)
        print(f"  - inventory written in {time.perf_counter() - t0:.2f}s", flush=True)

    cache_path = default_cache_path("retail_denorm.sqlite")

    print("Building SQLite cache for order_items (+ optional payments/shipments)...", flush=True)
    t0 = time.perf_counter()

    order_items_rows = build_order_items_cache_rows(order_items_ref, products_by_src, chunksize=batch_size)
    payments_rows = (
        build_optional_order_object_cache_rows(payments_ref, chunksize=batch_size)
        if payments_ref is not None
        else None
    )
    shipments_rows = (
        build_optional_order_object_cache_rows(shipments_ref, chunksize=batch_size)
        if shipments_ref is not None
        else None
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

    print(f"Writing denormalized orders to Redis (target {orders_target_rows:,})...", flush=True)
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
                    fetch_optional_by_order_id(cache_path, "payments", base_order_ids)
                    if payments_ref is not None
                    else {}
                )
                shipments_by_order = (
                    fetch_optional_by_order_id(cache_path, "shipments", base_order_ids)
                    if shipments_ref is not None
                    else {}
                )

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
                        "store_id": store_obj["_id"] if isinstance(store_obj, dict) else None,
                        "order_date": row.get(date_col) if date_col else None,
                        "status": row.get(status_col) if status_col else None,
                        "items": items_by_order.get(base_id, []),
                        "payment": payments_by_order.get(base_id, {}),
                        "shipment": shipments_by_order.get(base_id, {}),
                    }

                    _set_json(client, f"retail:orders:{order_doc['_id']}", order_doc)
                    inserted_total += 1

                    if inserted_total % 100_000 == 0:
                        print(f"  - orders inserted: {inserted_total:,}", flush=True)

            pass_idx += 1
    finally:
        if not keep_cache:
            try:
                cache_path.unlink(missing_ok=True)
                print(f"Deleted SQLite cache: {cache_path}", flush=True)
            except Exception:
                pass


def main() -> int:
    ensure_supported_python()
    args = parse_args()

    dataset_id = args.dataset_id or os.getenv("KAGGLE_DATASET_ID")
    print("Downloading dataset via kagglehub...", flush=True)
    dataset_path = download_dataset(dataset_id) if dataset_id else download_dataset()
    print(f"Dataset ready at: {dataset_path}", flush=True)

    print("Discovering CSV tables...", flush=True)
    table_defs = build_table_defs(dataset_path)
    print(f"Discovered {len(table_defs)} tables.", flush=True)

    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )

    try:
        if args.mode == "tables":
            for td in table_defs:
                start = time.perf_counter()
                print(f"Importing {td.name}...", flush=True)
                import_table_redis(client, td, args.batch_size, args.reset)
                print(
                    f"[OK] Imported {td.name} in {time.perf_counter() - start:.2f}s",
                    flush=True,
                )

            orders_def = next((d for d in table_defs if is_orders_table(d.name)), None)
            if orders_def:
                print(f"Expanding orders to {args.orders_target_rows:,} rows...", flush=True)
                start = time.perf_counter()
                expand_orders_redis(client, orders_def, args.orders_target_rows, args.batch_size)
                print(
                    f"[OK] Orders expanded in {time.perf_counter() - start:.2f}s",
                    flush=True,
                )
        else:
            start = time.perf_counter()
            import_denormalized_redis(
                client,
                table_defs,
                args.batch_size,
                args.reset,
                args.orders_target_rows,
            )
            print(
                f"[OK] Denormalized import finished in {time.perf_counter() - start:.2f}s",
                flush=True,
            )
    finally:
        client.close()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
