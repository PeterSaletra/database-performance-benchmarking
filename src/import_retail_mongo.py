from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

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


def _row_to_doc(row: tuple[object, ...], cols: list[str]) -> dict:
    doc: dict = {}
    for i, col in enumerate(cols):
        val = row[i]
        if val is None:
            continue
        doc[col] = val
    return doc


def import_table_mongo(collection, table_def, batch_size: int, reset: bool) -> None:
    import pandas as pd

    if reset:
        collection.drop()

    pk = normalize_column_name(table_def.primary_key)

    next_row_id = 1
    for chunk in pd.read_csv(table_def.csv_path, chunksize=batch_size, dtype=str):
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        chunk = chunk.where(pd.notna(chunk), None)
        cols = [normalize_column_name(c) for c in chunk.columns.tolist()]

        docs: list[dict] = []
        for row in chunk.itertuples(index=False, name=None):
            doc = _row_to_doc(row, cols)
            if pk != "__row_id" and pk in doc:
                doc["_id"] = doc[pk]
            else:
                doc["_id"] = next_row_id
                next_row_id += 1
            docs.append(doc)

        if not docs:
            continue

        try:
            collection.insert_many(docs, ordered=False)
        except BulkWriteError:
            pass


def expand_orders_mongo(db, orders_def, target_rows: int, batch_size: int) -> None:
    import pandas as pd

    collection = db["orders"]
    current = collection.estimated_document_count()
    if current >= target_rows:
        return

    pk = normalize_column_name(orders_def.primary_key)
    max_pk_int = None
    if pk != "__row_id":
        max_pk_int = _compute_csv_max_int(orders_def.csv_path, pk, chunksize=batch_size)

    pass_idx = 1
    while current < target_rows:
        offset = (max_pk_int or 0) * pass_idx
        inserted_this_pass = 0

        for chunk in pd.read_csv(orders_def.csv_path, chunksize=batch_size, dtype=str):
            chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
            chunk = chunk.where(pd.notna(chunk), None)
            cols = chunk.columns.tolist()

            docs: list[dict] = []
            for row in chunk.itertuples(index=False, name=None):
                if current + inserted_this_pass >= target_rows:
                    break

                doc = _row_to_doc(row, cols)
                if pk != "__row_id" and pk in doc:
                    old = doc.get(pk)
                    old_int = _try_int(old)
                    if old_int is not None and max_pk_int is not None:
                        new_id = old_int + offset
                    else:
                        new_id = f"{old}_{pass_idx}_{inserted_this_pass}"
                    doc[pk] = new_id
                    doc["_id"] = new_id
                else:
                    doc["_id"] = int(current + inserted_this_pass + 1)
                docs.append(doc)
                inserted_this_pass += 1

            if docs:
                try:
                    collection.insert_many(docs, ordered=False)
                except BulkWriteError:
                    pass

            if current + inserted_this_pass >= target_rows:
                break

        current += inserted_this_pass
        pass_idx += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import the Kaggle Retail Data Warehouse dataset into MongoDB."
    )
    parser.add_argument(
        "--mode",
        choices=["tables", "denormalized"],
        default=os.getenv("NOSQL_MODE", "denormalized"),
        help="Import mode: 'tables' (1 CSV -> 1 collection) or 'denormalized' (ERD-like documents).",
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("KAGGLE_DATASET_ID", None) or None,
        help="Kaggle dataset id (default: retail DWH 12-table dataset).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop collections before import.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("IMPORT_BATCH_SIZE", "10000")),
        help="CSV chunk size / insert batch size (default: 10000).",
    )
    parser.add_argument(
        "--orders-target-rows",
        type=int,
        default=int(os.getenv("ORDERS_TARGET_ROWS", str(DEFAULT_ORDERS_TARGET_ROWS))),
        help=f"Target number of rows in orders collection (default: {DEFAULT_ORDERS_TARGET_ROWS}).",
    )
    return parser.parse_args()


_OID_CACHE: dict[str, ObjectId] = {}


def _to_oid(hex24: str) -> ObjectId:
    cached = _OID_CACHE.get(hex24)
    if cached is not None:
        return cached
    oid = ObjectId(hex24)
    _OID_CACHE[hex24] = oid
    return oid


def _convert_product_in_item(item: dict) -> dict:
    product = item.get("product")
    if isinstance(product, dict) and isinstance(product.get("_id"), str) and len(product["_id"]) == 24:
        product = {**product, "_id": _to_oid(product["_id"])}

        cat = product.get("category")
        if isinstance(cat, dict) and isinstance(cat.get("_id"), str) and len(cat["_id"]) == 24:
            product["category"] = {**cat, "_id": _to_oid(cat["_id"])}

        sup = product.get("supplier")
        if isinstance(sup, dict) and isinstance(sup.get("_id"), str) and len(sup["_id"]) == 24:
            product["supplier"] = {**sup, "_id": _to_oid(sup["_id"])}

        item = {**item, "product": product}
    return item


def import_denormalized_mongo(db, table_defs, batch_size: int, reset: bool, orders_target_rows: int) -> None:
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
        db["customers"].drop()
        db["stores"].drop()
        db["products"].drop()
        db["orders"].drop()
        db["inventory"].drop()

    print("Loading base entities (customers/stores/products)...", flush=True)
    t0 = time.perf_counter()

    customers_by_src = denorm_load_customers(customers_ref, chunksize=batch_size)
    stores_by_src = denorm_load_stores(stores_ref, employees_ref, chunksize=batch_size)
    products_by_src = denorm_load_products(products_ref, categories_ref, suppliers_ref, chunksize=batch_size)

    print(
        f"  - loaded customers={len(customers_by_src):,}, stores={len(stores_by_src):,}, products={len(products_by_src):,} in {time.perf_counter() - t0:.2f}s",
        flush=True,
    )

    def _insert_many(coll_name: str, docs: list[dict]) -> None:
        if not docs:
            return
        try:
            db[coll_name].insert_many(docs, ordered=False)
        except BulkWriteError:
            pass

    print("Inserting base collections into MongoDB...", flush=True)
    t0 = time.perf_counter()

    _insert_many(
        "customers",
        [{**d, "_id": _to_oid(d["_id"])} for d in customers_by_src.values()],
    )
    _insert_many(
        "stores",
        [{**d, "_id": _to_oid(d["_id"])} for d in stores_by_src.values()],
    )
    _insert_many(
        "products",
        [
            {
                **d,
                "_id": _to_oid(d["_id"]),
                **(
                    {"category": {**d["category"], "_id": _to_oid(d["category"]["_id"])}}
                    if isinstance(d.get("category"), dict) and isinstance(d["category"].get("_id"), str)
                    else {}
                ),
                **(
                    {"supplier": {**d["supplier"], "_id": _to_oid(d["supplier"]["_id"])}}
                    if isinstance(d.get("supplier"), dict) and isinstance(d["supplier"].get("_id"), str)
                    else {}
                ),
            }
            for d in products_by_src.values()
        ],
    )

    print(f"  - base collections inserted in {time.perf_counter() - t0:.2f}s", flush=True)

    if inventory_ref is not None:
        print("Building + inserting inventory documents...", flush=True)
        t0 = time.perf_counter()
        inv_docs = build_inventory_docs(inventory_ref, stores_by_src, products_by_src, chunksize=batch_size)
        inv_docs_m = []
        for d in inv_docs:
            inv_docs_m.append(
                {
                    **d,
                    "_id": _to_oid(d["_id"]),
                    "store_id": _to_oid(d["store_id"]),
                    "products": [
                        {
                            **p,
                            "product": _convert_product_in_item({"product": p["product"]})["product"],
                        }
                        for p in d.get("products", [])
                        if isinstance(p.get("product"), dict) and isinstance(p["product"].get("_id"), str)
                    ],
                }
            )
        _insert_many("inventory", inv_docs_m)
        print(
            f"  - inventory inserted: {len(inv_docs_m):,} docs in {time.perf_counter() - t0:.2f}s",
            flush=True,
        )

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

    print(f"Importing denormalized orders (target {orders_target_rows:,})...", flush=True)
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
                base_rows: list[dict[str, Any]] = []
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

                docs: list[dict[str, Any]] = []
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

                    items = [
                        _convert_product_in_item(i)
                        for i in items_by_order.get(base_id, [])
                        if isinstance(i, dict)
                    ]

                    payment_obj = payments_by_order.get(base_id, {})
                    shipment_obj = shipments_by_order.get(base_id, {})

                    doc = {
                        "_id": _to_oid(stable_object_id_hex("orders", new_id)),
                        "customer": (
                            {**customer_obj, "_id": _to_oid(customer_obj["_id"])}
                            if isinstance(customer_obj, dict) and "_id" in customer_obj
                            else None
                        ),
                        "store_id": _to_oid(store_obj["_id"]) if isinstance(store_obj, dict) else None,
                        "order_date": row.get(date_col) if date_col else None,
                        "status": row.get(status_col) if status_col else None,
                        "items": items,
                        "payment": payment_obj if payment_obj else {},
                        "shipment": shipment_obj if shipment_obj else {},
                    }
                    doc = {k: v for k, v in doc.items() if v not in (None, {}, [])}
                    docs.append(doc)

                if docs:
                    t_ins = time.perf_counter()
                    try:
                        db["orders"].insert_many(docs, ordered=False)
                    except BulkWriteError:
                        pass
                    inserted_total += len(docs)

                    if inserted_total == len(docs) or inserted_total % 100_000 == 0:
                        print(
                            f"  - orders inserted: {inserted_total:,}/{orders_target_rows:,} (+{len(docs):,} in {time.perf_counter() - t_ins:.2f}s)",
                            flush=True,
                        )

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

    dataset_id = args.dataset_id or os.getenv("KAGGLE_DATASET_ID")
    print("Downloading dataset via kagglehub...", flush=True)
    dataset_path = download_dataset(dataset_id) if dataset_id else download_dataset()
    print(f"Dataset ready at: {dataset_path}", flush=True)

    print("Discovering CSV tables...", flush=True)
    table_defs = build_table_defs(dataset_path)
    print(f"Discovered {len(table_defs)} tables.", flush=True)

    user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "benchmark_user")
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "benchmark_pass")
    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", "27017"))
    db_name = os.getenv("MONGO_DB", "benchmark_db")

    client = MongoClient(
        f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin",
        serverSelectionTimeoutMS=5000,
    )

    try:
        db = client[db_name]

        if args.mode == "tables":
            for td in table_defs:
                start = time.perf_counter()
                import_table_mongo(db[td.name], td, args.batch_size, args.reset)
                print(f"[OK] Imported {td.name} in {time.perf_counter() - start:.2f}s")

            orders_def = next((d for d in table_defs if is_orders_table(d.name)), None)
            if orders_def:
                print(f"Expanding orders to {args.orders_target_rows:,} docs...")
                start = time.perf_counter()
                expand_orders_mongo(db, orders_def, args.orders_target_rows, args.batch_size)
                print(f"[OK] Orders expanded in {time.perf_counter() - start:.2f}s")
        else:
            start = time.perf_counter()
            import_denormalized_mongo(db, table_defs, args.batch_size, args.reset, args.orders_target_rows)
            print(f"[OK] Denormalized import finished in {time.perf_counter() - start:.2f}s")

    finally:
        client.close()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
