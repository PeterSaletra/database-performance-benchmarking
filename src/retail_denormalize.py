from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from retail_import_common import normalize_column_name, normalize_table_name


def stable_hex(namespace: str, value: str) -> str:
    h = hashlib.md5(f"{namespace}:{value}".encode("utf-8"), usedforsecurity=False).hexdigest()
    return h


def stable_object_id_hex(namespace: str, value: str) -> str:
    # 24 hex chars for Mongo ObjectId.
    return stable_hex(namespace, value)[:24]


@dataclass(frozen=True)
class TableRef:
    name: str
    csv_path: Path
    primary_key: str
    columns: list[str]


def pick_table(table_refs: dict[str, TableRef], *names: str) -> TableRef | None:
    for n in names:
        key = normalize_table_name(n)
        if key in table_refs:
            return table_refs[key]
    return None


def find_column(columns: list[str], *candidates: str) -> str | None:
    cols = [normalize_column_name(c) for c in columns]
    for cand in candidates:
        c = normalize_column_name(cand)
        if c in cols:
            return c
    return None


def _col_like(columns: list[str], pattern: str) -> list[str]:
    rx = re.compile(pattern)
    out: list[str] = []
    for c in [normalize_column_name(c) for c in columns]:
        if rx.search(c):
            out.append(c)
    return out


def build_address(row: dict[str, Any]) -> dict[str, Any]:
    address: dict[str, Any] = {}

    # Common address fields (best-effort).
    for k in list(row.keys()):
        if k.startswith("address_"):
            address[k.removeprefix("address_")] = row[k]

    for key in (
        "address",
        "street",
        "street_1",
        "street_2",
        "city",
        "state",
        "province",
        "zip",
        "zipcode",
        "postal",
        "postal_code",
        "country",
    ):
        if key in row and row[key] is not None and key not in address:
            address[key] = row[key]

    return address


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except Exception:
        return None


def _sqlite_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_cache_dir(cache_dir: Path) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)


def rebuild_sqlite_cache(
    cache_path: Path,
    order_items_rows: Iterable[tuple[str, str]],
    payments_rows: Iterable[tuple[str, str]] | None = None,
    shipments_rows: Iterable[tuple[str, str]] | None = None,
) -> None:
    if cache_path.exists():
        cache_path.unlink()

    conn = _sqlite_connect(cache_path)
    try:
        def _bulk_insert(
            label: str,
            sql: str,
            rows: Iterable[tuple[str, str]],
            batch_size: int = 50_000,
            progress_every: int = 200_000,
        ) -> int:
            total = 0
            buf: list[tuple[str, str]] = []
            t0 = time.perf_counter()
            for r in rows:
                buf.append(r)
                if len(buf) >= batch_size:
                    conn.executemany(sql, buf)
                    conn.commit()
                    total += len(buf)
                    buf.clear()
                    if total == batch_size or (progress_every > 0 and total % progress_every == 0):
                        print(
                            f"  - cache {label}: {total:,} rows inserted in {time.perf_counter() - t0:.1f}s",
                            flush=True,
                        )
            if buf:
                conn.executemany(sql, buf)
                conn.commit()
                total += len(buf)
                buf.clear()
            print(
                f"  - cache {label}: {total:,} rows inserted in {time.perf_counter() - t0:.1f}s",
                flush=True,
            )
            return total

        conn.execute("CREATE TABLE order_items (order_id TEXT, item_json TEXT);")
        _bulk_insert(
            "order_items",
            "INSERT INTO order_items(order_id, item_json) VALUES (?, ?);",
            order_items_rows,
        )
        conn.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id);")

        if payments_rows is not None:
            conn.execute("CREATE TABLE payments (order_id TEXT PRIMARY KEY, json TEXT);")
            _bulk_insert(
                "payments",
                "INSERT OR REPLACE INTO payments(order_id, json) VALUES (?, ?);",
                payments_rows,
                batch_size=20_000,
                progress_every=100_000,
            )

        if shipments_rows is not None:
            conn.execute("CREATE TABLE shipments (order_id TEXT PRIMARY KEY, json TEXT);")
            _bulk_insert(
                "shipments",
                "INSERT OR REPLACE INTO shipments(order_id, json) VALUES (?, ?);",
                shipments_rows,
                batch_size=20_000,
                progress_every=100_000,
            )
    finally:
        conn.close()


def fetch_items_for_orders(cache_path: Path, order_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not order_ids:
        return {}

    conn = _sqlite_connect(cache_path)
    try:
        out: dict[str, list[dict[str, Any]]] = {oid: [] for oid in order_ids}

        # SQLite has a limit on query variables; chunk it.
        chunk_size = 800
        for i in range(0, len(order_ids), chunk_size):
            sub = order_ids[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(sub))
            cur = conn.execute(
                f"SELECT order_id, item_json FROM order_items WHERE order_id IN ({placeholders});",
                sub,
            )
            for order_id, item_json in cur.fetchall():
                try:
                    item = json.loads(item_json)
                except Exception:
                    continue
                out.setdefault(order_id, []).append(item)

        return out
    finally:
        conn.close()


def fetch_optional_by_order_id(cache_path: Path, table: str, order_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not order_ids:
        return {}

    conn = _sqlite_connect(cache_path)
    try:
        out: dict[str, dict[str, Any]] = {}
        chunk_size = 800
        for i in range(0, len(order_ids), chunk_size):
            sub = order_ids[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(sub))
            cur = conn.execute(
                f"SELECT order_id, json FROM {table} WHERE order_id IN ({placeholders});",
                sub,
            )
            for order_id, raw in cur.fetchall():
                try:
                    out[order_id] = json.loads(raw)
                except Exception:
                    continue
        return out
    finally:
        conn.close()


def normalize_row_dict(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in row.items():
        kn = normalize_column_name(k)
        if v is None:
            continue
        out[kn] = v
    return out


def maybe_pick_id(row: dict[str, Any], primary_key: str) -> str | None:
    pk = normalize_column_name(primary_key)
    if pk in row and row[pk] is not None:
        return str(row[pk])
    return None


def iter_csv_rows(csv_path: Path, chunksize: int) -> Iterable[list[dict[str, Any]]]:
    import pandas as pd

    for chunk in pd.read_csv(csv_path, chunksize=chunksize, dtype=str):
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        chunk = chunk.where(pd.notna(chunk), None)
        yield [normalize_row_dict(r) for r in chunk.to_dict(orient="records")]


def compute_csv_max_int(csv_path: Path, column: str, chunksize: int) -> int | None:
    import pandas as pd

    max_value: int | None = None
    col = normalize_column_name(column)
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, dtype=str):
        chunk = chunk.rename(columns={c: normalize_column_name(c) for c in chunk.columns})
        if col not in chunk.columns:
            return None
        series = pd.to_numeric(chunk[col], errors="coerce")
        v = series.max(skipna=True)
        if pd.isna(v):
            continue
        v_int = int(v)
        max_value = v_int if max_value is None else max(max_value, v_int)
    return max_value


def build_table_ref_map(table_defs: list[Any]) -> dict[str, TableRef]:
    out: dict[str, TableRef] = {}
    for td in table_defs:
        out[normalize_table_name(td.name)] = TableRef(
            name=normalize_table_name(td.name),
            csv_path=Path(td.csv_path),
            primary_key=normalize_column_name(td.primary_key),
            columns=[normalize_column_name(c) for c in td.columns],
        )
    return out


def infer_fk_column(columns: list[str], *endswith_candidates: str) -> str | None:
    cols = [normalize_column_name(c) for c in columns]
    for cand in endswith_candidates:
        c = normalize_column_name(cand)
        if c in cols:
            return c
    for c in cols:
        for suf in endswith_candidates:
            if c.endswith(normalize_column_name(suf)):
                return c
    return None


def denorm_load_customers(customers: TableRef, chunksize: int) -> dict[str, dict[str, Any]]:
    id_col = normalize_column_name(customers.primary_key)
    fn_col = find_column(customers.columns, "first_name", "firstname", "fname")
    ln_col = find_column(customers.columns, "last_name", "lastname", "lname")
    name_col = find_column(customers.columns, "name", "full_name", "customer_name")

    out: dict[str, dict[str, Any]] = {}
    for rows in iter_csv_rows(customers.csv_path, chunksize):
        for row in rows:
            source_id = row.get(id_col)
            if source_id is None:
                continue
            sid = str(source_id)
            oid = stable_object_id_hex("customers", sid)

            first = row.get(fn_col) if fn_col else None
            last = row.get(ln_col) if ln_col else None
            if (first is None or last is None) and name_col and row.get(name_col):
                parts = str(row[name_col]).split()
                if first is None and parts:
                    first = parts[0]
                if last is None and len(parts) > 1:
                    last = " ".join(parts[1:])

            doc = {
                "_id": oid,
                "first_name": first,
                "last_name": last,
                "address": build_address(row),
            }
            # drop None fields
            doc = {k: v for k, v in doc.items() if v not in (None, {}, [])}
            out[sid] = doc
    return out


def denorm_load_stores(
    stores: TableRef,
    employees: TableRef | None,
    chunksize: int,
) -> dict[str, dict[str, Any]]:
    id_col = normalize_column_name(stores.primary_key)
    name_col = find_column(stores.columns, "store_name", "name")
    out: dict[str, dict[str, Any]] = {}

    # Preload employees per store.
    employees_by_store: dict[str, list[dict[str, Any]]] = {}
    if employees is not None:
        store_fk = infer_fk_column(employees.columns, "store_id")
        for rows in iter_csv_rows(employees.csv_path, chunksize):
            for row in rows:
                if not store_fk or row.get(store_fk) is None:
                    continue
                sid = str(row[store_fk])
                e = {k: v for k, v in row.items() if k != store_fk and v is not None}
                if e:
                    employees_by_store.setdefault(sid, []).append(e)

    for rows in iter_csv_rows(stores.csv_path, chunksize):
        for row in rows:
            source_id = row.get(id_col)
            if source_id is None:
                continue
            sid = str(source_id)
            oid = stable_object_id_hex("stores", sid)
            doc = {
                "_id": oid,
                "store_name": row.get(name_col) if name_col else None,
                "address": build_address(row),
                "employees": employees_by_store.get(sid, []),
            }
            doc = {k: v for k, v in doc.items() if v not in (None, {}, [])}
            out[sid] = doc

    return out


def denorm_load_products(
    products: TableRef,
    categories: TableRef | None,
    suppliers: TableRef | None,
    chunksize: int,
) -> dict[str, dict[str, Any]]:
    id_col = normalize_column_name(products.primary_key)
    name_col = find_column(products.columns, "product_name", "name")
    price_col = find_column(products.columns, "price", "unit_price")

    categories_map: dict[str, dict[str, Any]] = {}
    if categories is not None:
        cid = normalize_column_name(categories.primary_key)
        cname = find_column(categories.columns, "category_name", "name")
        for rows in iter_csv_rows(categories.csv_path, chunksize):
            for row in rows:
                if row.get(cid) is None:
                    continue
                categories_map[str(row[cid])] = {
                    "_id": stable_object_id_hex("categories", str(row[cid])),
                    "name": row.get(cname) if cname else None,
                }

    suppliers_map: dict[str, dict[str, Any]] = {}
    if suppliers is not None:
        sid = normalize_column_name(suppliers.primary_key)
        sname = find_column(suppliers.columns, "supplier_name", "name")
        for rows in iter_csv_rows(suppliers.csv_path, chunksize):
            for row in rows:
                if row.get(sid) is None:
                    continue
                suppliers_map[str(row[sid])] = {
                    "_id": stable_object_id_hex("suppliers", str(row[sid])),
                    "name": row.get(sname) if sname else None,
                }

    category_fk = infer_fk_column(products.columns, "category_id")
    supplier_fk = infer_fk_column(products.columns, "supplier_id")

    out: dict[str, dict[str, Any]] = {}
    for rows in iter_csv_rows(products.csv_path, chunksize):
        for row in rows:
            source_id = row.get(id_col)
            if source_id is None:
                continue
            sid = str(source_id)
            oid = stable_object_id_hex("products", sid)

            category_obj = None
            if category_fk and row.get(category_fk) is not None:
                category_obj = categories_map.get(str(row[category_fk]))

            supplier_obj = None
            if supplier_fk and row.get(supplier_fk) is not None:
                supplier_obj = suppliers_map.get(str(row[supplier_fk]))

            price = _safe_float(row.get(price_col)) if price_col else None

            doc = {
                "_id": oid,
                "product_name": row.get(name_col) if name_col else None,
                "category": category_obj,
                "supplier": supplier_obj,
                "price": price if price is not None else row.get(price_col) if price_col else None,
            }
            doc = {k: v for k, v in doc.items() if v not in (None, {}, [])}
            out[sid] = doc

    return out


def build_inventory_docs(
    inventory: TableRef,
    stores_by_source_id: dict[str, dict[str, Any]],
    products_by_source_id: dict[str, dict[str, Any]],
    chunksize: int,
) -> list[dict[str, Any]]:
    store_fk = infer_fk_column(inventory.columns, "store_id")
    product_fk = infer_fk_column(inventory.columns, "product_id")
    qty_col = find_column(inventory.columns, "quantity", "qty", "stock")

    by_store: dict[str, list[dict[str, Any]]] = {}
    for rows in iter_csv_rows(inventory.csv_path, chunksize):
        for row in rows:
            if not store_fk or not product_fk:
                continue
            if row.get(store_fk) is None or row.get(product_fk) is None:
                continue
            store_sid = str(row[store_fk])
            product_sid = str(row[product_fk])

            prod = products_by_source_id.get(product_sid)
            if prod is None:
                continue
            qty = _safe_float(row.get(qty_col)) if qty_col else None

            by_store.setdefault(store_sid, []).append(
                {
                    "product": prod,
                    "quantity": int(qty) if qty is not None and qty.is_integer() else qty,
                }
            )

    docs: list[dict[str, Any]] = []
    for store_sid, products in by_store.items():
        store_doc = stores_by_source_id.get(store_sid)
        if store_doc is None:
            continue
        docs.append(
            {
                "_id": stable_object_id_hex("inventory", store_sid),
                "store_id": store_doc["_id"],
                "products": [p for p in products if p.get("product") is not None],
            }
        )
    return docs


def build_order_items_cache_rows(
    order_items: TableRef,
    products_by_source_id: dict[str, dict[str, Any]],
    chunksize: int,
) -> Iterable[tuple[str, str]]:
    order_fk = infer_fk_column(order_items.columns, "order_id")
    product_fk = infer_fk_column(order_items.columns, "product_id")
    qty_col = find_column(order_items.columns, "quantity", "qty")
    unit_price_col = find_column(order_items.columns, "unit_price", "price")

    if not order_fk or not product_fk:
        return []

    def _iter() -> Iterable[tuple[str, str]]:
        for rows in iter_csv_rows(order_items.csv_path, chunksize):
            for row in rows:
                if row.get(order_fk) is None or row.get(product_fk) is None:
                    continue
                order_id = str(row[order_fk])
                product_sid = str(row[product_fk])
                product_doc = products_by_source_id.get(product_sid)

                qty = _safe_float(row.get(qty_col)) if qty_col else None
                unit_price = _safe_float(row.get(unit_price_col)) if unit_price_col else None

                item = {
                    "product": product_doc if product_doc is not None else {"_id": stable_object_id_hex("products", product_sid)},
                    "quantity": int(qty) if qty is not None and qty.is_integer() else qty,
                    "unit_price": unit_price if unit_price is not None else row.get(unit_price_col) if unit_price_col else None,
                }
                item = {k: v for k, v in item.items() if v not in (None, {}, [])}
                yield (order_id, json.dumps(item, ensure_ascii=False))

    return _iter()


def build_optional_order_object_cache_rows(
    table: TableRef,
    chunksize: int,
) -> Iterable[tuple[str, str]]:
    order_fk = infer_fk_column(table.columns, "order_id")
    if not order_fk:
        return []

    def _iter() -> Iterable[tuple[str, str]]:
        for rows in iter_csv_rows(table.csv_path, chunksize):
            for row in rows:
                if row.get(order_fk) is None:
                    continue
                order_id = str(row[order_fk])
                obj = {k: v for k, v in row.items() if k != order_fk and v is not None}
                yield (order_id, json.dumps(obj, ensure_ascii=False))

    return _iter()

