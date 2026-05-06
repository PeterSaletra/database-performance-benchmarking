from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import itertools
import json
import os
import random
import statistics
import string
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import mysql.connector
import psycopg
from bson import ObjectId
from pymongo import MongoClient


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ts_ms() -> int:
    return int(_utc_now().timestamp() * 1000)


def _random_payload(size: int = 64) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(size))


def _stable_object_id_hex(namespace: str, value: str) -> str:
    h = hashlib.md5(f"{namespace}:{value}".encode("utf-8"), usedforsecurity=False).hexdigest()
    return h[:24]


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Scenario:
    scenario_id: str
    operation: str
    description: str
    executor: Callable[["DBClient", dict[str, Any]], int]


@dataclass
class TrialResult:
    run_id: str
    db_engine: str
    scenario_id: str
    operation: str
    trial_no: int
    latency_ms: float
    rows_affected: int
    data_size_label: str
    mode: str
    timestamp_utc: str


class DBClient:
    name: str

    def setup(self, seed_rows: int) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def create_single(self, payload: str) -> tuple[str, int]:
        raise NotImplementedError

    def create_bulk(self, payloads: list[str]) -> int:
        raise NotImplementedError

    def read_by_id(self, row_id: str) -> int:
        raise NotImplementedError

    def read_latest(self, limit: int) -> int:
        raise NotImplementedError

    def read_contains(self, needle: str, limit: int) -> int:
        raise NotImplementedError

    def update_by_id(self, row_id: str, payload: str) -> int:
        raise NotImplementedError

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        raise NotImplementedError

    def update_contains(self, needle: str, suffix: str) -> int:
        raise NotImplementedError

    def delete_by_id(self, row_id: str) -> int:
        raise NotImplementedError

    def delete_latest(self, limit: int) -> int:
        raise NotImplementedError

    def delete_contains(self, needle: str, limit: int) -> int:
        raise NotImplementedError

    def configure_mode(self, mode: str) -> None:
        # Default no-op for engines that don't manage benchmark indexes.
        _ = mode

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            # queryPlanner is stable and compact enough for report artifacts.
            r2 = (
                self.collection.find({}, {"_id": 1})
                .sort("updated_at", -1)
                .limit(10)
                .explain(verbosity="queryPlanner")
            )
            out["R2_latest"] = json.dumps(r2, ensure_ascii=False, indent=2)
        except TypeError:
            # Older pymongo versions may not support verbosity kwarg.
            try:
                r2 = (
                    self.collection.find({}, {"_id": 1})
                    .sort("updated_at", -1)
                    .limit(10)
                    .explain()
                )
                out["R2_latest"] = json.dumps(r2, ensure_ascii=False, indent=2)
            except Exception:
                pass
        except Exception:
            pass

        try:
            r3 = (
                self.collection.find(
                    {"payload": {"$regex": "^seed_payload"}},
                    {"_id": 1},
                )
                .limit(20)
                .explain(verbosity="queryPlanner")
            )
            out["R3_prefix"] = json.dumps(r3, ensure_ascii=False, indent=2)
        except TypeError:
            try:
                r3 = (
                    self.collection.find(
                        {"payload": {"$regex": "^seed_payload"}},
                        {"_id": 1},
                    )
                    .limit(20)
                    .explain()
                )
                out["R3_prefix"] = json.dumps(r3, ensure_ascii=False, indent=2)
            except Exception:
                pass
        except Exception:
            pass

        return out


class PostgresClient(DBClient):
    name = "postgres"

    def __init__(self) -> None:
        self.conn = psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "benchmark_db"),
            user=os.getenv("POSTGRES_USER", "benchmark_user"),
            password=os.getenv("POSTGRES_PASSWORD", "benchmark_pass"),
        )
        self.conn.autocommit = False

        self._pk_col: str | None = None
        self._pk_is_numeric: bool = True
        self._date_col: str | None = None
        self._status_col: str | None = None
        self._next_id: int = 9_000_000_000_000
        self.seed_ids: list[str] = []

    def setup(self, seed_rows: int) -> None:
        seed_rows = max(int(seed_rows or 0), 5)

        with self.conn.cursor() as cur:
            try:
                cur.execute('SELECT 1 FROM "orders" LIMIT 1;')
            except Exception as exc:
                raise RuntimeError(
                    "Postgres: missing table 'orders'. Run import first (python src/import_data.py --reset)."
                ) from exc

            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = current_schema()
                  AND tc.table_name = 'orders'
                ORDER BY kcu.ordinal_position
                LIMIT 1
                """
            )
            pk_row = cur.fetchone()
            self._pk_col = str(pk_row[0]) if pk_row else "order_id"

            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'orders'
                ORDER BY ordinal_position
                """
            )
            cols = [(str(r[0]), str(r[1]).lower()) for r in cur.fetchall()]
            col_names = [c for c, _ in cols]

            pk_type = next((t for c, t in cols if c == self._pk_col), "bigint")
            self._pk_is_numeric = pk_type in {"bigint", "integer", "smallint", "numeric", "decimal"}

            self._date_col = "order_date" if "order_date" in col_names else next(
                (c for c in col_names if any(tok in c for tok in ("date", "time", "created", "updated"))),
                None,
            )
            self._status_col = (
                "status"
                if "status" in col_names
                else "order_status"
                if "order_status" in col_names
                else next((c for c, t in cols if "text" in t and c != self._pk_col), None)
            )

            if not self._status_col:
                raise RuntimeError("Postgres: could not infer a status/text column in orders.")

            if self._pk_is_numeric:
                cur.execute(f'SELECT COALESCE(MAX("{self._pk_col}"), 0) FROM "orders";')
                max_id = int(cur.fetchone()[0] or 0)
                self._next_id = max(self._next_id, max_id + 1)

            cur.execute(
                f'SELECT COUNT(*) FROM "orders" WHERE "{self._status_col}" LIKE %s;',
                ("seed_status%",),
            )
            current = int(cur.fetchone()[0] or 0)
            missing = max(0, seed_rows - current)

            if missing:
                now = _utc_now()
                if self._pk_is_numeric:
                    ids: list[Any] = [self._next_id + i for i in range(missing)]
                    self._next_id += missing
                else:
                    ids = [f"seed_{_ts_ms()}_{i}" for i in range(missing)]

                if self._date_col:
                    cur.executemany(
                        f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}", "{self._date_col}") VALUES (%s, %s, %s) ON CONFLICT ("{self._pk_col}") DO NOTHING;',
                        [(rid, f"seed_status_{i}", now) for i, rid in enumerate(ids)],
                    )
                else:
                    cur.executemany(
                        f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}") VALUES (%s, %s) ON CONFLICT ("{self._pk_col}") DO NOTHING;',
                        [(rid, f"seed_status_{i}") for i, rid in enumerate(ids)],
                    )

            cur.execute(
                f'SELECT "{self._pk_col}" FROM "orders" WHERE "{self._status_col}" LIKE %s ORDER BY "{self._pk_col}" LIMIT %s;',
                ("seed_status%", seed_rows),
            )
            self.seed_ids = [str(r[0]) for r in cur.fetchall()]

        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def create_single(self, payload: str) -> tuple[str, int]:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")

        now = _utc_now()
        if self._pk_is_numeric:
            rid: Any = self._next_id
            self._next_id += 1
        else:
            rid = f"c_{_ts_ms()}_{next(_ROW_SEQ)}"

        with self.conn.cursor() as cur:
            if self._date_col:
                cur.execute(
                    f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}", "{self._date_col}") VALUES (%s, %s, %s)',
                    (rid, payload, now),
                )
            else:
                cur.execute(
                    f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}") VALUES (%s, %s)',
                    (rid, payload),
                )
        self.conn.commit()
        return str(rid), 1

    def create_bulk(self, payloads: list[str]) -> int:
        if not payloads:
            return 0
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")

        now = _utc_now()
        if self._pk_is_numeric:
            ids: list[Any] = [self._next_id + i for i in range(len(payloads))]
            self._next_id += len(payloads)
        else:
            ids = [f"cb_{_ts_ms()}_{i}_{next(_ROW_SEQ)}" for i in range(len(payloads))]

        with self.conn.cursor() as cur:
            if self._date_col:
                cur.executemany(
                    f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}", "{self._date_col}") VALUES (%s, %s, %s)',
                    [(ids[i], payloads[i], now) for i in range(len(payloads))],
                )
            else:
                cur.executemany(
                    f'INSERT INTO "orders" ("{self._pk_col}", "{self._status_col}") VALUES (%s, %s)',
                    [(ids[i], payloads[i]) for i in range(len(payloads))],
                )
        self.conn.commit()
        return len(payloads)

    def read_by_id(self, row_id: str) -> int:
        if not self._pk_col:
            raise RuntimeError("Postgres client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT "{self._pk_col}" FROM "orders" WHERE "{self._pk_col}" = %s', (key,))
            return 1 if cur.fetchone() else 0

    def read_latest(self, limit: int) -> int:
        if not self._pk_col:
            raise RuntimeError("Postgres client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT "{self._pk_col}" FROM "orders" ORDER BY "{order_col}" DESC LIMIT %s', (limit,))
            return len(cur.fetchall())

    def read_contains(self, needle: str, limit: int) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")
        with self.conn.cursor() as cur:
            cur.execute(
                f'SELECT "{self._pk_col}" FROM "orders" WHERE "{self._status_col}" LIKE %s LIMIT %s',
                (f"{needle}%", limit),
            )
            return len(cur.fetchall())

    def update_by_id(self, row_id: str, payload: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        now = _utc_now()
        with self.conn.cursor() as cur:
            if self._date_col:
                cur.execute(
                    f'UPDATE "orders" SET "{self._status_col}" = %s, "{self._date_col}" = %s WHERE "{self._pk_col}" = %s',
                    (payload, now, key),
                )
            else:
                cur.execute(
                    f'UPDATE "orders" SET "{self._status_col}" = %s WHERE "{self._pk_col}" = %s',
                    (payload, key),
                )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        now = _utc_now()
        with self.conn.cursor() as cur:
            if self._date_col:
                cur.execute(
                    f"""
                    WITH ids AS (
                        SELECT \"{self._pk_col}\" AS id FROM \"orders\" ORDER BY \"{order_col}\" DESC LIMIT %s
                    )
                    UPDATE \"orders\" o
                    SET \"{self._status_col}\" = COALESCE(o.\"{self._status_col}\", '') || %s,
                        \"{self._date_col}\" = %s
                    FROM ids
                    WHERE o.\"{self._pk_col}\" = ids.id
                    """,
                    (limit, suffix, now),
                )
            else:
                cur.execute(
                    f"""
                    WITH ids AS (
                        SELECT \"{self._pk_col}\" AS id FROM \"orders\" ORDER BY \"{order_col}\" DESC LIMIT %s
                    )
                    UPDATE \"orders\" o
                    SET \"{self._status_col}\" = COALESCE(o.\"{self._status_col}\", '') || %s
                    FROM ids
                    WHERE o.\"{self._pk_col}\" = ids.id
                    """,
                    (limit, suffix),
                )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def update_contains(self, needle: str, suffix: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")
        now = _utc_now()
        with self.conn.cursor() as cur:
            if self._date_col:
                cur.execute(
                    f'UPDATE "orders" SET "{self._status_col}" = COALESCE("{self._status_col}", \'\') || %s, "{self._date_col}" = %s WHERE "{self._status_col}" LIKE %s',
                    (suffix, now, f"{needle}%"),
                )
            else:
                cur.execute(
                    f'UPDATE "orders" SET "{self._status_col}" = COALESCE("{self._status_col}", \'\') || %s WHERE "{self._status_col}" LIKE %s',
                    (suffix, f"{needle}%"),
                )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_by_id(self, row_id: str) -> int:
        if not self._pk_col:
            raise RuntimeError("Postgres client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        with self.conn.cursor() as cur:
            cur.execute(f'DELETE FROM "orders" WHERE "{self._pk_col}" = %s', (key,))
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_latest(self, limit: int) -> int:
        if not self._pk_col:
            raise RuntimeError("Postgres client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM \"orders\"
                WHERE \"{self._pk_col}\" IN (
                    SELECT \"{self._pk_col}\" FROM \"orders\" ORDER BY \"{order_col}\" DESC LIMIT %s
                )
                """,
                (limit,),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_contains(self, needle: str, limit: int) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("Postgres client not initialized; call setup() first")
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM \"orders\"
                WHERE \"{self._pk_col}\" IN (
                    SELECT \"{self._pk_col}\" FROM \"orders\"
                    WHERE \"{self._status_col}\" LIKE %s
                    LIMIT %s
                )
                """,
                (f"{needle}%", limit),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def configure_mode(self, mode: str) -> None:
        if not self._status_col:
            return
        with self.conn.cursor() as cur:
            if mode == "after-index":
                if self._date_col:
                    cur.execute(
                        f'CREATE INDEX IF NOT EXISTS idx_orders_order_date_desc ON "orders" ("{self._date_col}" DESC);'
                    )
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS idx_orders_status_prefix ON "orders" ("{self._status_col}" text_pattern_ops);'
                )
            else:
                cur.execute('DROP INDEX IF EXISTS idx_orders_order_date_desc;')
                cur.execute('DROP INDEX IF EXISTS idx_orders_status_prefix;')
        self.conn.commit()

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if not (self._pk_col and self._status_col):
            return out
        order_col = self._date_col or self._pk_col
        with self.conn.cursor() as cur:
            cur.execute(
                f'EXPLAIN SELECT "{self._pk_col}" FROM "orders" ORDER BY "{order_col}" DESC LIMIT 10;'
            )
            out["R2_latest"] = "\n".join(str(r[0]) for r in cur.fetchall())

            cur.execute(
                f'EXPLAIN SELECT "{self._pk_col}" FROM "orders" WHERE "{self._status_col}" LIKE %s LIMIT 20;',
                ("seed_status%",),
            )
            out["R3_prefix"] = "\n".join(str(r[0]) for r in cur.fetchall())
        return out


class MySQLClient(DBClient):
    name = "mysql"

    def __init__(self) -> None:
        self.conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "benchmark_db"),
            user=os.getenv("MYSQL_USER", "benchmark_user"),
            password=os.getenv("MYSQL_PASSWORD", "benchmark_pass"),
            autocommit=False,
        )

        self._pk_col: str | None = None
        self._pk_is_numeric: bool = True
        self._date_col: str | None = None
        self._status_col: str | None = None
        self._next_id: int = 9_000_000_000_000
        self.seed_ids: list[str] = []

    def setup(self, seed_rows: int) -> None:
        seed_rows = max(int(seed_rows or 0), 5)
        cur = self.conn.cursor()

        try:
            cur.execute("SELECT 1 FROM orders LIMIT 1;")
        except Exception as exc:
            cur.close()
            raise RuntimeError(
                "MySQL: missing table 'orders'. Run import first (python src/import_data.py --reset)."
            ) from exc

        cur.execute("SHOW KEYS FROM orders WHERE Key_name = 'PRIMARY';")
        pk_rows = cur.fetchall()
        self._pk_col = str(pk_rows[0][4]) if pk_rows else "order_id"

        cur.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'orders'
            ORDER BY ORDINAL_POSITION
            """
        )
        cols = [(str(r[0]), str(r[1]).lower()) for r in cur.fetchall()]
        col_names = [c for c, _ in cols]

        pk_type = next((t for c, t in cols if c == self._pk_col), "bigint")
        self._pk_is_numeric = pk_type in {"bigint", "int", "integer", "smallint", "decimal", "numeric"}

        self._date_col = "order_date" if "order_date" in col_names else next(
            (c for c in col_names if any(tok in c for tok in ("date", "time", "created", "updated"))),
            None,
        )
        self._status_col = (
            "status"
            if "status" in col_names
            else "order_status"
            if "order_status" in col_names
            else next((c for c, t in cols if any(x in t for x in ("varchar", "text")) and c != self._pk_col), None)
        )

        if not self._status_col:
            cur.close()
            raise RuntimeError("MySQL: could not infer a status/text column in orders.")

        if self._pk_is_numeric:
            cur.execute(f"SELECT COALESCE(MAX(`{self._pk_col}`), 0) FROM orders;")
            max_id = int(cur.fetchone()[0] or 0)
            self._next_id = max(self._next_id, max_id + 1)

        cur.execute(f"SELECT COUNT(*) FROM orders WHERE `{self._status_col}` LIKE %s;", ("seed_status%",))
        current = int(cur.fetchone()[0] or 0)
        missing = max(0, seed_rows - current)

        if missing:
            now = _utc_now().replace(tzinfo=None)
            if self._pk_is_numeric:
                ids: list[Any] = [self._next_id + i for i in range(missing)]
                self._next_id += missing
            else:
                ids = [f"seed_{_ts_ms()}_{i}" for i in range(missing)]

            if self._date_col:
                cur.executemany(
                    f"INSERT IGNORE INTO orders (`{self._pk_col}`, `{self._status_col}`, `{self._date_col}`) VALUES (%s, %s, %s)",
                    [(rid, f"seed_status_{i}", now) for i, rid in enumerate(ids)],
                )
            else:
                cur.executemany(
                    f"INSERT IGNORE INTO orders (`{self._pk_col}`, `{self._status_col}`) VALUES (%s, %s)",
                    [(rid, f"seed_status_{i}") for i, rid in enumerate(ids)],
                )

        cur.execute(
            f"SELECT `{self._pk_col}` FROM orders WHERE `{self._status_col}` LIKE %s ORDER BY `{self._pk_col}` LIMIT %s;",
            ("seed_status%", seed_rows),
        )
        self.seed_ids = [str(r[0]) for r in cur.fetchall()]

        self.conn.commit()
        cur.close()

    def close(self) -> None:
        self.conn.close()

    def create_single(self, payload: str) -> tuple[str, int]:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")

        now = _utc_now().replace(tzinfo=None)
        if self._pk_is_numeric:
            rid: Any = self._next_id
            self._next_id += 1
        else:
            rid = f"c_{_ts_ms()}_{next(_ROW_SEQ)}"

        cur = self.conn.cursor()
        try:
            if self._date_col:
                cur.execute(
                    f"INSERT INTO orders (`{self._pk_col}`, `{self._status_col}`, `{self._date_col}`) VALUES (%s, %s, %s)",
                    (rid, payload, now),
                )
            else:
                cur.execute(
                    f"INSERT INTO orders (`{self._pk_col}`, `{self._status_col}`) VALUES (%s, %s)",
                    (rid, payload),
                )
            affected = cur.rowcount or 0
            self.conn.commit()
            return str(rid), int(affected)
        finally:
            cur.close()

    def create_bulk(self, payloads: list[str]) -> int:
        if not payloads:
            return 0
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")

        now = _utc_now().replace(tzinfo=None)
        if self._pk_is_numeric:
            ids: list[Any] = [self._next_id + i for i in range(len(payloads))]
            self._next_id += len(payloads)
        else:
            ids = [f"cb_{_ts_ms()}_{i}_{next(_ROW_SEQ)}" for i in range(len(payloads))]

        cur = self.conn.cursor()
        try:
            if self._date_col:
                cur.executemany(
                    f"INSERT INTO orders (`{self._pk_col}`, `{self._status_col}`, `{self._date_col}`) VALUES (%s, %s, %s)",
                    [(ids[i], payloads[i], now) for i in range(len(payloads))],
                )
            else:
                cur.executemany(
                    f"INSERT INTO orders (`{self._pk_col}`, `{self._status_col}`) VALUES (%s, %s)",
                    [(ids[i], payloads[i]) for i in range(len(payloads))],
                )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected) if affected is not None else len(payloads)
        finally:
            cur.close()

    def read_by_id(self, row_id: str) -> int:
        if not self._pk_col:
            raise RuntimeError("MySQL client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        cur = self.conn.cursor()
        try:
            cur.execute(f"SELECT `{self._pk_col}` FROM orders WHERE `{self._pk_col}` = %s", (key,))
            return 1 if cur.fetchone() else 0
        finally:
            cur.close()

    def read_latest(self, limit: int) -> int:
        if not self._pk_col:
            raise RuntimeError("MySQL client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT `{self._pk_col}` FROM orders ORDER BY `{order_col}` DESC LIMIT %s",
                (limit,),
            )
            return len(cur.fetchall())
        finally:
            cur.close()

    def read_contains(self, needle: str, limit: int) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT `{self._pk_col}` FROM orders WHERE `{self._status_col}` LIKE %s LIMIT %s",
                (f"{needle}%", limit),
            )
            return len(cur.fetchall())
        finally:
            cur.close()

    def update_by_id(self, row_id: str, payload: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        now = _utc_now().replace(tzinfo=None)
        cur = self.conn.cursor()
        try:
            if self._date_col:
                cur.execute(
                    f"UPDATE orders SET `{self._status_col}` = %s, `{self._date_col}` = %s WHERE `{self._pk_col}` = %s",
                    (payload, now, key),
                )
            else:
                cur.execute(
                    f"UPDATE orders SET `{self._status_col}` = %s WHERE `{self._pk_col}` = %s",
                    (payload, key),
                )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        now = _utc_now().replace(tzinfo=None)
        cur = self.conn.cursor()
        try:
            if self._date_col:
                cur.execute(
                    f"""
                    UPDATE orders o
                    JOIN (SELECT `{self._pk_col}` AS id FROM (SELECT `{self._pk_col}` FROM orders ORDER BY `{order_col}` DESC LIMIT %s) t1) t
                      ON o.`{self._pk_col}` = t.id
                    SET o.`{self._status_col}` = CONCAT(COALESCE(o.`{self._status_col}`, ''), %s),
                        o.`{self._date_col}` = %s
                    """,
                    (limit, suffix, now),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE orders o
                    JOIN (SELECT `{self._pk_col}` AS id FROM (SELECT `{self._pk_col}` FROM orders ORDER BY `{order_col}` DESC LIMIT %s) t1) t
                      ON o.`{self._pk_col}` = t.id
                    SET o.`{self._status_col}` = CONCAT(COALESCE(o.`{self._status_col}`, ''), %s)
                    """,
                    (limit, suffix),
                )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def update_contains(self, needle: str, suffix: str) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")
        now = _utc_now().replace(tzinfo=None)
        cur = self.conn.cursor()
        try:
            if self._date_col:
                cur.execute(
                    f"UPDATE orders SET `{self._status_col}` = CONCAT(COALESCE(`{self._status_col}`, ''), %s), `{self._date_col}` = %s WHERE `{self._status_col}` LIKE %s",
                    (suffix, now, f"{needle}%"),
                )
            else:
                cur.execute(
                    f"UPDATE orders SET `{self._status_col}` = CONCAT(COALESCE(`{self._status_col}`, ''), %s) WHERE `{self._status_col}` LIKE %s",
                    (suffix, f"{needle}%"),
                )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def delete_by_id(self, row_id: str) -> int:
        if not self._pk_col:
            raise RuntimeError("MySQL client not initialized; call setup() first")
        key: Any = int(row_id) if self._pk_is_numeric else row_id
        cur = self.conn.cursor()
        try:
            cur.execute(f"DELETE FROM orders WHERE `{self._pk_col}` = %s", (key,))
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def delete_latest(self, limit: int) -> int:
        if not self._pk_col:
            raise RuntimeError("MySQL client not initialized; call setup() first")
        order_col = self._date_col or self._pk_col
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"""
                DELETE FROM orders
                WHERE `{self._pk_col}` IN (
                    SELECT id FROM (SELECT `{self._pk_col}` AS id FROM orders ORDER BY `{order_col}` DESC LIMIT %s) t
                )
                """,
                (limit,),
            )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def delete_contains(self, needle: str, limit: int) -> int:
        if not (self._pk_col and self._status_col):
            raise RuntimeError("MySQL client not initialized; call setup() first")
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"""
                DELETE FROM orders
                WHERE `{self._pk_col}` IN (
                    SELECT id FROM (
                        SELECT `{self._pk_col}` AS id FROM orders WHERE `{self._status_col}` LIKE %s LIMIT %s
                    ) t
                )
                """,
                (f"{needle}%", limit),
            )
            affected = cur.rowcount or 0
            self.conn.commit()
            return int(affected)
        finally:
            cur.close()

    def configure_mode(self, mode: str) -> None:
        if not self._status_col:
            return
        cur = self.conn.cursor()
        try:
            if mode == "after-index":
                if self._date_col:
                    try:
                        cur.execute(
                            f"CREATE INDEX idx_orders_order_date ON orders(`{self._date_col}`)"
                        )
                    except Exception:
                        pass
                try:
                    cur.execute(
                        f"CREATE INDEX idx_orders_status_prefix ON orders(`{self._status_col}`(64))"
                    )
                except Exception:
                    pass
            else:
                try:
                    cur.execute("DROP INDEX idx_orders_order_date ON orders")
                except Exception:
                    pass
                try:
                    cur.execute("DROP INDEX idx_orders_status_prefix ON orders")
                except Exception:
                    pass
            self.conn.commit()
        finally:
            cur.close()

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        if not (self._pk_col and self._status_col):
            return out
        order_col = self._date_col or self._pk_col
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"EXPLAIN SELECT `{self._pk_col}` FROM orders ORDER BY `{order_col}` DESC LIMIT 10;"
            )
            cols_1 = [d[0] for d in cur.description]
            rows_1 = cur.fetchall()
            out["R2_latest"] = json.dumps(
                [dict(zip(cols_1, r)) for r in rows_1], ensure_ascii=False, indent=2
            )

            cur.execute(
                f"EXPLAIN SELECT `{self._pk_col}` FROM orders WHERE `{self._status_col}` LIKE %s LIMIT %s;",
                ("seed_status%", 20),
            )
            cols_2 = [d[0] for d in cur.description]
            rows_2 = cur.fetchall()
            out["R3_prefix"] = json.dumps(
                [dict(zip(cols_2, r)) for r in rows_2], ensure_ascii=False, indent=2
            )
            return out
        finally:
            cur.close()


class MongoClientAdapter(DBClient):
    name = "mongo"

    def __init__(self) -> None:
        user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "benchmark_user")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "benchmark_pass")
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", "27017"))
        db_name = os.getenv("MONGO_DB", "benchmark_db")

        self.client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin",
            serverSelectionTimeoutMS=5000,
        )
        self.db = self.client[db_name]
        self.collection = self.db["orders"]
        self.seed_ids: list[str] = []

    def setup(self, seed_rows: int) -> None:
        seed_rows = max(int(seed_rows or 0), 5)
        # These indexes exist only to support benchmark queries; they do not change the dataset.
        try:
            self.collection.create_index([("order_date", -1)], name="idx_orders_order_date")
        except Exception:
            pass
        try:
            self.collection.create_index([("status", 1)], name="idx_orders_status")
        except Exception:
            pass

        current = self.collection.count_documents({"status": {"$regex": "^seed_status"}})
        missing = max(0, seed_rows - int(current))
        if missing:
            now = _utc_now().isoformat()
            docs: list[dict[str, Any]] = []
            for i in range(missing):
                oid = ObjectId(_stable_object_id_hex("mongo-seed", str(i)))
                docs.append({"_id": oid, "status": f"seed_status_{i}", "order_date": now})
            if docs:
                try:
                    self.collection.insert_many(docs, ordered=False)
                except Exception:
                    # Ignore duplicate key errors on reruns.
                    pass

        ids = list(
            self.collection.find(
                {"status": {"$regex": "^seed_status"}},
                {"_id": 1},
            )
            .sort("_id", 1)
            .limit(seed_rows)
        )
        self.seed_ids = [str(d["_id"]) for d in ids]

    def configure_mode(self, mode: str) -> None:
        # Mongo indexes are created in setup(); keep this for symmetry.
        _ = mode

    def close(self) -> None:
        self.client.close()

    def create_single(self, payload: str) -> tuple[str, int]:
        oid = ObjectId(_stable_object_id_hex("mongo", f"{_ts_ms()}:{next(_ROW_SEQ)}"))
        now = _utc_now().isoformat()
        self.collection.insert_one({"_id": oid, "status": payload, "order_date": now})
        return str(oid), 1

    def create_bulk(self, payloads: list[str]) -> int:
        if not payloads:
            return 0
        now = _utc_now().isoformat()
        docs: list[dict[str, Any]] = []
        for i, payload in enumerate(payloads):
            oid = ObjectId(_stable_object_id_hex("mongo-bulk", f"{_ts_ms()}:{i}:{next(_ROW_SEQ)}"))
            docs.append({"_id": oid, "status": payload, "order_date": now})
        if docs:
            self.collection.insert_many(docs, ordered=False)
        return len(docs)

    def read_by_id(self, row_id: str) -> int:
        try:
            oid = ObjectId(row_id)
        except Exception:
            return 0
        return 1 if self.collection.find_one({"_id": oid}, {"_id": 1}) else 0

    def read_latest(self, limit: int) -> int:
        return len(list(self.collection.find({}, {"_id": 1}).sort("order_date", -1).limit(limit)))

    def read_contains(self, needle: str, limit: int) -> int:
        return len(
            list(
                self.collection.find(
                    {"status": {"$regex": f"^{needle}"}},
                    {"_id": 1},
                ).limit(limit)
            )
        )

    def update_by_id(self, row_id: str, payload: str) -> int:
        try:
            oid = ObjectId(row_id)
        except Exception:
            return 0
        out = self.collection.update_one(
            {"_id": oid},
            {"$set": {"status": payload, "order_date": _utc_now().isoformat()}},
        )
        return int(out.modified_count)

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        ids = [
            doc["_id"]
            for doc in self.collection.find({}, {"_id": 1}).sort("order_date", -1).limit(limit)
        ]
        if not ids:
            return 0

        now = _utc_now().isoformat()
        try:
            out = self.collection.update_many(
                {"_id": {"$in": ids}},
                [{"$set": {"status": {"$concat": ["$status", suffix]}, "order_date": now}}],
            )
            return int(out.modified_count)
        except Exception:
            docs = list(self.collection.find({"_id": {"$in": ids}}, {"_id": 1, "status": 1}))
            modified = 0
            for doc in docs:
                current = doc.get("status") or ""
                r = self.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": current + suffix, "order_date": now}},
                )
                modified += int(getattr(r, "modified_count", 0) or 0)
            return modified

    def update_contains(self, needle: str, suffix: str) -> int:
        now = _utc_now().isoformat()
        try:
            out = self.collection.update_many(
                {"status": {"$regex": f"^{needle}"}},
                [{"$set": {"status": {"$concat": ["$status", suffix]}, "order_date": now}}],
            )
            return int(out.modified_count)
        except Exception:
            docs = list(
                self.collection.find(
                    {"status": {"$regex": f"^{needle}"}},
                    {"_id": 1, "status": 1},
                ).limit(5000)
            )
            modified = 0
            for doc in docs:
                current = doc.get("status") or ""
                r = self.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": current + suffix, "order_date": now}},
                )
                modified += int(getattr(r, "modified_count", 0) or 0)
            return modified

    def delete_by_id(self, row_id: str) -> int:
        try:
            oid = ObjectId(row_id)
        except Exception:
            return 0
        out = self.collection.delete_one({"_id": oid})
        return int(out.deleted_count)

    def delete_latest(self, limit: int) -> int:
        ids = [doc["_id"] for doc in self.collection.find({}, {"_id": 1}).sort("order_date", -1).limit(limit)]
        if not ids:
            return 0
        out = self.collection.delete_many({"_id": {"$in": ids}})
        return int(out.deleted_count)

    def delete_contains(self, needle: str, limit: int) -> int:
        ids = [
            doc["_id"]
            for doc in self.collection.find({"status": {"$regex": f"^{needle}"}}, {"_id": 1}).limit(limit)
        ]
        if not ids:
            return 0
        out = self.collection.delete_many({"_id": {"$in": ids}})
        return int(out.deleted_count)

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            r2 = (
                self.collection.find({}, {"_id": 1})
                .sort("order_date", -1)
                .limit(10)
                .explain(verbosity="queryPlanner")
            )
            out["R2_latest"] = json.dumps(r2, ensure_ascii=False, indent=2)
        except Exception:
            pass

        try:
            r3 = (
                self.collection.find(
                    {"status": {"$regex": "^seed_status"}},
                    {"_id": 1},
                )
                .limit(20)
                .explain(verbosity="queryPlanner")
            )
            out["R3_prefix"] = json.dumps(r3, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return out


class ScyllaClient(DBClient):
    name = "scylla"

    def __init__(self) -> None:
        try:
            from cassandra.cluster import Cluster
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing dependency 'cassandra-driver'. Run: pip install -r requirements.txt"
            ) from exc

        host = os.getenv("SCYLLA_HOST", "localhost")
        port = int(os.getenv("SCYLLA_PORT", "9042"))
        keyspace = os.getenv("SCYLLA_KEYSPACE", "benchmark_db")

        self.cluster = Cluster([host], port=port)
        self.session = self.cluster.connect()
        self.keyspace = keyspace

        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
        self.session.set_keyspace(self.keyspace)

        self.table = os.getenv("SCYLLA_ORDERS_TABLE", "retail_orders")
        self.seed_ids: list[str] = []
        self._recent_ids: deque[str] = deque(maxlen=5000)

        self._prepared_insert: Any = None
        self._prepared_select: Any = None
        self._prepared_delete: Any = None

    def setup(self, seed_rows: int) -> None:
        seed_rows = max(int(seed_rows or 0), 5)

        # Verify that the importer-created KV table exists.
        try:
            self.session.execute(f"SELECT id FROM {self.table} LIMIT 1")
        except Exception as exc:
            raise RuntimeError(
                f"Scylla: missing table '{self.table}'. Run import first (python src/import_data.py --reset)."
            ) from exc

        self._prepared_insert = self.session.prepare(
            f"INSERT INTO {self.table} (id, data) VALUES (?, ?)"
        )
        self._prepared_select = self.session.prepare(
            f"SELECT data FROM {self.table} WHERE id = ?"
        )
        self._prepared_delete = self.session.prepare(
            f"DELETE FROM {self.table} WHERE id = ?"
        )

        now = _utc_now().isoformat()
        ids: list[str] = []
        for i in range(seed_rows):
            rid = f"seed_{i}"
            payload = json.dumps(
                {"status": f"seed_status_{i}", "order_date": now},
                ensure_ascii=False,
            )
            self.session.execute(self._prepared_insert, (rid, payload))
            ids.append(rid)

        self.seed_ids = ids
        self._recent_ids.clear()
        self._recent_ids.extend(ids)

    def close(self) -> None:
        self.cluster.shutdown()

    def create_single(self, payload: str) -> tuple[str, int]:
        rid = f"c_{_ts_ms()}_{next(_ROW_SEQ)}"
        data = json.dumps(
            {"status": payload, "order_date": _utc_now().isoformat()},
            ensure_ascii=False,
        )
        self.session.execute(self._prepared_insert, (rid, data))
        self._recent_ids.append(rid)
        return rid, 1

    def create_bulk(self, payloads: list[str]) -> int:
        if not payloads:
            return 0
        now = _utc_now().isoformat()
        affected = 0
        for payload in payloads:
            rid = f"cb_{_ts_ms()}_{next(_ROW_SEQ)}"
            data = json.dumps({"status": payload, "order_date": now}, ensure_ascii=False)
            self.session.execute(self._prepared_insert, (rid, data))
            self._recent_ids.append(rid)
            affected += 1
        return affected

    def read_by_id(self, row_id: str) -> int:
        out = self.session.execute(self._prepared_select, (row_id,)).one()
        return 1 if out else 0

    def read_latest(self, limit: int) -> int:
        ids = list(self._recent_ids)[-max(0, int(limit)) :]
        found = 0
        for rid in reversed(ids):
            if self.session.execute(self._prepared_select, (rid,)).one():
                found += 1
        return found

    def read_contains(self, needle: str, limit: int) -> int:
        limit = max(0, int(limit))
        if limit == 0:
            return 0

        matched = 0
        # Prefer scanning benchmark-inserted ids (fast point reads).
        for rid in reversed(self._recent_ids):
            row = self.session.execute(self._prepared_select, (rid,)).one()
            if not row:
                continue
            try:
                data = json.loads(getattr(row, "data", "") or "{}")
            except Exception:
                data = {}
            status = str(data.get("status") or "")
            if status.startswith(needle):
                matched += 1
                if matched >= limit:
                    return matched

        # Best-effort fallback: limited scan.
        try:
            rows = list(self.session.execute(f"SELECT id, data FROM {self.table} LIMIT 5000"))
        except Exception:
            return matched

        for row in rows:
            try:
                data = json.loads(getattr(row, "data", "") or "{}")
            except Exception:
                continue
            status = str(data.get("status") or "")
            if status.startswith(needle):
                matched += 1
                if matched >= limit:
                    break
        return matched

    def update_by_id(self, row_id: str, payload: str) -> int:
        prev = self.session.execute(self._prepared_select, (row_id,)).one()
        if not prev:
            return 0
        try:
            data = json.loads(getattr(prev, "data", "") or "{}")
        except Exception:
            data = {}
        data["status"] = payload
        data["order_date"] = _utc_now().isoformat()
        self.session.execute(self._prepared_insert, (row_id, json.dumps(data, ensure_ascii=False)))
        return 1

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        ids = list(self._recent_ids)[-max(0, int(limit)) :]
        affected = 0
        for rid in reversed(ids):
            prev = self.session.execute(self._prepared_select, (rid,)).one()
            if not prev:
                continue
            try:
                data = json.loads(getattr(prev, "data", "") or "{}")
            except Exception:
                data = {}
            current = str(data.get("status") or "")
            affected += self.update_by_id(rid, current + suffix)
        return affected

    def update_contains(self, needle: str, suffix: str) -> int:
        affected = 0
        for rid in list(self._recent_ids):
            prev = self.session.execute(self._prepared_select, (rid,)).one()
            if not prev:
                continue
            try:
                data = json.loads(getattr(prev, "data", "") or "{}")
            except Exception:
                continue
            status = str(data.get("status") or "")
            if not status.startswith(needle):
                continue
            affected += self.update_by_id(rid, status + suffix)
        return affected

    def delete_by_id(self, row_id: str) -> int:
        prev = self.session.execute(self._prepared_select, (row_id,)).one()
        if not prev:
            return 0
        self.session.execute(self._prepared_delete, (row_id,))
        try:
            self._recent_ids.remove(row_id)
        except ValueError:
            pass
        return 1

    def delete_latest(self, limit: int) -> int:
        ids = list(self._recent_ids)[-max(0, int(limit)) :]
        deleted = 0
        for rid in reversed(ids):
            deleted += self.delete_by_id(rid)
        return deleted

    def delete_contains(self, needle: str, limit: int) -> int:
        limit = max(0, int(limit))
        if limit == 0:
            return 0
        deleted = 0
        for rid in list(self._recent_ids):
            row = self.session.execute(self._prepared_select, (rid,)).one()
            if not row:
                continue
            try:
                data = json.loads(getattr(row, "data", "") or "{}")
            except Exception:
                continue
            status = str(data.get("status") or "")
            if not status.startswith(needle):
                continue
            deleted += self.delete_by_id(rid)
            if deleted >= limit:
                break
        return deleted


def _new_row_id(prefix: str) -> str:
    return f"{prefix}_{_ts_ms()}_{next(_ROW_SEQ)}_{random.randint(1000, 9999)}"


_ROW_SEQ = itertools.count(1)


def _scenario_create_single(client: DBClient, ctx: dict[str, Any]) -> int:
    row_id = _new_row_id("c1")
    ctx["last_single_id"] = row_id
    return client.create_single(row_id, _random_payload())


def _scenario_create_bulk_10(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("cb10"), _random_payload()) for _ in range(10)]
    return client.create_bulk(rows)


def _scenario_create_bulk_100(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("cb100"), _random_payload()) for _ in range(100)]
    return client.create_bulk(rows)


def _scenario_create_bulk_250(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("cb250"), _random_payload()) for _ in range(250)]
    return client.create_bulk(rows)


def _scenario_create_single_marker(client: DBClient, ctx: dict[str, Any]) -> int:
    row_id = _new_row_id("cmark")
    ctx["last_marker_id"] = row_id
    return client.create_single(row_id, "marker_payload_v1")


def _scenario_create_bulk_50_marker(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("cb50m"), "marker_payload_batch") for _ in range(50)]
    return client.create_bulk(rows)


def _scenario_read_by_id_seed(client: DBClient, ctx: dict[str, Any]) -> int:
    seed_id = ctx["seed_ids"][0]
    return client.read_by_id(seed_id)


def _scenario_read_latest_10(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.read_latest(10)


def _scenario_read_contains_seed(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.read_contains("seed_payload", 20)


def _scenario_read_latest_100(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.read_latest(100)


def _scenario_read_contains_anchor(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.read_contains("seed_payload_anchor", 10)


def _scenario_read_by_last_marker(client: DBClient, ctx: dict[str, Any]) -> int:
    marker_id = ctx.get("last_marker_id")
    if not marker_id:
        marker_id = _new_row_id("rmark")
        client.create_single(marker_id, "marker_payload_read")
        ctx["last_marker_id"] = marker_id
    return client.read_by_id(marker_id)


def _scenario_update_by_id_seed(client: DBClient, ctx: dict[str, Any]) -> int:
    seed_id = ctx["seed_ids"][0]
    return client.update_by_id(seed_id, _random_payload())


def _scenario_update_bulk_latest_10(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.update_bulk_latest(10, "_u10")


def _scenario_update_contains_seed(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.update_contains("seed_payload", "_ucontains")


def _scenario_update_by_id_seed_2(client: DBClient, ctx: dict[str, Any]) -> int:
    seed_id = ctx["seed_ids"][1]
    return client.update_by_id(seed_id, _random_payload())


def _scenario_update_bulk_latest_50(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.update_bulk_latest(50, "_u50")


def _scenario_update_contains_anchor(client: DBClient, ctx: dict[str, Any]) -> int:
    return client.update_contains("seed_payload_anchor", "_uanchor")


def _scenario_delete_by_id_temp(client: DBClient, ctx: dict[str, Any]) -> int:
    row_id = _new_row_id("d1")
    client.create_single(row_id, "temp")
    return client.delete_by_id(row_id)


def _scenario_delete_latest_5(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("dl5"), "temp") for _ in range(5)]
    client.create_bulk(rows)
    return client.delete_latest(5)


def _scenario_delete_contains_temp(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("dct"), "temp_delete_marker") for _ in range(5)]
    client.create_bulk(rows)
    return client.delete_contains("temp_delete_marker", 5)


def _scenario_delete_latest_20(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("dl20"), "temp") for _ in range(20)]
    client.create_bulk(rows)
    return client.delete_latest(20)


def _scenario_delete_by_id_marker(client: DBClient, ctx: dict[str, Any]) -> int:
    row_id = _new_row_id("dmark")
    client.create_single(row_id, "marker_payload_delete")
    return client.delete_by_id(row_id)


def _scenario_delete_contains_marker_20(client: DBClient, ctx: dict[str, Any]) -> int:
    rows = [(_new_row_id("dcm20"), "marker_payload_delete_batch") for _ in range(20)]
    client.create_bulk(rows)
    return client.delete_contains("marker_payload_delete_batch", 20)


def build_scenarios() -> list[Scenario]:
    return [
        Scenario("C1", "create", "Insert single row", _scenario_create_single),
        Scenario("C2", "create", "Insert 10 rows", _scenario_create_bulk_10),
        Scenario("C3", "create", "Insert 100 rows", _scenario_create_bulk_100),
        Scenario("C4", "create", "Insert 250 rows", _scenario_create_bulk_250),
        Scenario("C5", "create", "Insert single marker row", _scenario_create_single_marker),
        Scenario("C6", "create", "Insert 50 marker rows", _scenario_create_bulk_50_marker),
        Scenario("R1", "read", "Read by existing ID", _scenario_read_by_id_seed),
        Scenario("R2", "read", "Read latest 10 rows", _scenario_read_latest_10),
        Scenario("R3", "read", "Read by payload pattern", _scenario_read_contains_seed),
        Scenario("R4", "read", "Read latest 100 rows", _scenario_read_latest_100),
        Scenario("R5", "read", "Read by anchor payload pattern", _scenario_read_contains_anchor),
        Scenario("R6", "read", "Read by last marker ID", _scenario_read_by_last_marker),
        Scenario("U1", "update", "Update one row by ID", _scenario_update_by_id_seed),
        Scenario("U2", "update", "Update latest 10 rows", _scenario_update_bulk_latest_10),
        Scenario("U3", "update", "Update by payload pattern", _scenario_update_contains_seed),
        Scenario("U4", "update", "Update second seed row by ID", _scenario_update_by_id_seed_2),
        Scenario("U5", "update", "Update latest 50 rows", _scenario_update_bulk_latest_50),
        Scenario("U6", "update", "Update by anchor payload pattern", _scenario_update_contains_anchor),
        Scenario("D1", "delete", "Delete one temp row", _scenario_delete_by_id_temp),
        Scenario("D2", "delete", "Delete latest 5 temp rows", _scenario_delete_latest_5),
        Scenario("D3", "delete", "Delete by temp marker", _scenario_delete_contains_temp),
        Scenario("D4", "delete", "Delete latest 20 temp rows", _scenario_delete_latest_20),
        Scenario("D5", "delete", "Delete one marker row by ID", _scenario_delete_by_id_marker),
        Scenario("D6", "delete", "Delete 20 marker rows by pattern", _scenario_delete_contains_marker_20),
    ]


def _client_factory(name: str) -> DBClient:
    if name == "postgres":
        return PostgresClient()
    if name == "mysql":
        return MySQLClient()
    if name == "mongo":
        return MongoClientAdapter()
    if name == "scylla":
        return ScyllaClient()
    raise ValueError(f"Unsupported engine: {name}")


def _seed_ids(client: DBClient, count: int = 5) -> list[str]:
    seed_ids = [f"seed_anchor_{i}" for i in range(count)]
    for sid in seed_ids:
        try:
            if client.read_by_id(sid):
                continue
            client.create_single(sid, f"seed_payload_anchor_{sid}")
        except Exception:
            # Repeated runs may hit duplicate keys or engine-specific transient errors.
            # For relational engines, clear aborted transaction state so benchmarking can continue.
            conn = getattr(client, "conn", None)
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
    return seed_ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CRUD benchmark scenarios across selected databases.")
    parser.add_argument(
        "--db",
        nargs="+",
        choices=["postgres", "mysql", "mongo", "scylla", "all"],
        default=["all"],
        help="Databases to benchmark (default: all).",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=3,
        help="Number of trials per scenario (default: 3).",
    )
    parser.add_argument(
        "--seed-rows",
        type=int,
        default=1000,
        help="Minimum seed rows in benchmark_ops table/collection before benchmarks (default: 1000).",
    )
    parser.add_argument(
        "--size-label",
        default="mvp",
        help="Logical data size label saved in results (e.g., 500k, 1m, 10m).",
    )
    parser.add_argument(
        "--mode",
        choices=["baseline", "after-index"],
        default="baseline",
        help="Benchmark mode label saved in results.",
    )
    parser.add_argument(
        "--save-explain",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save EXPLAIN samples to JSON file (default: enabled).",
    )
    parser.add_argument(
        "--output-dir",
        default="data/results",
        help="Output directory for benchmark results.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file.",
    )
    return parser.parse_args()


def _write_outputs(output_dir: Path, run_id: str, results: list[TrialResult]) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"benchmark_{run_id}.csv"
    json_path = output_dir / f"benchmark_{run_id}.json"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "db_engine",
                "scenario_id",
                "operation",
                "trial_no",
                "latency_ms",
                "rows_affected",
                "data_size_label",
                "mode",
                "timestamp_utc",
            ],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(row.__dict__)

    payload = {
        "run_id": run_id,
        "generated_at_utc": _utc_now().isoformat(),
        "rows": [r.__dict__ for r in results],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return csv_path, json_path


def _print_summary(results: list[TrialResult]) -> None:
    grouped: dict[tuple[str, str], list[float]] = {}
    for row in results:
        grouped.setdefault((row.db_engine, row.operation), []).append(row.latency_ms)

    print("\n=== Summary (avg latency ms by DB/operation) ===")
    for (db, op), vals in sorted(grouped.items()):
        avg = statistics.mean(vals)
        print(f"{db:10s} {op:7s} avg={avg:9.2f} ms n={len(vals)}")


def main() -> int:
    args = parse_args()
    _load_env_file(Path(args.env_file))

    dbs = args.db
    if "all" in dbs:
        dbs = ["postgres", "mysql", "mongo", "scylla"]

    scenarios = build_scenarios()
    run_id = _utc_now().strftime("%Y%m%d_%H%M%S")
    results: list[TrialResult] = []
    explain_payload: dict[str, Any] = {
        "run_id": run_id,
        "mode": args.mode,
        "generated_at_utc": _utc_now().isoformat(),
        "databases": {},
    }

    print(f"Run ID: {run_id}")
    print(f"Databases: {', '.join(dbs)}")
    print(f"Scenarios: {len(scenarios)} | Trials: {args.trials}")

    for db in dbs:
        print(f"\n--- DB: {db} ---")
        client = _client_factory(db)
        try:
            client.setup(args.seed_rows)
            client.configure_mode(args.mode)
            seed_ids = _seed_ids(client)
            ctx = {"seed_ids": seed_ids}

            for scenario in scenarios:
                for trial in range(1, args.trials + 1):
                    start = time.perf_counter()
                    affected = scenario.executor(client, ctx)
                    elapsed_ms = (time.perf_counter() - start) * 1000.0

                    result = TrialResult(
                        run_id=run_id,
                        db_engine=db,
                        scenario_id=scenario.scenario_id,
                        operation=scenario.operation,
                        trial_no=trial,
                        latency_ms=round(elapsed_ms, 3),
                        rows_affected=affected,
                        data_size_label=args.size_label,
                        mode=args.mode,
                        timestamp_utc=_utc_now().isoformat(),
                    )
                    results.append(result)
                    print(
                        f"[{db}] {scenario.scenario_id} trial={trial} latency={result.latency_ms:.3f}ms rows={affected}",
                        flush=True,
                    )

            if args.save_explain:
                explain_payload["databases"][db] = client.explain_samples()
        finally:
            client.close()

    csv_path, json_path = _write_outputs(Path(args.output_dir), run_id, results)
    _print_summary(results)

    print("\nSaved:")
    print(f"- CSV:  {csv_path}")
    print(f"- JSON: {json_path}")

    if args.save_explain:
        explain_path = Path(args.output_dir) / f"explain_{run_id}.json"
        explain_path.write_text(json.dumps(explain_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"- EXPLAIN: {explain_path}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted.")
        sys.exit(130)
