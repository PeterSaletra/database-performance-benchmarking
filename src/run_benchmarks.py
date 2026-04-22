from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import os
import random
import statistics
import string
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import mysql.connector
import psycopg
from pymongo import MongoClient


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ts_ms() -> int:
    return int(_utc_now().timestamp() * 1000)


def _random_payload(size: int = 64) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(size))


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

    def create_single(self, row_id: str, payload: str) -> int:
        raise NotImplementedError

    def create_bulk(self, rows: list[tuple[str, str]]) -> int:
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
        return {}


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

    def setup(self, seed_rows: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS benchmark_ops (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_benchmark_ops_updated_at ON benchmark_ops(updated_at DESC);"
            )
            cur.execute("SELECT COUNT(*) FROM benchmark_ops;")
            current = int(cur.fetchone()[0])

            if current < seed_rows:
                missing = seed_rows - current
                now = _utc_now()
                rows = [
                    (f"seed_{i}_{_ts_ms()}", f"seed_payload_{i}", now)
                    for i in range(missing)
                ]
                cur.executemany(
                    "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
                    rows,
                )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def create_single(self, row_id: str, payload: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
                (row_id, payload, _utc_now()),
            )
        self.conn.commit()
        return 1

    def create_bulk(self, rows: list[tuple[str, str]]) -> int:
        now = _utc_now()
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
                [(rid, payload, now) for rid, payload in rows],
            )
        self.conn.commit()
        return len(rows)

    def read_by_id(self, row_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM benchmark_ops WHERE id = %s", (row_id,))
            return 1 if cur.fetchone() else 0

    def read_latest(self, limit: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s", (limit,)
            )
            return len(cur.fetchall())

    def read_contains(self, needle: str, limit: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM benchmark_ops WHERE payload LIKE %s LIMIT %s",
                (f"{needle}%", limit),
            )
            return len(cur.fetchall())

    def update_by_id(self, row_id: str, payload: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE benchmark_ops SET payload = %s, updated_at = %s WHERE id = %s",
                (payload, _utc_now(), row_id),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH ids AS (
                    SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s
                )
                UPDATE benchmark_ops b
                SET payload = b.payload || %s, updated_at = %s
                FROM ids
                WHERE b.id = ids.id
                """,
                (limit, suffix, _utc_now()),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def update_contains(self, needle: str, suffix: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE benchmark_ops SET payload = payload || %s, updated_at = %s WHERE payload LIKE %s",
                (suffix, _utc_now(), f"{needle}%"),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_by_id(self, row_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM benchmark_ops WHERE id = %s", (row_id,))
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_latest(self, limit: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM benchmark_ops
                WHERE id IN (
                    SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s
                )
                """,
                (limit,),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def delete_contains(self, needle: str, limit: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM benchmark_ops
                WHERE id IN (
                    SELECT id FROM benchmark_ops
                    WHERE payload LIKE %s
                    LIMIT %s
                )
                """,
                (f"{needle}%", limit),
            )
            affected = cur.rowcount or 0
        self.conn.commit()
        return affected

    def configure_mode(self, mode: str) -> None:
        with self.conn.cursor() as cur:
            if mode == "after-index":
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_benchmark_ops_payload_prefix ON benchmark_ops (payload text_pattern_ops);"
                )
            else:
                cur.execute("DROP INDEX IF EXISTS idx_benchmark_ops_payload_prefix;")
        self.conn.commit()

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        with self.conn.cursor() as cur:
            cur.execute("EXPLAIN SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT 10;")
            out["R2_latest"] = "\n".join(str(r[0]) for r in cur.fetchall())

            cur.execute(
                "EXPLAIN SELECT id FROM benchmark_ops WHERE payload LIKE %s LIMIT 20;",
                ("seed_payload%",),
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

    def setup(self, seed_rows: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_ops (
                id VARCHAR(255) PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at DATETIME(6) NOT NULL
            ) ENGINE=InnoDB;
            """
        )
        try:
            cur.execute(
                "CREATE INDEX idx_benchmark_ops_updated_at ON benchmark_ops(updated_at DESC);"
            )
        except Exception:
            # Index may already exist on repeated runs.
            pass
        cur.execute("SELECT COUNT(*) FROM benchmark_ops;")
        current = int(cur.fetchone()[0])

        if current < seed_rows:
            missing = seed_rows - current
            now = _utc_now().replace(tzinfo=None)
            rows = [(f"seed_{i}_{_ts_ms()}", f"seed_payload_{i}", now) for i in range(missing)]
            cur.executemany(
                "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
                rows,
            )
        self.conn.commit()
        cur.close()

    def close(self) -> None:
        self.conn.close()

    def create_single(self, row_id: str, payload: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
            (row_id, payload, _utc_now().replace(tzinfo=None)),
        )
        self.conn.commit()
        cur.close()
        return 1

    def create_bulk(self, rows: list[tuple[str, str]]) -> int:
        cur = self.conn.cursor()
        now = _utc_now().replace(tzinfo=None)
        cur.executemany(
            "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
            [(rid, payload, now) for rid, payload in rows],
        )
        self.conn.commit()
        cur.close()
        return len(rows)

    def read_by_id(self, row_id: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM benchmark_ops WHERE id = %s", (row_id,))
        out = 1 if cur.fetchone() else 0
        cur.close()
        return out

    def read_latest(self, limit: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s", (limit,))
        out = len(cur.fetchall())
        cur.close()
        return out

    def read_contains(self, needle: str, limit: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id FROM benchmark_ops WHERE payload LIKE %s LIMIT %s",
            (f"{needle}%", limit),
        )
        out = len(cur.fetchall())
        cur.close()
        return out

    def update_by_id(self, row_id: str, payload: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE benchmark_ops SET payload = %s, updated_at = %s WHERE id = %s",
            (payload, _utc_now().replace(tzinfo=None), row_id),
        )
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE benchmark_ops
            SET payload = CONCAT(payload, %s), updated_at = %s
            WHERE id IN (
                SELECT id FROM (
                    SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s
                ) AS t
            )
            """,
            (suffix, _utc_now().replace(tzinfo=None), limit),
        )
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def update_contains(self, needle: str, suffix: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE benchmark_ops SET payload = CONCAT(payload, %s), updated_at = %s WHERE payload LIKE %s",
            (suffix, _utc_now().replace(tzinfo=None), f"{needle}%"),
        )
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def delete_by_id(self, row_id: str) -> int:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM benchmark_ops WHERE id = %s", (row_id,))
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def delete_latest(self, limit: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            DELETE FROM benchmark_ops
            WHERE id IN (
                SELECT id FROM (
                    SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT %s
                ) AS t
            )
            """,
            (limit,),
        )
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def delete_contains(self, needle: str, limit: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            DELETE FROM benchmark_ops
            WHERE id IN (
                SELECT id FROM (
                    SELECT id FROM benchmark_ops WHERE payload LIKE %s LIMIT %s
                ) AS t
            )
            """,
            (f"{needle}%", limit),
        )
        affected = cur.rowcount or 0
        self.conn.commit()
        cur.close()
        return affected

    def configure_mode(self, mode: str) -> None:
        cur = self.conn.cursor()
        if mode == "after-index":
            try:
                cur.execute(
                    "CREATE INDEX idx_benchmark_ops_payload_prefix ON benchmark_ops(payload(64));"
                )
            except Exception:
                pass
        else:
            try:
                cur.execute("DROP INDEX idx_benchmark_ops_payload_prefix ON benchmark_ops;")
            except Exception:
                pass
        self.conn.commit()
        cur.close()

    def explain_samples(self) -> dict[str, str]:
        out: dict[str, str] = {}
        cur = self.conn.cursor()
        cur.execute("EXPLAIN SELECT id FROM benchmark_ops ORDER BY updated_at DESC LIMIT 10;")
        cols_1 = [d[0] for d in cur.description]
        rows_1 = cur.fetchall()
        out["R2_latest"] = json.dumps([dict(zip(cols_1, r)) for r in rows_1], ensure_ascii=False, indent=2)

        cur.execute("EXPLAIN SELECT id FROM benchmark_ops WHERE payload LIKE %s LIMIT %s;", ("seed_payload%", 20))
        cols_2 = [d[0] for d in cur.description]
        rows_2 = cur.fetchall()
        out["R3_prefix"] = json.dumps([dict(zip(cols_2, r)) for r in rows_2], ensure_ascii=False, indent=2)
        cur.close()
        return out


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
        self.collection = self.db["benchmark_ops"]

    def setup(self, seed_rows: int) -> None:
        self.collection.create_index([("updated_at", -1)])
        current = self.collection.estimated_document_count()
        if current < seed_rows:
            missing = seed_rows - current
            rows = [
                {
                    "_id": f"seed_{i}_{_ts_ms()}",
                    "payload": f"seed_payload_{i}",
                    "updated_at": _utc_now(),
                }
                for i in range(missing)
            ]
            if rows:
                self.collection.insert_many(rows, ordered=False)

    def configure_mode(self, mode: str) -> None:
        if mode == "after-index":
            self.collection.create_index([("payload", 1)], name="idx_benchmark_ops_payload_prefix")
        else:
            try:
                self.collection.drop_index("idx_benchmark_ops_payload_prefix")
            except Exception:
                pass

    def close(self) -> None:
        self.client.close()

    def create_single(self, row_id: str, payload: str) -> int:
        self.collection.insert_one({"_id": row_id, "payload": payload, "updated_at": _utc_now()})
        return 1

    def create_bulk(self, rows: list[tuple[str, str]]) -> int:
        docs = [{"_id": rid, "payload": payload, "updated_at": _utc_now()} for rid, payload in rows]
        if docs:
            self.collection.insert_many(docs, ordered=False)
        return len(docs)

    def read_by_id(self, row_id: str) -> int:
        return 1 if self.collection.find_one({"_id": row_id}, {"_id": 1}) else 0

    def read_latest(self, limit: int) -> int:
        return len(list(self.collection.find({}, {"_id": 1}).sort("updated_at", -1).limit(limit)))

    def read_contains(self, needle: str, limit: int) -> int:
        return len(
            list(
                self.collection.find(
                    {"payload": {"$regex": f"^{needle}"}},
                    {"_id": 1},
                ).limit(limit)
            )
        )

    def update_by_id(self, row_id: str, payload: str) -> int:
        out = self.collection.update_one(
            {"_id": row_id},
            {"$set": {"payload": payload, "updated_at": _utc_now()}},
        )
        return int(out.modified_count)

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        ids = [doc["_id"] for doc in self.collection.find({}, {"_id": 1}).sort("updated_at", -1).limit(limit)]
        if not ids:
            return 0

        out = self.collection.update_many(
            {"_id": {"$in": ids}},
            [{"$set": {"payload": {"$concat": ["$payload", suffix]}, "updated_at": _utc_now()}}],
        )
        return int(out.modified_count)

    def update_contains(self, needle: str, suffix: str) -> int:
        out = self.collection.update_many(
            {"payload": {"$regex": f"^{needle}"}},
            [{"$set": {"payload": {"$concat": ["$payload", suffix]}, "updated_at": _utc_now()}}],
        )
        return int(out.modified_count)

    def delete_by_id(self, row_id: str) -> int:
        out = self.collection.delete_one({"_id": row_id})
        return int(out.deleted_count)

    def delete_latest(self, limit: int) -> int:
        ids = [doc["_id"] for doc in self.collection.find({}, {"_id": 1}).sort("updated_at", -1).limit(limit)]
        if not ids:
            return 0
        out = self.collection.delete_many({"_id": {"$in": ids}})
        return int(out.deleted_count)

    def delete_contains(self, needle: str, limit: int) -> int:
        ids = [
            doc["_id"]
            for doc in self.collection.find({"payload": {"$regex": f"^{needle}"}}, {"_id": 1}).limit(limit)
        ]
        if not ids:
            return 0
        out = self.collection.delete_many({"_id": {"$in": ids}})
        return int(out.deleted_count)


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

    def setup(self, seed_rows: int) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_ops (
                id text PRIMARY KEY,
                payload text,
                updated_at timestamp
            )
            """
        )

        count_row = self.session.execute("SELECT COUNT(*) AS c FROM benchmark_ops").one()
        current = int(getattr(count_row, "c", 0) or 0)
        if current < seed_rows:
            missing = seed_rows - current
            insert_stmt = self.session.prepare(
                "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (?, ?, ?)"
            )
            now = _utc_now()
            for i in range(missing):
                self.session.execute(insert_stmt, (f"seed_{i}_{_ts_ms()}", f"seed_payload_{i}", now))

    def close(self) -> None:
        self.cluster.shutdown()

    def create_single(self, row_id: str, payload: str) -> int:
        self.session.execute(
            "INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (%s, %s, %s)",
            (row_id, payload, _utc_now()),
        )
        return 1

    def create_bulk(self, rows: list[tuple[str, str]]) -> int:
        stmt = self.session.prepare("INSERT INTO benchmark_ops(id, payload, updated_at) VALUES (?, ?, ?)")
        now = _utc_now()
        for rid, payload in rows:
            self.session.execute(stmt, (rid, payload, now))
        return len(rows)

    def read_by_id(self, row_id: str) -> int:
        out = self.session.execute("SELECT id FROM benchmark_ops WHERE id = %s", (row_id,)).one()
        return 1 if out else 0

    def read_latest(self, limit: int) -> int:
        # Scylla doesn't support ORDER BY without proper clustering keys.
        rows = list(self.session.execute("SELECT id FROM benchmark_ops LIMIT %s", (limit,)))
        return len(rows)

    def read_contains(self, needle: str, limit: int) -> int:
        # Fallback full scan for MVP parity across engines.
        rows = list(self.session.execute("SELECT id, payload FROM benchmark_ops LIMIT 5000"))
        matched = 0
        for row in rows:
            payload = getattr(row, "payload", "") or ""
            if payload.startswith(needle):
                matched += 1
            if matched >= limit:
                break
        return matched

    def update_by_id(self, row_id: str, payload: str) -> int:
        self.session.execute(
            "UPDATE benchmark_ops SET payload = %s, updated_at = %s WHERE id = %s",
            (payload, _utc_now(), row_id),
        )
        return 1

    def update_bulk_latest(self, limit: int, suffix: str) -> int:
        rows = list(self.session.execute("SELECT id, payload FROM benchmark_ops LIMIT %s", (limit,)))
        affected = 0
        for row in rows:
            rid = getattr(row, "id")
            payload = getattr(row, "payload", "") or ""
            self.session.execute(
                "UPDATE benchmark_ops SET payload = %s, updated_at = %s WHERE id = %s",
                (payload + suffix, _utc_now(), rid),
            )
            affected += 1
        return affected

    def update_contains(self, needle: str, suffix: str) -> int:
        rows = list(self.session.execute("SELECT id, payload FROM benchmark_ops LIMIT 5000"))
        affected = 0
        for row in rows:
            payload = getattr(row, "payload", "") or ""
            if not payload.startswith(needle):
                continue
            rid = getattr(row, "id")
            self.session.execute(
                "UPDATE benchmark_ops SET payload = %s, updated_at = %s WHERE id = %s",
                (payload + suffix, _utc_now(), rid),
            )
            affected += 1
        return affected

    def delete_by_id(self, row_id: str) -> int:
        self.session.execute("DELETE FROM benchmark_ops WHERE id = %s", (row_id,))
        return 1

    def delete_latest(self, limit: int) -> int:
        ids = [getattr(r, "id") for r in self.session.execute("SELECT id FROM benchmark_ops LIMIT %s", (limit,))]
        for rid in ids:
            self.session.execute("DELETE FROM benchmark_ops WHERE id = %s", (rid,))
        return len(ids)

    def delete_contains(self, needle: str, limit: int) -> int:
        rows = list(self.session.execute("SELECT id, payload FROM benchmark_ops LIMIT 5000"))
        deleted = 0
        for row in rows:
            payload = getattr(row, "payload", "") or ""
            if payload.startswith(needle):
                self.session.execute("DELETE FROM benchmark_ops WHERE id = %s", (getattr(row, "id"),))
                deleted += 1
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
