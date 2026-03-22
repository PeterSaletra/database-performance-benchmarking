import os
import sys
from dataclasses import dataclass

import mysql.connector
import psycopg
from pymongo import MongoClient


@dataclass
class CheckResult:
    name: str
    success: bool
    details: str


def check_postgres() -> CheckResult:
    try:
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "benchmark_db"),
            user=os.getenv("POSTGRES_USER", "benchmark_user"),
            password=os.getenv("POSTGRES_PASSWORD", "benchmark_pass"),
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
        return CheckResult("PostgreSQL", True, version)
    except Exception as exc:
        return CheckResult("PostgreSQL", False, str(exc))


def check_mysql() -> CheckResult:
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "benchmark_db"),
            user=os.getenv("MYSQL_USER", "benchmark_user"),
            password=os.getenv("MYSQL_PASSWORD", "benchmark_pass"),
            connection_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION();")
            version = cur.fetchone()[0]
        conn.close()
        return CheckResult("MySQL", True, version)
    except Exception as exc:
        return CheckResult("MySQL", False, str(exc))


def check_mongo() -> CheckResult:
    try:
        user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "benchmark_user")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "benchmark_pass")
        host = os.getenv("MONGO_HOST", "localhost")
        port = int(os.getenv("MONGO_PORT", "27017"))
        client = MongoClient(
            f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin",
            serverSelectionTimeoutMS=5000,
        )
        client.admin.command("ping")
        version = client.server_info().get("version", "unknown")
        client.close()
        return CheckResult("MongoDB", True, version)
    except Exception as exc:
        return CheckResult("MongoDB", False, str(exc))


def check_scylla() -> CheckResult:
    try:
        try:
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
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "Missing dependency 'cassandra-driver'. ScyllaDB connectivity checks require Python 3.11/3.12 on Windows (Python 3.13+ is commonly unsupported)."
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

        host = os.getenv("SCYLLA_HOST", "localhost")
        port = int(os.getenv("SCYLLA_PORT", "9042"))

        kwargs = {}
        if connection_class is not None:
            kwargs["connection_class"] = connection_class

        cluster = Cluster([host], port=port, **kwargs)
        session = cluster.connect()
        row = session.execute("SELECT release_version FROM system.local;").one()
        version = getattr(row, "release_version", None) or "unknown"
        cluster.shutdown()
        return CheckResult("ScyllaDB", True, version)
    except Exception as exc:
        return CheckResult("ScyllaDB", False, str(exc))


def main() -> int:
    checks = [check_postgres(), check_mysql(), check_mongo(), check_scylla()]

    print("=== Database Connectivity Check ===")
    for result in checks:
        status = "OK" if result.success else "FAIL"
        print(f"[{status}] {result.name}: {result.details}")

    failed = [check for check in checks if not check.success]
    if failed:
        print(f"\n{len(failed)} service(s) failed.")
        return 1

    print("\nAll services are reachable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
