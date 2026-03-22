import os
import sys
from dataclasses import dataclass

import mysql.connector
import psycopg
import redis
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


def check_redis() -> CheckResult:
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            decode_responses=True,
            socket_connect_timeout=5,
        )
        pong = client.ping()
        client.close()
        return CheckResult("Redis", bool(pong), "PING OK" if pong else "PING failed")
    except Exception as exc:
        return CheckResult("Redis", False, str(exc))


def main() -> int:
    checks = [check_postgres(), check_mysql(), check_mongo(), check_redis()]

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
