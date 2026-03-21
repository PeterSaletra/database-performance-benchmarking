import argparse
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import mysql.connector
import psycopg
import redis
from faker import Faker
from pymongo import MongoClient


@dataclass
class UserRecord:
    user_id: int
    full_name: str
    email: str
    city: str
    created_at: datetime


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


def generate_users(total_rows: int, seed: int) -> list[UserRecord]:
    faker = Faker("pl_PL")
    faker.seed_instance(seed)
    random.seed(seed)

    users: list[UserRecord] = []
    for i in range(1, total_rows + 1):
        users.append(
            UserRecord(
                user_id=i,
                full_name=faker.name(),
                email=f"user{i}@example.com",
                city=faker.city(),
                created_at=faker.date_time_between(start_date="-3y", end_date="now"),
            )
        )
    return users


def chunked(records: list[UserRecord], chunk_size: int) -> Iterable[list[UserRecord]]:
    for i in range(0, len(records), chunk_size):
        yield records[i : i + chunk_size]


def import_postgres(records: list[UserRecord], batch_size: int, reset: bool) -> None:
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "benchmark_db"),
        user=os.getenv("POSTGRES_USER", "benchmark_user"),
        password=os.getenv("POSTGRES_PASSWORD", "benchmark_pass"),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS benchmark_users (
                    user_id BIGINT PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    city TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );
                """
            )
            if reset:
                cur.execute("TRUNCATE TABLE benchmark_users;")

            for batch in chunked(records, batch_size):
                cur.executemany(
                    """
                    INSERT INTO benchmark_users (user_id, full_name, email, city, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET full_name = EXCLUDED.full_name,
                        email = EXCLUDED.email,
                        city = EXCLUDED.city,
                        created_at = EXCLUDED.created_at;
                    """,
                    [
                        (r.user_id, r.full_name, r.email, r.city, r.created_at)
                        for r in batch
                    ],
                )
        conn.commit()
    finally:
        conn.close()


def import_mysql(records: list[UserRecord], batch_size: int, reset: bool) -> None:
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "benchmark_db"),
        user=os.getenv("MYSQL_USER", "benchmark_user"),
        password=os.getenv("MYSQL_PASSWORD", "benchmark_pass"),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS benchmark_users (
                    user_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    city VARCHAR(255) NOT NULL,
                    created_at DATETIME NOT NULL
                );
                """
            )
            if reset:
                cur.execute("TRUNCATE TABLE benchmark_users;")

            for batch in chunked(records, batch_size):
                cur.executemany(
                    """
                    INSERT INTO benchmark_users (user_id, full_name, email, city, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        full_name = VALUES(full_name),
                        email = VALUES(email),
                        city = VALUES(city),
                        created_at = VALUES(created_at);
                    """,
                    [
                        (r.user_id, r.full_name, r.email, r.city, r.created_at)
                        for r in batch
                    ],
                )
        conn.commit()
    finally:
        conn.close()


def import_mongo(records: list[UserRecord], batch_size: int, reset: bool) -> None:
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
        collection = client[db_name]["benchmark_users"]
        if reset:
            collection.drop()

        for batch in chunked(records, batch_size):
            docs = [
                {
                    "_id": r.user_id,
                    "full_name": r.full_name,
                    "email": r.email,
                    "city": r.city,
                    "created_at": r.created_at,
                }
                for r in batch
            ]
            collection.insert_many(docs, ordered=False)
    finally:
        client.close()


def import_redis(records: list[UserRecord], reset: bool) -> None:
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )

    try:
        if reset:
            keys = client.keys("benchmark_users:*")
            if keys:
                client.delete(*keys)

        pipe = client.pipeline(transaction=False)
        for r in records:
            key = f"benchmark_users:{r.user_id}"
            pipe.hset(
                key,
                mapping={
                    "user_id": str(r.user_id),
                    "full_name": r.full_name,
                    "email": r.email,
                    "city": r.city,
                    "created_at": r.created_at.isoformat(),
                },
            )
        pipe.execute()
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import synthetic benchmark data into PostgreSQL, MySQL, MongoDB and Redis."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10_000,
        help="Number of records to generate and import (default: 10000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2_000,
        help="Batch size for SQL and Mongo imports (default: 2000).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed for deterministic data generation (default: 42).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Remove existing benchmark data before import.",
    )
    return parser.parse_args()


def main() -> int:
    load_env_file(Path(".env"))
    args = parse_args()

    if args.rows <= 0:
        print("--rows must be greater than 0")
        return 2
    if args.batch_size <= 0:
        print("--batch-size must be greater than 0")
        return 2

    print(
        f"Generating {args.rows} records (seed={args.seed}) and importing with batch_size={args.batch_size}..."
    )
    records = generate_users(args.rows, args.seed)

    try:
        start = time.perf_counter()
        import_postgres(records, args.batch_size, args.reset)
        print(f"[OK] PostgreSQL import finished in {time.perf_counter() - start:.2f}s")

        start = time.perf_counter()
        import_mysql(records, args.batch_size, args.reset)
        print(f"[OK] MySQL import finished in {time.perf_counter() - start:.2f}s")

        start = time.perf_counter()
        import_mongo(records, args.batch_size, args.reset)
        print(f"[OK] MongoDB import finished in {time.perf_counter() - start:.2f}s")

        start = time.perf_counter()
        import_redis(records, args.reset)
        print(f"[OK] Redis import finished in {time.perf_counter() - start:.2f}s")
    except Exception as exc:  # noqa: BLE001
        print(f"Import failed: {exc}")
        return 1

    print("All imports finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
