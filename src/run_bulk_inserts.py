"""Run bulk insert benchmarks for multiple databases.

Usage examples:
  python src/run_bulk_inserts.py --rows 1000000 --engines postgres mysql mongo scylla --output-dir data/results

Configuration:
- The script reads connection info from environment variables. See per-engine defaults in code.
- For safety during development, override `--rows` to a smaller number.

This script creates a fresh table/collection for each run and (optionally) creates indexes.
It records timing results to JSON files under the specified `--output-dir`.
"""
import os
import time
import json
import argparse
import importlib
from datetime import datetime
from faker import Faker

fake = Faker()


def timestamp():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def gen_row(i):
    return {
        "id": i,
        "name": fake.name(),
        "price": round(fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=1, max_value=1000), 2),
        "category": fake.random_element(elements=("A", "B", "C", "D")),
    }


def run_postgres(conn_info, rows, batch_size, create_index):
    import psycopg

    results = []
    dsn = conn_info.get("dsn")
    table = conn_info.get("table", "bulk_insert_test")
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(
                f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, price NUMERIC, category TEXT)"
            )
            if create_index:
                cur.execute(f"CREATE INDEX idx_{table}_category ON {table}(category)")

            start = time.perf_counter()
            to_insert = []
            for i in range(1, rows + 1):
                r = gen_row(i)
                to_insert.append((r["id"], r["name"], r["price"], r["category"]))
                if len(to_insert) >= batch_size:
                    cur.executemany(
                        f"INSERT INTO {table} (id,name,price,category) VALUES (%s,%s,%s,%s)", to_insert
                    )
                    conn.commit()
                    to_insert.clear()
            if to_insert:
                cur.executemany(
                    f"INSERT INTO {table} (id,name,price,category) VALUES (%s,%s,%s,%s)", to_insert
                )
                conn.commit()
            end = time.perf_counter()
            results.append({"engine": "postgres", "rows": rows, "time_s": end - start, "mode": "with_index" if create_index else "no_index"})
    return results


def run_mysql(conn_info, rows, batch_size, create_index):
    import mysql.connector

    results = []
    cfg = conn_info.get("config")
    table = conn_info.get("table", "bulk_insert_test")
    cnx = mysql.connector.connect(**cfg)
    cur = cnx.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(
        f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, price DECIMAL(10,2), category VARCHAR(16))"
    )
    if create_index:
        cur.execute(f"CREATE INDEX idx_{table}_category ON {table}(category)")
    cnx.commit()

    start = time.perf_counter()
    to_insert = []
    for i in range(1, rows + 1):
        r = gen_row(i)
        to_insert.append((r["id"], r["name"], r["price"], r["category"]))
        if len(to_insert) >= batch_size:
            cur.executemany(
                f"INSERT INTO {table} (id,name,price,category) VALUES (%s,%s,%s,%s)", to_insert
            )
            cnx.commit()
            to_insert.clear()
    if to_insert:
        cur.executemany(
            f"INSERT INTO {table} (id,name,price,category) VALUES (%s,%s,%s,%s)", to_insert
        )
        cnx.commit()
    end = time.perf_counter()
    cur.close()
    cnx.close()
    results.append({"engine": "mysql", "rows": rows, "time_s": end - start, "mode": "with_index" if create_index else "no_index"})
    return results


def run_mongo(conn_info, rows, batch_size, create_index):
    from pymongo import MongoClient, InsertOne

    results = []
    uri = conn_info.get("uri")
    dbname = conn_info.get("db", "benchmark")
    coll_name = conn_info.get("collection", "bulk_insert_test")
    client = MongoClient(uri)
    db = client[dbname]
    coll = db[coll_name]
    coll.drop()
    if create_index:
        coll.create_index("category")

    start = time.perf_counter()
    ops = []
    for i in range(1, rows + 1):
        r = gen_row(i)
        ops.append(InsertOne(r))
        if len(ops) >= batch_size:
            coll.bulk_write(ops)
            ops.clear()
    if ops:
        coll.bulk_write(ops)
    end = time.perf_counter()
    results.append({"engine": "mongo", "rows": rows, "time_s": end - start, "mode": "with_index" if create_index else "no_index"})
    client.close()
    return results


def run_scylla(conn_info, rows, batch_size, create_index):
    from cassandra.cluster import Cluster

    results = []
    hosts = conn_info.get("hosts", ["127.0.0.1"]) 
    keyspace = conn_info.get("keyspace", "benchmarkks")
    table = conn_info.get("table", "bulk_insert_test")
    cluster = Cluster(hosts)
    session = cluster.connect()
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
    session.execute(f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}")
    session.set_keyspace(keyspace)
    session.execute(
        f"CREATE TABLE {table} (id bigint PRIMARY KEY, name text, price double, category text)"
    )
    if create_index:
        try:
            session.execute(f"CREATE INDEX ON {table}(category)")
        except Exception:
            pass

    insert_cql = f"INSERT INTO {table} (id,name,price,category) VALUES (?,?,?,?)"
    prepared = session.prepare(insert_cql)

    start = time.perf_counter()
    batch = []
    for i in range(1, rows + 1):
        r = gen_row(i)
        batch.append((r["id"], r["name"], r["price"], r["category"]))
        if len(batch) >= batch_size:
            for params in batch:
                session.execute(prepared, params)
            batch.clear()
    if batch:
        for params in batch:
            session.execute(prepared, params)
    end = time.perf_counter()
    session.shutdown()
    cluster.shutdown()
    results.append({"engine": "scylla", "rows": rows, "time_s": end - start, "mode": "with_index" if create_index else "no_index"})
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engines", nargs="+", default=["postgres", "mysql", "mongo", "scylla"]) 
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--output-dir", default="data/results")
    parser.add_argument("--modes", nargs="+", choices=("with_index", "no_index"), default=["no_index", "with_index"])
    args = parser.parse_args()

    ensure_dir(args.output_dir)
    run_id = timestamp()
    all_results = []

    # Connection info defaults - override via environment variables if needed
    conn_defaults = {
        "postgres": {"dsn": os.environ.get("PG_DSN", "dbname=postgres user=postgres host=127.0.0.1 password=postgres"), "table": "bulk_insert_test"},
        "mysql": {"config": {"host": os.environ.get("MYSQL_HOST", "127.0.0.1"), "user": os.environ.get("MYSQL_USER", "root"), "password": os.environ.get("MYSQL_PASSWORD", ""), "database": os.environ.get("MYSQL_DB", "test")}, "table": "bulk_insert_test"},
        "mongo": {"uri": os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017"), "db": os.environ.get("MONGO_DB", "benchmark"), "collection": "bulk_insert_test"},
        "scylla": {"hosts": [os.environ.get("SCYLLA_HOST", "127.0.0.1")], "keyspace": os.environ.get("SCYLLA_KS", "benchmarkks"), "table": "bulk_insert_test"},
    }

    engine_funcs = {
        "postgres": run_postgres,
        "mysql": run_mysql,
        "mongo": run_mongo,
        "scylla": run_scylla,
    }

    # Check for required python drivers per engine
    engine_deps = {
        "postgres": "psycopg",
        "mysql": "mysql.connector",
        "mongo": "pymongo",
        "scylla": "cassandra",
    }

    for engine in args.engines:
        conn_info = conn_defaults.get(engine, {})
        dep = engine_deps.get(engine)
        missing_dep = None
        if dep:
            try:
                importlib.import_module(dep)
            except Exception:
                missing_dep = dep

        for mode in args.modes:
            create_index = mode == "with_index"
            print(f"Running {engine} rows={args.rows} mode={mode}")
            if missing_dep:
                msg = f"skipped: missing python package '{missing_dep}' (install with pip)"
                print(msg)
                all_results.append({"engine": engine, "rows": args.rows, "time_s": None, "mode": mode, "error": msg})
                continue

            try:
                res = engine_funcs[engine](conn_info, args.rows, args.batch_size, create_index)
                for r in res:
                    r.update({"run_id": run_id, "generated_at_utc": datetime.utcnow().isoformat()})
                    all_results.append(r)
            except Exception as e:
                err_msg = str(e)
                print(f"Error running {engine} ({mode}): {err_msg}")
                all_results.append({"engine": engine, "rows": args.rows, "time_s": None, "mode": mode, "error": err_msg})

    out_path = os.path.join(args.output_dir, f"bulk_insert_{run_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "generated_at_utc": datetime.utcnow().isoformat(), "rows": all_results}, f, indent=2)
    print("Results written to", out_path)


if __name__ == "__main__":
    main()
