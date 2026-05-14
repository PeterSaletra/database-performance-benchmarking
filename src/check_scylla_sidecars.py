import os
import sys
from cassandra.cluster import Cluster


def main() -> int:
    host = os.getenv("SCYLLA_HOST", "localhost")
    port = int(os.getenv("SCYLLA_PORT", "9042"))
    keyspace = os.getenv("SCYLLA_KEYSPACE", "benchmark_db")
    try:
        cluster = Cluster([host], port=port)
        session = cluster.connect()
        session.set_keyspace(keyspace)
    except Exception as exc:
        print("Error connecting to Scylla:", exc)
        return 2

    tables = [
        "scylla_orders_customer_id",
        "scylla_orders_store_id",
        "scylla_orders_order_date",
        "scylla_orders_promotion_id",
    ]

    for t in tables:
        try:
            row = session.execute(f"SELECT count(*) as c FROM {t}").one()
            cnt = getattr(row, "c", None)
            print(f"{t}: count={cnt}")
            sample = session.execute(f"SELECT * FROM {t} LIMIT 3")
            for r in sample:
                print("  sample:", r)
        except Exception as exc:
            print(f"{t}: error ->", exc)

    # show small sample from retail_orders
    try:
        print("\nretail_orders sample:")
        for r in session.execute("SELECT id, data FROM retail_orders LIMIT 3"):
            print(" ", r)
    except Exception:
        pass

    cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
