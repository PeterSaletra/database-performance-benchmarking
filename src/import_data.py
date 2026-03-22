import argparse
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Import the Kaggle Retail Data Warehouse dataset into PostgreSQL, MySQL, MongoDB and Redis (sequentially)."
        )
    )
    parser.add_argument(
        "--dataset-id",
        default=None,
        help="Kaggle dataset id (default: retail DWH 12-table dataset).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop/recreate tables/collections/keys before import.",
    )
    parser.add_argument(
        "--orders-target-rows",
        type=int,
        default=9_000_000,
        help="Target number of rows in orders table/collection (default: 9000000).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size for CSV chunking (used by MySQL/Mongo/Redis; default: 10000).",
    )
    parser.add_argument(
        "--nosql-mode",
        choices=["tables", "denormalized"],
        default="denormalized",
        help="Mongo/Redis import mode (default: denormalized).",
    )
    return parser.parse_args()


def run_step(label: str, script_path: Path, script_args: list[str]) -> None:
    cmd = [sys.executable, str(script_path), *script_args]
    print(f"\n=== {label} ===")
    print("Running:", " ".join(cmd))
    start = time.perf_counter()
    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0:
        if proc.returncode == 3221225477:
            raise RuntimeError(
                "Process crashed with Windows access violation (3221225477). "
                "This is commonly caused by using Python 3.14 with a broken/experimental numpy build. "
                "Recreate .venv with Python 3.11 or 3.12 and reinstall requirements."
            )
        raise RuntimeError(f"{label} failed with exit code {proc.returncode}")
    print(f"[OK] {label} finished in {time.perf_counter() - start:.2f}s")


def main() -> int:
    args = parse_args()

    src_dir = Path(__file__).resolve().parent
    steps: list[tuple[str, Path, list[str]]] = []

    common_args: list[str] = [
        "--orders-target-rows",
        str(args.orders_target_rows),
    ]
    if args.reset:
        common_args.append("--reset")
    if args.dataset_id:
        common_args.extend(["--dataset-id", args.dataset_id])

    # steps.append(("PostgreSQL import", src_dir / "import_retail_postgres.py", common_args))

    # steps.append(
    #     (
    #         "MySQL import",
    #         src_dir / "import_retail_mysql.py",
    #         [*common_args, "--batch-size", str(args.batch_size)],
    #     )
    # )
    # steps.append(
    #     (
    #         "MongoDB import",
    #         src_dir / "import_retail_mongo.py",
    #         [*common_args, "--batch-size", str(args.batch_size), "--mode", args.nosql_mode],
    #     )
    # )
    steps.append(
        (
            "Redis import",
            src_dir / "import_retail_redis.py",
            [*common_args, "--batch-size", str(args.batch_size), "--mode", args.nosql_mode],
        )
    )

    try:
        for label, script, s_args in steps:
            if not script.exists():
                print(f"Missing script: {script}")
                return 2
            run_step(label, script, s_args)
    except Exception as exc:
        print(f"Import pipeline failed: {exc}")
        return 1

    print("\nAll imports finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
