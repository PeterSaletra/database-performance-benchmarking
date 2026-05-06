from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from retail_client import RetailDBClient, retail_client_factory
from retail_scenarios import build_all_scenarios


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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
        "--size-label",
        default="mvp",
        help="Logical data size label saved in results.",
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
        help="Save EXPLAIN samples to a JSON file.",
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
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return csv_path, json_path


def _print_summary(results: list[TrialResult]) -> None:
    if not results:
        print("\nNo benchmark rows were recorded.")
        return

    grouped: dict[tuple[str, str], list[float]] = {}
    for row in results:
        grouped.setdefault((row.db_engine, row.operation), []).append(row.latency_ms)

    print("\n=== Summary (avg latency ms by DB/operation) ===")
    for (db, op), vals in sorted(grouped.items()):
        avg = statistics.mean(vals)
        print(f"{db:10s} {op:7s} avg={avg:9.2f} ms n={len(vals)}")


def _prepare_context(client: RetailDBClient) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "customer_id": client.sample_customer_id(),
        "product_id": client.sample_product_id(),
        "order_id": client.sample_order_id(),
        "order_item_id": client.sample_order_item_id(),
        "payment_id": client.sample_payment_id(),
        "shipment_id": client.sample_shipment_id(),
    }

    if ctx["order_id"] is None:
        ctx["order_id"] = ctx["customer_id"]
    if ctx["order_item_id"] is None:
        ctx["order_item_id"] = ctx["order_id"]
    if ctx["payment_id"] is None:
        ctx["payment_id"] = ctx["order_id"]
    if ctx["shipment_id"] is None:
        ctx["shipment_id"] = ctx["order_id"]
    return ctx


def main() -> int:
    args = parse_args()
    _load_env_file(Path(args.env_file))

    dbs = args.db
    if "all" in dbs:
        dbs = ["postgres", "mysql", "mongo", "scylla"]

    scenarios = build_all_scenarios()
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
    print(f"Size Label: {args.size_label}")

    for db in dbs:
        print(f"\n--- DB: {db} ---")
        client: RetailDBClient | None = None
        try:
            client = retail_client_factory(db)
            client.setup()
            client.configure_mode(args.mode)
            ctx = _prepare_context(client)

            print(
                "  Sample IDs: "
                + ", ".join(f"{key}={value}" for key, value in ctx.items()),
                flush=True,
            )

            for scenario in scenarios:
                for trial in range(1, args.trials + 1):
                    try:
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
                            rows_affected=int(affected),
                            data_size_label=args.size_label,
                            mode=args.mode,
                            timestamp_utc=_utc_now().isoformat(),
                        )
                        results.append(result)
                        print(
                            f"  [{db}] {scenario.scenario_id} trial={trial} latency={result.latency_ms:.3f}ms rows={affected}",
                            flush=True,
                        )
                    except Exception as exc:
                        print(f"  [{db}] {scenario.scenario_id} trial={trial} ERROR: {exc}", flush=True)
        except Exception as exc:
            print(f"  Error connecting to {db}: {exc}")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        if args.save_explain and client is not None:
            try:
                explain_payload["databases"][db] = client.explain_samples()
            except Exception as exc:
                explain_payload["databases"][db] = {"error": str(exc)}

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
