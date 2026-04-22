from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate benchmark charts from CSV results.")
    parser.add_argument(
        "--input",
        required=False,
        help="Path to benchmark CSV file produced by src/run_benchmarks.py",
    )
    parser.add_argument(
        "--baseline-input",
        required=False,
        help="Path to baseline benchmark CSV.",
    )
    parser.add_argument(
        "--after-input",
        required=False,
        help="Path to after-index benchmark CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default="plots",
        help="Directory where plots will be saved (default: plots).",
    )
    return parser.parse_args()


def _save_plot(df: pd.DataFrame, out_path: Path, title: str, x_col: str, y_col: str, hue_col: str) -> None:
    pivot = df.pivot(index=x_col, columns=hue_col, values=y_col)

    fig, ax = plt.subplots(figsize=(12, 6))
    pivot.plot(kind="bar", ax=ax)
    ax.set_title(title)
    ax.set_xlabel(x_col)
    ax.set_ylabel("Avg latency [ms]")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend(title=hue_col)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _save_comparison_plot(df: pd.DataFrame, out_path: Path, title: str, y_col: str) -> None:
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.bar(df["db_operation"], df[y_col])
    ax.set_title(title)
    ax.set_xlabel("db_operation")
    ax.set_ylabel(y_col)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.xticks(rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _compare_runs(baseline_path: Path, after_path: Path, out_dir: Path) -> list[Path]:
    base_df = pd.read_csv(baseline_path)
    after_df = pd.read_csv(after_path)

    base_agg = (
        base_df.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
        .mean()
        .rename(columns={"latency_ms": "baseline_latency_ms"})
    )
    after_agg = (
        after_df.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
        .mean()
        .rename(columns={"latency_ms": "after_latency_ms"})
    )

    merged = base_agg.merge(after_agg, on=["db_engine", "operation"], how="inner")
    merged["delta_ms"] = merged["after_latency_ms"] - merged["baseline_latency_ms"]
    merged["improvement_pct"] = (
        (merged["baseline_latency_ms"] - merged["after_latency_ms"]) / merged["baseline_latency_ms"]
    ) * 100.0
    merged["db_operation"] = merged["db_engine"] + "_" + merged["operation"]

    cmp_csv = out_dir / f"compare_{baseline_path.stem}_vs_{after_path.stem}.csv"
    merged.to_csv(cmp_csv, index=False)

    cmp_delta_png = out_dir / f"compare_{baseline_path.stem}_vs_{after_path.stem}_delta_ms.png"
    cmp_pct_png = out_dir / f"compare_{baseline_path.stem}_vs_{after_path.stem}_improvement_pct.png"

    _save_comparison_plot(
        df=merged.sort_values("delta_ms"),
        out_path=cmp_delta_png,
        title="After-index minus baseline latency [ms] by DB/operation",
        y_col="delta_ms",
    )

    _save_comparison_plot(
        df=merged.sort_values("improvement_pct", ascending=False),
        out_path=cmp_pct_png,
        title="Improvement [%] after indexes by DB/operation",
        y_col="improvement_pct",
    )

    return [cmp_csv, cmp_delta_png, cmp_pct_png]


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    generated: list[Path] = []

    if args.input:
        csv_path = Path(args.input)
        if not csv_path.exists():
            raise FileNotFoundError(f"Input CSV does not exist: {csv_path}")

        df = pd.read_csv(csv_path)
        if df.empty:
            raise RuntimeError("Input CSV is empty.")

        agg_scenario = (
            df.groupby(["db_engine", "scenario_id", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "avg_latency_ms"})
        )

        agg_operation = (
            df.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "avg_latency_ms"})
        )

        scenario_plot = out_dir / f"{csv_path.stem}_scenarios.png"
        operation_plot = out_dir / f"{csv_path.stem}_operations.png"

        _save_plot(
            df=agg_scenario,
            out_path=scenario_plot,
            title="Avg latency by scenario and database",
            x_col="scenario_id",
            y_col="avg_latency_ms",
            hue_col="db_engine",
        )

        _save_plot(
            df=agg_operation,
            out_path=operation_plot,
            title="Avg latency by CRUD operation and database",
            x_col="operation",
            y_col="avg_latency_ms",
            hue_col="db_engine",
        )
        generated.extend([scenario_plot, operation_plot])

    if args.baseline_input and args.after_input:
        baseline_path = Path(args.baseline_input)
        after_path = Path(args.after_input)
        if not baseline_path.exists():
            raise FileNotFoundError(f"Baseline CSV does not exist: {baseline_path}")
        if not after_path.exists():
            raise FileNotFoundError(f"After-index CSV does not exist: {after_path}")
        generated.extend(_compare_runs(baseline_path, after_path, out_dir))

    if not generated:
        raise RuntimeError("No output generated. Provide --input and/or --baseline-input with --after-input.")

    print("Generated artifacts:")
    for path in generated:
        print(f"- {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
