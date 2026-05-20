from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate benchmark charts from CSV results.")

    input_group = parser.add_mutually_exclusive_group(required=False)
    input_group.add_argument(
        "--input",
        required=False,
        help="Path to benchmark CSV file produced by src/run_benchmarks.py",
    )
    input_group.add_argument(
        "--all",
        action="store_true",
        help="Generate plots for all benchmark_*.csv files found in --input-dir.",
    )

    parser.add_argument(
        "--input-dir",
        default="data/results",
        help="Directory to scan for benchmark_*.csv when using --all (default: data/results).",
    )
    parser.add_argument(
        "--no-scylla",
        action="store_true",
        help="Exclude Scylla (db_engine=scylla) from charts.",
    )
    parser.add_argument(
        "--pairs",
        action="store_true",
        help="Generate separate pair charts: mongo+scylla and postgres+mysql.",
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


def _filter_scylla(df: pd.DataFrame, exclude_scylla: bool) -> pd.DataFrame:
    if not exclude_scylla:
        return df
    if "db_engine" not in df.columns:
        raise RuntimeError("Input CSV does not contain required column: db_engine")
    return df[df["db_engine"].astype(str).str.lower() != "scylla"].copy()


def _pair_slices(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    if "db_engine" not in df.columns:
        raise RuntimeError("Input CSV does not contain required column: db_engine")

    normalized = df.copy()
    normalized["db_engine"] = normalized["db_engine"].astype(str).str.lower()

    pair_definitions: list[tuple[str, set[str]]] = [
        ("mongo_scylla", {"mongo", "scylla"}),
        ("postgres_mysql", {"postgres", "mysql"}),
    ]

    out: list[tuple[str, pd.DataFrame]] = []
    for pair_name, engines in pair_definitions:
        pair_df = normalized[normalized["db_engine"].isin(engines)].copy()
        if not pair_df.empty:
            out.append((pair_name, pair_df))
    return out


def _generate_plots_for_input(csv_path: Path, out_dir: Path, exclude_scylla: bool, pairs: bool) -> list[Path]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {csv_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        raise RuntimeError(f"Input CSV is empty: {csv_path}")

    df = _filter_scylla(df, exclude_scylla)
    if df.empty:
        raise RuntimeError(f"No rows left after filtering (no-scylla): {csv_path}")

    base_suffix = "_no_scylla" if exclude_scylla else ""
    slices: list[tuple[str, pd.DataFrame]]
    if pairs:
        slices = _pair_slices(df)
        if not slices:
            raise RuntimeError(f"No rows left for pair charts after filtering: {csv_path}")
    else:
        slices = [("all", df)]

    generated: list[Path] = []
    for pair_name, plot_df in slices:
        agg_scenario = (
            plot_df.groupby(["db_engine", "scenario_id", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "avg_latency_ms"})
        )

        agg_operation = (
            plot_df.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "avg_latency_ms"})
        )

        pair_suffix = f"_{pair_name}" if pair_name != "all" else ""
        scenario_plot = out_dir / f"{csv_path.stem}{base_suffix}{pair_suffix}_scenarios.png"
        operation_plot = out_dir / f"{csv_path.stem}{base_suffix}{pair_suffix}_operations.png"

        pair_title_suffix = "" if pair_name == "all" else f" ({pair_name.replace('_', ' vs ')})"
        _save_plot(
            df=agg_scenario,
            out_path=scenario_plot,
            title=f"Avg latency by scenario and database{pair_title_suffix}",
            x_col="scenario_id",
            y_col="avg_latency_ms",
            hue_col="db_engine",
        )

        _save_plot(
            df=agg_operation,
            out_path=operation_plot,
            title=f"Avg latency by CRUD operation and database{pair_title_suffix}",
            x_col="operation",
            y_col="avg_latency_ms",
            hue_col="db_engine",
        )

        generated.extend([scenario_plot, operation_plot])

    return generated


def _compare_runs(
    baseline_path: Path,
    after_path: Path,
    out_dir: Path,
    exclude_scylla: bool,
    pairs: bool,
) -> list[Path]:
    base_df = pd.read_csv(baseline_path)
    after_df = pd.read_csv(after_path)

    base_df = _filter_scylla(base_df, exclude_scylla)
    after_df = _filter_scylla(after_df, exclude_scylla)

    if base_df.empty or after_df.empty:
        raise RuntimeError("No data left after filtering (no-scylla) for comparison inputs.")

    slices: list[tuple[str, pd.DataFrame, pd.DataFrame]]
    if pairs:
        base_pairs = dict(_pair_slices(base_df))
        after_pairs = dict(_pair_slices(after_df))
        slices = []
        for pair_name in ("mongo_scylla", "postgres_mysql"):
            base_pair = base_pairs.get(pair_name)
            after_pair = after_pairs.get(pair_name)
            if base_pair is not None and after_pair is not None and not base_pair.empty and not after_pair.empty:
                slices.append((pair_name, base_pair, after_pair))
        if not slices:
            raise RuntimeError("No data left for pair comparison after filtering inputs.")
    else:
        slices = [("all", base_df, after_df)]

    suffix = "_no_scylla" if exclude_scylla else ""
    generated: list[Path] = []
    for pair_name, base_part, after_part in slices:
        base_agg = (
            base_part.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "baseline_latency_ms"})
        )
        after_agg = (
            after_part.groupby(["db_engine", "operation"], as_index=False)["latency_ms"]
            .mean()
            .rename(columns={"latency_ms": "after_latency_ms"})
        )

        merged = base_agg.merge(after_agg, on=["db_engine", "operation"], how="inner")
        merged["delta_ms"] = merged["after_latency_ms"] - merged["baseline_latency_ms"]
        merged["improvement_pct"] = (
            (merged["baseline_latency_ms"] - merged["after_latency_ms"]) / merged["baseline_latency_ms"]
        ) * 100.0
        merged["db_operation"] = merged["db_engine"] + "_" + merged["operation"]

        pair_suffix = f"_{pair_name}" if pair_name != "all" else ""
        cmp_csv = out_dir / f"compare{suffix}{pair_suffix}_{baseline_path.stem}_vs_{after_path.stem}.csv"
        merged.to_csv(cmp_csv, index=False)

        cmp_delta_png = out_dir / f"compare{suffix}{pair_suffix}_{baseline_path.stem}_vs_{after_path.stem}_delta_ms.png"
        cmp_pct_png = out_dir / f"compare{suffix}{pair_suffix}_{baseline_path.stem}_vs_{after_path.stem}_improvement_pct.png"

        pair_title_suffix = "" if pair_name == "all" else f" ({pair_name.replace('_', ' vs ')})"
        _save_comparison_plot(
            df=merged.sort_values("delta_ms"),
            out_path=cmp_delta_png,
            title=f"After-index minus baseline latency [ms] by DB/operation{pair_title_suffix}",
            y_col="delta_ms",
        )

        _save_comparison_plot(
            df=merged.sort_values("improvement_pct", ascending=False),
            out_path=cmp_pct_png,
            title=f"Improvement [%] after indexes by DB/operation{pair_title_suffix}",
            y_col="improvement_pct",
        )

        generated.extend([cmp_csv, cmp_delta_png, cmp_pct_png])

    return generated


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    generated: list[Path] = []

    if args.input:
        generated.extend(
            _generate_plots_for_input(Path(args.input), out_dir, exclude_scylla=args.no_scylla, pairs=args.pairs)
        )

    if args.all:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

        csv_paths = sorted(input_dir.glob("benchmark_*.csv"))
        if not csv_paths:
            raise RuntimeError(f"No benchmark_*.csv files found in: {input_dir}")

        for csv_path in csv_paths:
            generated.extend(_generate_plots_for_input(csv_path, out_dir, exclude_scylla=args.no_scylla, pairs=args.pairs))

    if args.baseline_input and args.after_input:
        baseline_path = Path(args.baseline_input)
        after_path = Path(args.after_input)
        if not baseline_path.exists():
            raise FileNotFoundError(f"Baseline CSV does not exist: {baseline_path}")
        if not after_path.exists():
            raise FileNotFoundError(f"After-index CSV does not exist: {after_path}")
        generated.extend(
            _compare_runs(
                baseline_path,
                after_path,
                out_dir,
                exclude_scylla=args.no_scylla,
                pairs=args.pairs,
            )
        )

    if not generated:
        raise RuntimeError(
            "No output generated. Provide --input or --all and/or --baseline-input with --after-input."
        )

    print("Generated artifacts:")
    for path in generated:
        print(f"- {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
