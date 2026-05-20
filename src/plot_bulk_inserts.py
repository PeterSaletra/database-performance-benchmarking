"""Plot bulk insert benchmark results saved as JSON.

Usage:
  python src/plot_bulk_inserts.py --input-dir data/results --output-dir plots
"""
import os
import json
import argparse
from glob import glob
import matplotlib.pyplot as plt


def load_results(input_dir):
    files = glob(os.path.join(input_dir, "bulk_insert_*.json"))
    rows = []
    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                j = json.load(fh)
                for r in j.get("rows", []):
                    rows.append(r)
        except Exception:
            continue
    return rows


def aggregate(rows):
    # key: (engine, mode) -> time_s (take last if multiple, skip None/errors)
    agg = {}
    for r in rows:
        key = (r.get("engine"), r.get("mode"))
        time_s = r.get("time_s")
        # Only include valid results (skip errors/skipped with None times)
        if time_s is not None:
            agg[key] = time_s
    return agg


def plot(agg, out_dir):
    ensure_dir = os.makedirs
    ensure_dir(out_dir, exist_ok=True)
    engines = sorted({k[0] for k in agg.keys()})
    modes = sorted({k[1] for k in agg.keys()})

    labels = []
    data = {mode: [] for mode in modes}
    for engine in engines:
        labels.append(engine)
        for mode in modes:
            data[mode].append(agg.get((engine, mode), None))

    x = range(len(engines))
    width = 0.35
    fig, ax = plt.subplots()
    offsets = [(-width/2, width/2), (-width, width), (-1.5*width, width)]
    for i, mode in enumerate(modes):
        vals = data[mode]
        ax.bar([p + i * width for p in x], vals, width, label=mode)

    ax.set_xticks([p + width*(len(modes)-1)/2 for p in x])
    ax.set_xticklabels(labels)
    ax.set_ylabel('insert time (s)')
    ax.set_title('Bulk insert: engines (no_index vs with_index)')
    ax.legend()
    out_path = os.path.join(out_dir, 'bulk_insert_comparison.png')
    fig.tight_layout()
    fig.savefig(out_path)
    print('Saved plot to', out_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/results")
    parser.add_argument("--output-dir", default="plots")
    args = parser.parse_args()

    rows = load_results(args.input_dir)
    if not rows:
        print("No result files found in", args.input_dir)
        return
    agg = aggregate(rows)
    plot(agg, args.output_dir)


if __name__ == '__main__':
    main()
