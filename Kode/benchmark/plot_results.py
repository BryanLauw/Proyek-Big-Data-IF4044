#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict


def load_summary(path: str):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def plot_summary(rows, output_path):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed. Run: pip install matplotlib")
        return

    by_query = defaultdict(dict)
    for row in rows:
        q = row["query"]
        by_query[q]["csv"] = to_float(row.get("csv_avg_sec"))
        by_query[q]["iceberg"] = to_float(row.get("iceberg_avg_sec"))

    queries = list(by_query.keys())
    csv_vals = [by_query[q].get("csv") for q in queries]
    ice_vals = [by_query[q].get("iceberg") for q in queries]

    x = list(range(len(queries)))
    width = 0.35

    plt.figure(figsize=(10, 5))
    plt.bar([i - width / 2 for i in x], csv_vals, width=width, label="CSV")
    plt.bar([i + width / 2 for i in x], ice_vals, width=width, label="Iceberg")
    plt.xticks(x, queries, rotation=15)
    plt.ylabel("Average time (sec)")
    plt.title("Benchmark CSV vs Iceberg")
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path)
    print(f"Saved plot: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark summary CSV")
    parser.add_argument("--input", required=True, help="Path to summary CSV")
    parser.add_argument("--output", required=True, help="Output PNG path")
    args = parser.parse_args()

    rows = load_summary(args.input)
    plot_summary(rows, args.output)


if __name__ == "__main__":
    main()
