#!/usr/bin/env python3

import argparse
import json


RATIO_FIELDS = [
    "max_sort_ratio",
    "max_hash_ratio",
    "max_hashagg_ratio",
    "parallel_ratio",
    "peak_mem_ratio",
    "spill_rate_recent",
    "ap_max_predicted_mem_ratio",
    "tp_concurrency_ratio",
    "dynamic_pool_current_ratio",
    "dynamic_pool_usage_ratio",
    "shared_pool_usage_ratio",
    "target_dynamic_pool_ratio",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Filter query- or window-level samples")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=["query", "window"], default="query")
    return parser.parse_args()


def is_ratio_valid(sample, field):
    value = sample.get(field)
    if value is None:
        return True
    return 0 <= float(value) <= 1


def keep_query_sample(sample):
    if float(sample.get("estimation_error", 0) or 0) >= 10:
        return False
    if float(sample.get("peak_mem_ratio", 0) or 0) <= 0:
        return False
    if not bool(sample.get("is_ap", False)):
        return False
    return all(is_ratio_valid(sample, field) for field in RATIO_FIELDS)


def keep_window_sample(sample):
    if int(sample.get("ap_count", 0) or 0) <= 0:
        return False
    return all(is_ratio_valid(sample, field) for field in RATIO_FIELDS)


def main():
    args = parse_args()
    kept = 0
    total = 0
    predicate = keep_query_sample if args.mode == "query" else keep_window_sample

    with open(args.input, "r", encoding="utf-8") as src, open(args.output, "w", encoding="utf-8") as dst:
        for line in src:
            if not line.strip():
                continue
            total += 1
            sample = json.loads(line)
            if predicate(sample):
                dst.write(json.dumps(sample, ensure_ascii=False) + "\n")
                kept += 1

    print(f"kept {kept}/{total} rows into {args.output}")


if __name__ == "__main__":
    main()
