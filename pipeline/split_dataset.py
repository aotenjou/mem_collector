#!/usr/bin/env python3

import argparse
import json
import os


def parse_args():
    parser = argparse.ArgumentParser(description="Split JSONL dataset by time order")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--time-field", default="collected_at")
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--val-ratio", type=float, default=0.15)
    return parser.parse_args()


def load_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def dump_rows(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    args = parse_args()
    rows = load_rows(args.input)
    rows.sort(key=lambda item: item.get(args.time_field, ""))

    total = len(rows)
    train_end = int(total * args.train_ratio)
    val_end = int(total * (args.train_ratio + args.val_ratio))

    train_rows = rows[:train_end]
    val_rows = rows[train_end:val_end]
    test_rows = rows[val_end:]

    os.makedirs(args.output_dir, exist_ok=True)
    dump_rows(os.path.join(args.output_dir, "train.jsonl"), train_rows)
    dump_rows(os.path.join(args.output_dir, "val.jsonl"), val_rows)
    dump_rows(os.path.join(args.output_dir, "test.jsonl"), test_rows)

    print(
        f"split {total} rows -> train={len(train_rows)} val={len(val_rows)} test={len(test_rows)} in {args.output_dir}"
    )


if __name__ == "__main__":
    main()
