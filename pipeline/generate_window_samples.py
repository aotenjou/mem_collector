#!/usr/bin/env python3

import argparse
import os
from contextlib import closing
from datetime import datetime

import psycopg2


def parse_args():
    parser = argparse.ArgumentParser(description="Generate manual mem_window_samples from query samples")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="")
    parser.add_argument("--window-file", required=True, help="Each line: window_start_iso,window_seconds")
    return parser.parse_args()


def parse_window(line):
    parts = [part.strip() for part in line.split(",")]
    if len(parts) != 2:
        raise ValueError(f"invalid window definition: {line}")
    datetime.fromisoformat(parts[0])
    return parts[0], int(parts[1])


def main():
    args = parse_args()

    with open(args.window_file, "r", encoding="utf-8") as fh:
        windows = [parse_window(line.strip()) for line in fh if line.strip()]

    with closing(
        psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for window_start, window_seconds in windows:
                cur.execute(
                    "SELECT mem_collector_generate_window_sample(%s::timestamptz, %s)",
                    (window_start, window_seconds),
                )

    print(f"generated {len(windows)} window samples from {os.path.basename(args.window_file)}")


if __name__ == "__main__":
    main()
