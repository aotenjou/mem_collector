#!/usr/bin/env python3

import argparse
import json
import os
import shlex
import subprocess
from contextlib import closing

import psycopg2
from psycopg2.extras import RealDictCursor


def parse_args():
    parser = argparse.ArgumentParser(description="Label mem_window_samples with target_dynamic_pool_ratio")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="")
    parser.add_argument("--ratio-start", type=float, default=0.1)
    parser.add_argument("--ratio-stop", type=float, default=0.95)
    parser.add_argument("--ratio-step", type=float, default=0.05)
    parser.add_argument("--set-sql", default="SELECT set_config('mem_collector.dynamic_pool_ratio', %s, false)")
    parser.add_argument("--replay-command-template", default="")
    parser.add_argument("--export-output", default="")
    parser.add_argument("--export-only-labeled", action="store_true")
    return parser.parse_args()


def iter_ratios(start, stop, step):
    current = start
    while current < stop:
        yield round(current, 6)
        current += step


def heuristic_score(window, ratio):
    spill = float(window.get("spill_rate_recent", 0) or 0)
    mem_pressure = float(window.get("ap_max_predicted_mem_ratio", 0) or 0)
    target = min(0.8, max(0.2, mem_pressure * 0.9 + spill * 0.3))
    distance = abs(ratio - target)
    return spill * 1000.0 + distance * 100.0 + float(window.get("tp_concurrency_ratio", 0) or 0) * 10.0


def replay_score(template, ratio, window_id):
    command = template.format(ratio=ratio, window_id=window_id)
    completed = subprocess.run(shlex.split(command), capture_output=True, text=True, check=True)
    payload = json.loads(completed.stdout.strip())
    return float(payload["spill_count"]) * 1000.0 + float(payload["tps_jitter_pct"])


def export_rows(conn, output_path, labeled_only):
    sql = "SELECT * FROM mem_window_samples"
    if labeled_only:
        sql += " WHERE target_dynamic_pool_ratio IS NOT NULL"
    sql += " ORDER BY window_start ASC"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        for row in rows:
            normalized = {}
            for key, value in row.items():
                if hasattr(value, "isoformat"):
                    normalized[key] = value.isoformat()
                else:
                    normalized[key] = value
            fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")


def main():
    args = parse_args()

    with closing(
        psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )
    ) as conn:
        conn.autocommit = False

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM mem_window_samples WHERE target_dynamic_pool_ratio IS NULL ORDER BY window_start ASC"
            )
            windows = cur.fetchall()

        for window in windows:
            best_ratio = None
            best_score = None

            for ratio in iter_ratios(args.ratio_start, args.ratio_stop, args.ratio_step):
                with conn.cursor() as cur:
                    cur.execute(args.set_sql, (str(ratio),))

                if args.replay_command_template:
                    score = replay_score(args.replay_command_template, ratio, window["id"])
                else:
                    score = heuristic_score(window, ratio)

                if best_score is None or score < best_score:
                    best_score = score
                    best_ratio = ratio

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE mem_window_samples SET target_dynamic_pool_ratio = %s WHERE id = %s",
                    (best_ratio, window["id"]),
                )
            conn.commit()

        if args.export_output:
            export_rows(conn, args.export_output, args.export_only_labeled)

    print(f"labeled {len(windows)} window samples")


if __name__ == "__main__":
    main()
