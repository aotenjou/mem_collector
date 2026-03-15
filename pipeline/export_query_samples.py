#!/usr/bin/env python3

import argparse
import json
from contextlib import closing

import psycopg2
from psycopg2.extras import RealDictCursor


def parse_args():
    parser = argparse.ArgumentParser(description="Export mem_query_samples or mem_window_samples to JSONL")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="")
    parser.add_argument("--table", default="mem_query_samples", choices=["mem_query_samples", "mem_window_samples"])
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def build_query(table: str, limit: int) -> str:
    order_column = "collected_at" if table == "mem_query_samples" else "window_start"
    sql = f"SELECT * FROM {table} ORDER BY {order_column} ASC"
    if limit > 0:
        sql += f" LIMIT {limit}"
    return sql


def main():
    args = parse_args()
    sql = build_query(args.table, args.limit)

    with closing(
        psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
        )
    ) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    with open(args.output, "w", encoding="utf-8") as fh:
        for row in rows:
            normalized = {}
            for key, value in row.items():
                if hasattr(value, "isoformat"):
                    normalized[key] = value.isoformat()
                else:
                    normalized[key] = value
            fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")

    print(f"exported {len(rows)} rows from {args.table} to {args.output}")


if __name__ == "__main__":
    main()
