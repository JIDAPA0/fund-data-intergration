#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys

import pymysql
from pymysql.constants import CLIENT


def apply_sql(sql_file: pathlib.Path, host: str, port: int, user: str, password: str) -> None:
    sql = sql_file.read_text(encoding="utf-8")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            while cur.nextset():
                pass
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply SQL file to MySQL using multi-statements.")
    parser.add_argument("sql_file", type=pathlib.Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3307)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()

    if not args.sql_file.exists():
        print(f"SQL file not found: {args.sql_file}", file=sys.stderr)
        return 1

    apply_sql(args.sql_file, args.host, args.port, args.user, args.password)
    print(f"Applied {args.sql_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())