#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor


ROOT = Path(__file__).resolve().parents[2]


def run_build(python_bin: str) -> None:
    cmd = [python_bin, str(ROOT / "etl" / "jobs" / "build_traceability_mart.py")]
    proc = subprocess.run(cmd, cwd=ROOT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Build failed with code {proc.returncode}")


def check_row_counts(host: str, port: int, user: str, password: str, database: str) -> int:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
    )

    checks = [
        ("bridge_thai_master", 1),
        ("fact_effective_exposure_stock", 1),
        ("fact_effective_exposure_sector", 1),
        ("fact_effective_exposure_region", 1),
        ("agg_top_holdings", 1),
        ("agg_dashboard_cards", 1),
    ]

    failures = []
    try:
        with conn.cursor() as cur:
            print(f"Smoke test row-count checks on {database} @ {host}:{port}")
            print("-" * 72)
            print(f"{'TABLE':36} {'ROWS':>12} {'MIN_EXPECTED':>12}  STATUS")
            print("-" * 72)

            for table, min_rows in checks:
                cur.execute(f"SELECT COUNT(*) AS c FROM `{table}`")
                rows = int((cur.fetchone() or {}).get("c", 0))
                ok = rows >= min_rows
                status = "PASS" if ok else "FAIL"
                print(f"{table:36} {rows:12d} {min_rows:12d}  {status}")
                if not ok:
                    failures.append((table, rows, min_rows))

            print("-" * 72)
            if failures:
                print("RESULT: FAIL")
                for table, rows, min_rows in failures:
                    print(f"  - {table}: rows={rows}, expected>={min_rows}")
                return 1

            print("RESULT: PASS")
            return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test: run build and check key mart table row counts.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3307)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="fund_traceability")
    parser.add_argument("--python", dest="python_bin", default=sys.executable)
    parser.add_argument("--skip-build", action="store_true", help="skip build step and only verify row counts")
    args = parser.parse_args()

    try:
        if not args.skip_build:
            run_build(args.python_bin)
        return check_row_counts(args.host, args.port, args.user, args.password, args.database)
    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Smoke test error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
