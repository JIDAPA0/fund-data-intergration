#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import pymysql
from pymysql.cursors import DictCursor


@dataclass
class Check:
    name: str
    sql: str
    max_allowed: float
    metric_label: str
    note: str


CHECKS = [
    Check(
        name="required_tables_present",
        sql="""
            SELECT 12 - COUNT(*) AS missing_count
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name IN (
                'bridge_thai_master',
                'fact_effective_exposure_stock',
                'fact_effective_exposure_sector',
                'fact_effective_exposure_region',
                'agg_top_holdings',
                'agg_top_holdings_topn',
                'agg_sector_exposure',
                'agg_sector_exposure_topn',
                'agg_country_exposure',
                'agg_country_exposure_topn',
                'agg_region_exposure',
                'agg_dashboard_cards'
              )
        """,
        max_allowed=0,
        metric_label="missing_count",
        note="required mart tables exist",
    ),
    Check(
        name="stock_negative_values",
        sql="""
            SELECT COUNT(*) AS bad_rows
            FROM fact_effective_exposure_stock
            WHERE COALESCE(true_weight_pct, -1) < 0
               OR COALESCE(true_value_thb, -1) < 0
        """,
        max_allowed=0,
        metric_label="bad_rows",
        note="stock true_weight_pct/true_value_thb non-negative",
    ),
    Check(
        name="fund_weight_over_100",
        sql="""
            SELECT COUNT(*) AS bad_funds
            FROM (
                SELECT fund_code
                FROM fact_effective_exposure_stock
                GROUP BY fund_code
                HAVING SUM(COALESCE(true_weight_pct, 0)) > 100.5
            ) t
        """,
        max_allowed=0,
        metric_label="bad_funds",
        note="sum(true_weight_pct) per fund <= 100.5",
    ),
    Check(
        name="coverage_ratio_out_of_range",
        sql="""
            SELECT COUNT(*) AS bad_rows
            FROM agg_fund_coverage
            WHERE coverage_ratio < 0 OR coverage_ratio > 1
        """,
        max_allowed=0,
        metric_label="bad_rows",
        note="coverage_ratio should be in [0,1]",
    ),
    Check(
        name="duplicate_topn_rank",
        sql="""
            SELECT COUNT(*) AS duplicate_ranks
            FROM (
                SELECT rank_no
                FROM agg_top_holdings_topn
                GROUP BY rank_no
                HAVING COUNT(*) > 1
            ) d
        """,
        max_allowed=0,
        metric_label="duplicate_ranks",
        note="rank_no uniqueness in agg_top_holdings_topn",
    ),
    Check(
        name="dashboard_total_diff",
        sql="""
            SELECT ABS(a.total_holdings_value_thb - b.fact_total) AS diff_value
            FROM (
                SELECT COALESCE(total_holdings_value_thb, 0) AS total_holdings_value_thb
                FROM agg_dashboard_cards
                LIMIT 1
            ) a
            CROSS JOIN (
                SELECT COALESCE(SUM(true_value_thb), 0) AS fact_total
                FROM fact_effective_exposure_stock
            ) b
        """,
        max_allowed=1.0,
        metric_label="diff_value",
        note="dashboard total matches fact total within 1 THB",
    ),
    Check(
        name="dashboard_cards_rowcount",
        sql="""
            SELECT ABS(COUNT(*) - 1) AS rowcount_delta
            FROM agg_dashboard_cards
        """,
        max_allowed=0,
        metric_label="rowcount_delta",
        note="agg_dashboard_cards must contain exactly 1 row",
    ),
]


def run_checks(host: str, port: int, user: str, password: str, database: str) -> int:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
    )

    failures = []
    try:
        with conn.cursor() as cur:
            print(f"Sanity checks on {database} @ {host}:{port}")
            print("-" * 92)
            print(f"{'CHECK':36} {'STATUS':7} {'METRIC':>14} {'THRESHOLD':>12}  NOTE")
            print("-" * 92)

            for c in CHECKS:
                if c.name == "required_tables_present":
                    cur.execute(c.sql, (database,))
                else:
                    cur.execute(c.sql)

                row = cur.fetchone() or {}
                value = float(row.get(c.metric_label, 0) or 0)
                ok = value <= c.max_allowed
                status = "PASS" if ok else "FAIL"
                print(f"{c.name:36} {status:7} {value:14.4f} {c.max_allowed:12.4f}  {c.note}")
                if not ok:
                    failures.append((c.name, value, c.max_allowed))

            print("-" * 92)
            if failures:
                print("RESULT: FAIL")
                for name, value, max_allowed in failures:
                    print(f"  - {name}: {value:.4f} > {max_allowed:.4f}")
                return 1

            print("RESULT: PASS")
            return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run traceability mart sanity checks (PASS/FAIL).")
    parser.add_argument("--host", default=os.getenv("MART_DB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MART_DB_PORT", "3307")))
    parser.add_argument("--user", default=os.getenv("MART_DB_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MART_DB_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("MART_DB_NAME", "fund_traceability"))
    args = parser.parse_args()

    try:
        return run_checks(args.host, args.port, args.user, args.password, args.database)
    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
