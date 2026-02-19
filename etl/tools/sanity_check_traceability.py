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


def run_checks(host: str, port: int, user: str, password: str, database: str, fx_missing_max_pct: float) -> int:
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

            # FX missing-rate alert check
            cur.execute(
                """
                SELECT COUNT(*) AS c
                FROM information_schema.views
                WHERE table_schema = DATABASE()
                  AND table_name = 'vw_nav_aum_thb'
                """
            )
            view_exists = int((cur.fetchone() or {}).get("c", 0)) == 1
            if not view_exists:
                failures.append(("vw_nav_aum_thb_missing", 1.0, 0.0))
                print(
                    f"{'vw_nav_aum_thb_missing':36} {'FAIL':7} {1.0:14.4f} {0.0:12.4f}  "
                    "required view for FX quality check not found"
                )
            else:
                cur.execute(
                    """
                    SELECT
                      CASE WHEN COUNT(*) = 0 THEN 0
                           ELSE (
                             SUM(CASE WHEN fund_currency <> 'THB' AND fx_rate_status = 'default_1_missing_fx' THEN 1 ELSE 0 END)
                             * 100.0 / COUNT(*)
                           )
                      END AS missing_fx_pct
                    FROM vw_nav_aum_thb
                    """
                )
                missing_fx_pct = float((cur.fetchone() or {}).get("missing_fx_pct", 0) or 0)
                ok = missing_fx_pct <= fx_missing_max_pct
                status = "PASS" if ok else "FAIL"
                print(
                    f"{'fx_missing_rate_pct':36} {status:7} {missing_fx_pct:14.4f} {fx_missing_max_pct:12.4f}  "
                    "percent of rows using default_1_missing_fx (non-THB only)"
                )
                if not ok:
                    failures.append(("fx_missing_rate_pct", missing_fx_pct, fx_missing_max_pct))

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
    parser.add_argument(
        "--fx-missing-max-pct",
        type=float,
        default=float(os.getenv("FX_MISSING_MAX_PCT", "5.0")),
        help="Fail when percent of non-THB rows with fx_rate_status=default_1_missing_fx exceeds this threshold",
    )
    args = parser.parse_args()

    try:
        return run_checks(args.host, args.port, args.user, args.password, args.database, args.fx_missing_max_pct)
    except pymysql.MySQLError as exc:
        print(f"DB error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
