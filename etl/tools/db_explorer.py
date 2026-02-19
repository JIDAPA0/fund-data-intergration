#!/usr/bin/env python3
"""Database explorer for table/schema/sample inspection.

Usage:
  DB_URI='mysql+pymysql://user:password@localhost:3306/raw_global_funds' python etl/tools/db_explorer.py
"""

from __future__ import annotations

import os
import sys
from typing import List

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Replace this value or set DB_URI env var before running.
DB_URI = os.getenv("DB_URI", "mysql+pymysql://user:password@localhost:3306/database_name")

SEPARATOR = "=" * 80


def build_engine(db_uri: str) -> Engine:
    return create_engine(db_uri, pool_pre_ping=True)


def list_tables(engine: Engine) -> List[str]:
    inspector = inspect(engine)
    return inspector.get_table_names()


def print_schema(engine: Engine, table_name: str) -> None:
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)

    schema_rows = [
        {
            "column_name": col.get("name"),
            "data_type": str(col.get("type")),
            "nullable": col.get("nullable"),
        }
        for col in columns
    ]
    schema_df = pd.DataFrame(schema_rows)

    print("Schema:")
    if schema_df.empty:
        print("  (No column metadata found)")
    else:
        print(schema_df.to_string(index=False))


def print_sample_data(engine: Engine, table_name: str, sample_size: int = 3) -> None:
    query = text(f"SELECT * FROM `{table_name}` LIMIT {sample_size}")

    with engine.connect() as conn:
        sample_df = pd.read_sql(query, conn)

    print(f"Sample Data (first {sample_size} rows):")
    if sample_df.empty:
        print("  (Table is empty)")
    else:
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(sample_df.to_string(index=False))


def main() -> int:
    print(SEPARATOR)
    print("Database Explorer")
    print(f"DB_URI: {DB_URI}")
    print(SEPARATOR)

    if "user:password" in DB_URI or "database_name" in DB_URI:
        print("ERROR: Please set a real DB_URI before running.")
        return 1

    try:
        engine = build_engine(DB_URI)
        tables = list_tables(engine)

        print("All tables:")
        if not tables:
            print("  (No tables found)")
            return 0

        for idx, table_name in enumerate(tables, 1):
            print(f"  {idx}. {table_name}")

        for table_name in tables:
            print("\n" + SEPARATOR)
            print(f"Table: {table_name}")
            print(SEPARATOR)

            try:
                print_schema(engine, table_name)
                print_sample_data(engine, table_name, sample_size=3)
            except SQLAlchemyError as table_err:
                print(f"Failed to inspect table '{table_name}': {table_err}")
            except Exception as table_err:  # broad by design for robust exploration
                print(f"Unexpected error on table '{table_name}': {table_err}")

        return 0

    except SQLAlchemyError as db_err:
        print(f"Database connection/query error: {db_err}")
        return 2
    except Exception as err:
        print(f"Unexpected error: {err}")
        return 3


if __name__ == "__main__":
    sys.exit(main())
