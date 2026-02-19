from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

from .calculations import build_exposure_tables
from .config import GLOBAL_DB_URI, MART_DB_URI, THAI_DB_URI
from .loaders import create_db_if_needed, load_source_data
from .mapping import build_bridge
from .writer import create_views, print_summary, write_tables


def main() -> int:
    print("Creating mart database if needed...")
    create_db_if_needed(MART_DB_URI)

    thai_engine = create_engine(THAI_DB_URI)
    global_engine = create_engine(GLOBAL_DB_URI)
    mart_engine = create_engine(MART_DB_URI)

    print("Loading raw datasets...")
    ds = load_source_data(thai_engine, global_engine)

    print("Building bridge and exposure tables...")
    bridge = build_bridge(ds)
    tables = build_exposure_tables(ds, bridge)

    print("Writing materialized tables...")
    write_tables(mart_engine, tables)

    print("Creating dashboard views...")
    create_views(mart_engine)

    print_summary(tables)
    print("Mart database:", make_url(MART_DB_URI).database)
    return 0
