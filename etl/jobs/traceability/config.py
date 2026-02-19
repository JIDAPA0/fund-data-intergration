from __future__ import annotations

import os

THAI_DB_URI = os.getenv("THAI_DB_URI", "mysql+pymysql://root:@127.0.0.1:3307/raw_thai_funds")
GLOBAL_DB_URI = os.getenv("GLOBAL_DB_URI", "mysql+pymysql://root:@127.0.0.1:3306/raw_ft")
MART_DB_URI = os.getenv("MART_DB_URI", "mysql+pymysql://root:@127.0.0.1:3307/fund_traceability")

TOP_N = int(os.getenv("TOP_N", "10"))

REGION_LIKE_VALUES = {
    "Americas",
    "North America",
    "South America",
    "Europe",
    "Asia",
    "Africa",
    "Middle East",
    "Pacific",
    "Global",
    "Other",
    "Others",
    "Greater Asia",
    "Greater China",
    "Developed Markets",
    "Emerging Markets",
}
