from __future__ import annotations

import re

import pandas as pd

from .config import REGION_LIKE_VALUES


def extract_token(value: str) -> str | None:
    if not isinstance(value, str):
        return None
    m = re.search(r"\(([^()]*)\)\s*$", value.strip())
    if not m:
        return None
    token = m.group(1).strip().upper()
    return token or None


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def is_country_label(label: str | None) -> bool:
    if not isinstance(label, str) or not label.strip():
        return False
    val = label.strip()
    if val in REGION_LIKE_VALUES:
        return False
    bad_tokens = [
        "Asia",
        "Europe",
        "America",
        "Pacific",
        "Global",
        "World",
        "Emerging",
        "Developed",
        "Middle East",
        "Africa",
        "Greater",
        "Other",
    ]
    return not any(tok in val for tok in bad_tokens)
