from __future__ import annotations

import pandas as pd

from .models import Dataset
from .utils import extract_token, to_float


def build_bridge(ds: Dataset) -> pd.DataFrame:
    feeder = ds.thai_feeder.copy()
    feeder["token"] = feeder["feeder_name"].map(extract_token)
    feeder["token_clean"] = feeder["token"].str.replace(" ", "", regex=False)
    feeder["token_isin"] = feeder["token_clean"].where(
        feeder["token_clean"].fillna("").str.fullmatch(r"[A-Z0-9]{12}"),
        None,
    )

    static = ds.ft_static.copy()
    static["isin_number"] = static["isin_number"].fillna("").str.upper().str.strip()

    bridge_feeder = feeder.merge(
        static[["ft_ticker", "ticker", "isin_number", "name", "ticker_type"]],
        left_on="token_isin",
        right_on="isin_number",
        how="left",
    )
    bridge_feeder["map_method"] = "feeder_holding_isin"

    # Fallback mapping: use Thai fund ISIN only when feeder-holding mapping is absent.
    thai_isin_map = ds.thai_isin.merge(
        static[["ft_ticker", "ticker", "isin_number", "name", "ticker_type", "assets_aum_full_value"]],
        left_on="isin_code",
        right_on="isin_number",
        how="inner",
    )
    feeder_mapped_funds = set(bridge_feeder.loc[bridge_feeder["ft_ticker"].notna(), "fund_code"].unique())
    thai_isin_map = thai_isin_map[~thai_isin_map["fund_code"].isin(feeder_mapped_funds)].copy()
    thai_isin_map["map_method"] = "thai_fund_isin_fallback"
    thai_isin_map["assets_aum_full_value"] = to_float(thai_isin_map["assets_aum_full_value"]).fillna(0.0)
    thai_isin_map["ticker_pref"] = thai_isin_map["ticker_type"].map({"Fund": 1, "ETF": 2}).fillna(9)
    thai_isin_map = (
        thai_isin_map.sort_values(["fund_code", "ticker_pref", "assets_aum_full_value"], ascending=[True, True, False])
        .drop_duplicates(["fund_code"], keep="first")
        .drop(columns=["ticker_pref", "assets_aum_full_value"])
    )
    thai_isin_map["feeder_name"] = thai_isin_map["name"]
    thai_isin_map["feeder_weight_pct"] = 100.0
    thai_isin_map["as_of_date"] = pd.NaT

    bridge_cols = [
        "fund_code",
        "feeder_name",
        "feeder_weight_pct",
        "as_of_date",
        "token",
        "token_isin",
        "ft_ticker",
        "ticker",
        "name",
        "ticker_type",
        "map_method",
    ]

    bridge_feeder = bridge_feeder.reindex(columns=bridge_cols)

    thai_isin_map = thai_isin_map.assign(token=None, token_isin=thai_isin_map["isin_code"])
    thai_isin_map = thai_isin_map.reindex(columns=bridge_cols)

    bridge = pd.concat([bridge_feeder, thai_isin_map], ignore_index=True)
    bridge["feeder_weight_pct"] = to_float(bridge["feeder_weight_pct"]).fillna(0.0)

    # Prefer feeder_holding_isin over thai_fund_isin when both map to same fund/master pair.
    bridge["priority"] = bridge["map_method"].map({"feeder_holding_isin": 1, "thai_fund_isin_fallback": 2}).fillna(9)
    bridge = bridge.sort_values(["fund_code", "ft_ticker", "priority"]).drop_duplicates(["fund_code", "ft_ticker"], keep="first")
    bridge = bridge.drop(columns=["priority"])

    return bridge
