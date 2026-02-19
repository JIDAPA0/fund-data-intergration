from __future__ import annotations

import pandas as pd

from .config import FX_BASE_CCY, TOP_N
from .models import Dataset
from .utils import is_country_label, to_float


def _weighted_avg(group: pd.DataFrame, col: str) -> float | None:
    g = group[group[col].notna() & group["aum"].notna() & (group["aum"] > 0)]
    if g.empty:
        return None
    return float((g[col] * g["aum"]).sum() / g["aum"].sum())


def _prepare_nav_with_fx(ds: Dataset) -> pd.DataFrame:
    nav = ds.thai_nav_aum.copy()
    nav["aum_native"] = to_float(nav["aum"]).fillna(0.0)
    nav["nav_as_of_date"] = pd.to_datetime(nav["nav_as_of_date"], errors="coerce").dt.date

    fund_ccy = ds.thai_funds[["fund_code", "currency"]].drop_duplicates("fund_code").copy()
    fund_ccy["fund_currency"] = fund_ccy["currency"].fillna(FX_BASE_CCY).astype(str).str.strip().str.upper()
    fund_ccy = fund_ccy.drop(columns=["currency"])

    nav = nav.merge(fund_ccy, on="fund_code", how="left")
    nav["fund_currency"] = nav["fund_currency"].fillna(FX_BASE_CCY)

    fx = ds.fx_rates.copy()
    if fx.empty:
        nav["fx_rate_to_thb"] = 1.0
        nav["fx_rate_date"] = nav["nav_as_of_date"]
        nav["fx_rate_status"] = "default_1_no_fx_table"
    else:
        fx["date_rate"] = pd.to_datetime(fx["date_rate"], errors="coerce").dt.date
        fx["from_ccy"] = fx["from_ccy"].fillna("").astype(str).str.strip().str.upper()
        fx["rate_to_thb"] = to_float(fx["rate_to_thb"])
        fx = fx[fx["date_rate"].notna() & fx["from_ccy"].ne("") & fx["rate_to_thb"].notna()].copy()

        fx_exact = fx[["from_ccy", "date_rate", "rate_to_thb"]].rename(
            columns={"date_rate": "fx_rate_date_exact", "rate_to_thb": "fx_rate_exact"}
        )
        nav = nav.merge(
            fx_exact,
            left_on=["fund_currency", "nav_as_of_date"],
            right_on=["from_ccy", "fx_rate_date_exact"],
            how="left",
        )
        nav = nav.drop(columns=["from_ccy"], errors="ignore")

        fx_latest = (
            fx.sort_values(["from_ccy", "date_rate"])
            .groupby("from_ccy", as_index=False)
            .tail(1)[["from_ccy", "date_rate", "rate_to_thb"]]
            .rename(columns={"date_rate": "fx_rate_date_latest", "rate_to_thb": "fx_rate_latest"})
        )
        nav = nav.merge(fx_latest, left_on="fund_currency", right_on="from_ccy", how="left")
        nav = nav.drop(columns=["from_ccy"], errors="ignore")

        nav["fx_rate_to_thb"] = (
            nav["fx_rate_exact"].where(nav["fx_rate_exact"].notna(), nav["fx_rate_latest"]).fillna(1.0)
        )
        nav["fx_rate_date"] = nav["fx_rate_date_exact"].where(
            nav["fx_rate_exact"].notna(), nav["fx_rate_date_latest"]
        )
        nav["fx_rate_date"] = nav["fx_rate_date"].fillna(nav["nav_as_of_date"])
        nav["fx_rate_status"] = "exact_or_latest"
        nav.loc[nav["fund_currency"] == FX_BASE_CCY, "fx_rate_status"] = "base_currency"
        nav.loc[
            (nav["fund_currency"] != FX_BASE_CCY)
            & nav["fx_rate_exact"].isna()
            & nav["fx_rate_latest"].isna(),
            "fx_rate_status",
        ] = "default_1_missing_fx"

    nav.loc[nav["fund_currency"] == FX_BASE_CCY, "fx_rate_to_thb"] = 1.0
    nav["aum"] = (nav["aum_native"] * nav["fx_rate_to_thb"]).fillna(0.0)

    return nav[
        [
            "fund_code",
            "nav_as_of_date",
            "aum",
            "aum_native",
            "fund_currency",
            "fx_rate_to_thb",
            "fx_rate_date",
            "fx_rate_status",
        ]
    ]


def build_exposure_tables(ds: Dataset, bridge: pd.DataFrame) -> dict[str, pd.DataFrame]:
    bridge_ok = bridge[bridge["ft_ticker"].notna()].copy()
    bridge_ok["feeder_weight_pct"] = to_float(bridge_ok["feeder_weight_pct"]).fillna(0.0)
    bridge_ok = bridge_ok[bridge_ok["feeder_weight_pct"] > 0].copy()

    # Normalize mapped weight per fund to avoid over-allocation from duplicate/over-100 inputs.
    bridge_ok["sum_weight_by_fund"] = bridge_ok.groupby("fund_code")["feeder_weight_pct"].transform("sum")
    bridge_ok["target_weight_by_fund"] = bridge_ok["sum_weight_by_fund"].clip(upper=100.0)
    bridge_ok["feeder_weight_pct_norm"] = (
        bridge_ok["feeder_weight_pct"] / bridge_ok["sum_weight_by_fund"].replace(0, pd.NA) * bridge_ok["target_weight_by_fund"]
    ).fillna(0.0)
    bridge_ok = bridge_ok.drop(columns=["sum_weight_by_fund", "target_weight_by_fund"])

    ft_holdings = ds.ft_holdings.copy()
    ft_holdings["portfolio_weight_pct"] = to_float(ft_holdings["portfolio_weight_pct"]).fillna(0.0)
    ft_holdings = (
        ft_holdings.groupby(["ticker", "holding_name", "holding_ticker", "holding_type", "date_scraper"], as_index=False)
        .agg(portfolio_weight_pct=("portfolio_weight_pct", "max"))
    )

    exp_stock = bridge_ok.merge(
        ft_holdings,
        left_on="ticker",
        right_on="ticker",
        how="inner",
        suffixes=("_bridge", "_holding"),
    )

    exp_stock["true_weight_pct"] = (exp_stock["feeder_weight_pct_norm"] * exp_stock["portfolio_weight_pct"]) / 100.0

    nav = _prepare_nav_with_fx(ds)
    nav_native = nav[
        ["fund_code", "nav_as_of_date", "aum_native", "fund_currency"]
    ].drop_duplicates(["fund_code"], keep="first")
    exp_stock = exp_stock.merge(nav, on="fund_code", how="left")
    exp_stock["aum"] = to_float(exp_stock["aum"]).fillna(0.0)
    exp_stock["true_value_thb"] = ((exp_stock["aum"] * exp_stock["true_weight_pct"]) / 100.0).fillna(0.0)

    exp_stock = exp_stock[[
        "fund_code",
        "ft_ticker",
        "ticker",
        "map_method",
        "feeder_name",
        "feeder_weight_pct",
        "feeder_weight_pct_norm",
        "holding_name",
        "holding_ticker",
        "holding_type",
        "portfolio_weight_pct",
        "true_weight_pct",
        "aum",
        "aum_native",
        "fund_currency",
        "fx_rate_to_thb",
        "fx_rate_date",
        "fx_rate_status",
        "true_value_thb",
        "nav_as_of_date",
        "date_scraper",
    ]]
    exp_stock["holding_ticker_norm"] = exp_stock["holding_ticker"].fillna("").astype(str).str.strip().str.upper()
    exp_stock["holding_name_norm"] = exp_stock["holding_name"].fillna("").astype(str).str.strip().str.upper()
    exp_stock["holding_key"] = exp_stock["holding_ticker_norm"].where(
        exp_stock["holding_ticker_norm"] != "",
        exp_stock["holding_name_norm"],
    )

    # Sector exposure
    ft_sector = ds.ft_sector.copy()
    ft_sector["weight_pct"] = to_float(ft_sector["weight_pct"]).fillna(0.0)
    ft_sector = ft_sector.groupby(["ticker", "category_name", "date_scraper"], as_index=False).agg(weight_pct=("weight_pct", "max"))
    exp_sector = bridge_ok.merge(ft_sector, on="ticker", how="inner")
    exp_sector["true_weight_pct"] = (exp_sector["feeder_weight_pct_norm"] * exp_sector["weight_pct"]) / 100.0
    exp_sector = exp_sector.merge(nav, on="fund_code", how="left")
    exp_sector["aum"] = to_float(exp_sector["aum"]).fillna(0.0)
    exp_sector["true_value_thb"] = ((exp_sector["aum"] * exp_sector["true_weight_pct"]) / 100.0).fillna(0.0)
    exp_sector = exp_sector[[
        "fund_code",
        "ft_ticker",
        "ticker",
        "map_method",
        "category_name",
        "weight_pct",
        "feeder_weight_pct",
        "feeder_weight_pct_norm",
        "true_weight_pct",
        "aum",
        "aum_native",
        "fund_currency",
        "fx_rate_to_thb",
        "fx_rate_date",
        "fx_rate_status",
        "true_value_thb",
        "nav_as_of_date",
        "date_scraper",
    ]].rename(columns={"category_name": "sector_name", "weight_pct": "sector_weight_pct"})

    # Country/region exposure
    ft_region = ds.ft_region.copy()
    ft_region["weight_pct"] = to_float(ft_region["weight_pct"]).fillna(0.0)
    ft_region = ft_region.groupby(["ticker", "category_name", "date_scraper"], as_index=False).agg(weight_pct=("weight_pct", "max"))
    exp_region = bridge_ok.merge(ft_region, on="ticker", how="inner")
    exp_region["true_weight_pct"] = (exp_region["feeder_weight_pct_norm"] * exp_region["weight_pct"]) / 100.0
    exp_region = exp_region.merge(nav, on="fund_code", how="left")
    exp_region["aum"] = to_float(exp_region["aum"]).fillna(0.0)
    exp_region["true_value_thb"] = ((exp_region["aum"] * exp_region["true_weight_pct"]) / 100.0).fillna(0.0)
    exp_region["is_country_like"] = exp_region["category_name"].map(is_country_label)
    exp_region = exp_region[[
        "fund_code",
        "ft_ticker",
        "ticker",
        "map_method",
        "category_name",
        "weight_pct",
        "feeder_weight_pct",
        "feeder_weight_pct_norm",
        "true_weight_pct",
        "aum",
        "aum_native",
        "fund_currency",
        "fx_rate_to_thb",
        "fx_rate_date",
        "fx_rate_status",
        "true_value_thb",
        "is_country_like",
        "nav_as_of_date",
        "date_scraper",
    ]].rename(columns={"category_name": "region_name", "weight_pct": "region_weight_pct"})

    # Coverage
    feeder_total = ds.thai_feeder.copy()
    feeder_total["feeder_weight_pct"] = to_float(feeder_total["feeder_weight_pct"]).fillna(0.0)
    feeder_total = feeder_total.groupby("fund_code", as_index=False)["feeder_weight_pct"].sum().rename(columns={"feeder_weight_pct": "raw_total_fund_holdings_pct"})
    feeder_total["total_fund_holdings_pct"] = feeder_total["raw_total_fund_holdings_pct"].clip(upper=100.0)

    mapped_weight = bridge_ok.groupby("fund_code", as_index=False)["feeder_weight_pct_norm"].sum().rename(columns={"feeder_weight_pct_norm": "mapped_holdings_pct"})

    coverage = feeder_total.merge(mapped_weight, on="fund_code", how="left")
    coverage["mapped_holdings_pct"] = coverage["mapped_holdings_pct"].fillna(0.0)
    coverage["coverage_ratio"] = (
        coverage["mapped_holdings_pct"] / coverage["total_fund_holdings_pct"].replace(0, pd.NA)
    ).clip(lower=0.0, upper=1.0)
    coverage = coverage.merge(nav, on="fund_code", how="left")

    # Return metric
    ret = ds.ft_return.copy()
    ret = ret[["ft_ticker", "ticker", "avg_fund_return_1y", "avg_fund_return_3y"]]
    ret["avg_fund_return_1y"] = to_float(ret["avg_fund_return_1y"])
    ret["avg_fund_return_3y"] = to_float(ret["avg_fund_return_3y"])

    fund_ret = bridge_ok[["fund_code", "ft_ticker", "ticker"]].drop_duplicates().merge(ret, on=["ft_ticker", "ticker"], how="left")
    fund_ret = fund_ret.merge(nav[["fund_code", "aum"]], on="fund_code", how="left")

    avg_1y = _weighted_avg(fund_ret, "avg_fund_return_1y")
    avg_3y = _weighted_avg(fund_ret, "avg_fund_return_3y")

    # Aggregates for dashboard
    top_holdings = (
        exp_stock.groupby(["holding_key", "holding_ticker_norm", "holding_type"], as_index=False)
        .agg(
            total_true_weight_pct=("true_weight_pct", "sum"),
            total_true_value_thb=("true_value_thb", "sum"),
            holding_name=("holding_name", "first"),
        )
        .rename(columns={"holding_ticker_norm": "holding_ticker"})
        .sort_values(["total_true_value_thb", "total_true_weight_pct"], ascending=False)
    )
    top_holdings["holding_ticker"] = top_holdings["holding_ticker"].replace("", pd.NA)
    top_holdings["rank_no"] = range(1, len(top_holdings) + 1)

    sector_agg = (
        exp_sector.groupby("sector_name", as_index=False)
        .agg(total_true_weight_pct=("true_weight_pct", "sum"), total_true_value_thb=("true_value_thb", "sum"))
        .sort_values("total_true_value_thb", ascending=False)
    )
    sector_total_value = pd.to_numeric(sector_agg["total_true_value_thb"], errors="coerce").fillna(0.0).sum()
    sector_agg["allocation_share_pct"] = (
        pd.to_numeric(sector_agg["total_true_value_thb"], errors="coerce").fillna(0.0) / sector_total_value * 100.0
        if sector_total_value
        else 0.0
    )

    region_agg = (
        exp_region.groupby(["region_name", "is_country_like"], as_index=False)
        .agg(total_true_weight_pct=("true_weight_pct", "sum"), total_true_value_thb=("true_value_thb", "sum"))
        .sort_values("total_true_value_thb", ascending=False)
    )

    country_agg = region_agg[region_agg["is_country_like"]].copy()
    country_total_value = pd.to_numeric(country_agg["total_true_value_thb"], errors="coerce").fillna(0.0).sum()
    country_agg["allocation_share_pct"] = (
        pd.to_numeric(country_agg["total_true_value_thb"], errors="coerce").fillna(0.0) / country_total_value * 100.0
        if country_total_value
        else 0.0
    )

    total_value = float(pd.to_numeric(exp_stock["true_value_thb"], errors="coerce").fillna(0.0).sum())

    top_sector_row = sector_agg.head(1)
    top_country_row = country_agg.head(1)

    dashboard_cards = pd.DataFrame(
        [
            {
                "total_holdings_value_thb": total_value,
                "top_sector_name": None if top_sector_row.empty else top_sector_row.iloc[0]["sector_name"],
                "top_sector_weight_pct": None if top_sector_row.empty else float(top_sector_row.iloc[0]["allocation_share_pct"]),
                "top_country_name": None if top_country_row.empty else top_country_row.iloc[0]["region_name"],
                "top_country_weight_pct": None if top_country_row.empty else float(top_country_row.iloc[0]["allocation_share_pct"]),
                "avg_fund_return_1y": avg_1y,
                "avg_fund_return_3y": avg_3y,
                "mapped_fund_count": int(bridge_ok["fund_code"].nunique()),
                "mapped_master_count": int(bridge_ok["ft_ticker"].nunique()),
            }
        ]
    )

    top_holdings_topn = top_holdings.head(TOP_N).copy()
    sector_topn = sector_agg.head(TOP_N).copy()
    country_topn = country_agg.head(TOP_N).copy()

    return {
        "stg_nav_aum_native": nav_native,
        "bridge_thai_master": bridge,
        "fact_effective_exposure_stock": exp_stock,
        "fact_effective_exposure_sector": exp_sector,
        "fact_effective_exposure_region": exp_region,
        "agg_top_holdings": top_holdings,
        "agg_top_holdings_topn": top_holdings_topn,
        "agg_sector_exposure": sector_agg,
        "agg_sector_exposure_topn": sector_topn,
        "agg_country_exposure": country_agg,
        "agg_country_exposure_topn": country_topn,
        "agg_region_exposure": region_agg,
        "agg_fund_coverage": coverage,
        "agg_dashboard_cards": dashboard_cards,
    }
