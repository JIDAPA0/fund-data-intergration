from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class Dataset:
    thai_funds: pd.DataFrame
    thai_isin: pd.DataFrame
    thai_nav_aum: pd.DataFrame
    thai_feeder: pd.DataFrame
    ft_static: pd.DataFrame
    ft_holdings: pd.DataFrame
    ft_sector: pd.DataFrame
    ft_region: pd.DataFrame
    ft_return: pd.DataFrame
    fx_rates: pd.DataFrame
