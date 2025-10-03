from __future__ import annotations

import re
from typing import Dict, List, Tuple

import pandas as pd


def check_nulls(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    issues = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            issues.append({"column": col, "issue": "nulls", "count": null_count})
    return pd.DataFrame(issues)


def check_duplicates(df: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    if not key_cols:
        return pd.DataFrame()
    dupes = df[df.duplicated(subset=key_cols, keep=False)]
    if dupes.empty:
        return pd.DataFrame()
    return dupes[key_cols].assign(issue="duplicate_key")


def check_iata_format(series: pd.Series) -> pd.DataFrame:
    pattern = re.compile(r"^[A-Z]{3}$")
    bad = series.fillna("").astype(str).str.upper().apply(lambda x: bool(pattern.match(x))) == False
    if bad.any():
        return pd.DataFrame({"value": series[bad], "issue": "bad_iata"})
    return pd.DataFrame()


def check_cancelled_binary(series: pd.Series) -> pd.DataFrame:
    bad = ~series.isin([0, 1])
    if bad.any():
        return pd.DataFrame({"value": series[bad], "issue": "cancelled_not_binary"})
    return pd.DataFrame()


def check_occupancy_bounds(series: pd.Series, lower: float = 0.0, upper: float = 1.2) -> pd.DataFrame:
    bad = (series < lower) | (series > upper)
    bad = bad.fillna(False)
    if bad.any():
        return pd.DataFrame({"value": series[bad], "issue": "occupancy_out_of_bounds"})
    return pd.DataFrame()


def filter_q1_2019(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if date_col not in df.columns:
        return df
    dt = pd.to_datetime(df[date_col], errors="coerce")
    return df[(dt.dt.year == 2019) & (dt.dt.quarter == 1)]


def assert_join_cardinality(left: pd.DataFrame, right: pd.DataFrame, left_on: str, right_on: str) -> Tuple[bool, float]:
    counts = right[right_on].value_counts()
    max_matches = left[left_on].map(counts).fillna(0)
    max_ratio = float(max_matches.max())
    return (max_ratio <= 1.0, max_ratio)



