from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd


def _ensure_dirs(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _read_metadata_schema(xlsx_path: str) -> Dict[str, str]:
    """Read schema dtypes from Airline_Challenge_Metadata.xlsx if present.

    Expects a sheet with columns: table, column, dtype (pandas dtype string).
    If not found, returns an empty dict and relies on explicit casts below.
    """
    if not os.path.exists(xlsx_path):
        return {}
    try:
        df = pd.read_excel(xlsx_path)
    except Exception:
        return {}
    required_cols = {"table", "column", "dtype"}
    if not required_cols.issubset(set(c.lower() for c in df.columns)):
        # try case-insensitive handling
        df.columns = [c.lower() for c in df.columns]
    if not required_cols.issubset(df.columns):
        return {}
    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        key = f"{row['table']}::{row['column']}".strip()
        mapping[key] = str(row["dtype"]).strip()
    return mapping


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def ingest_airports(csv_path: str, metadata_xlsx: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = _normalize_columns(df)
    # Standardize casing for IATA code and names
    if "IATA_CODE" in df.columns:
        df["IATA_CODE"] = df["IATA_CODE"].astype(str).str.strip().str.upper()
    # Best-effort categorical types
    cat_cols: List[str] = [c for c in ["IATA_CODE", "CITY", "STATE", "COUNTRY", "AIRPORT"] if c in df.columns]
    for c in cat_cols:
        df[c] = df[c].astype("string")
    return df


def ingest_flights(csv_path: str, metadata_xlsx: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    df = _normalize_columns(df)

    # Normalize destination column name
    if "DEST" not in df.columns and "DESTINATION" in df.columns:
        df = df.rename(columns={"DESTINATION": "DEST"})

    # Date and time
    for col in ["FL_DATE"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Scope to Q1 2019
    if "FL_DATE" in df.columns:
        df = df[(df["FL_DATE"].dt.year == 2019) & (df["FL_DATE"].dt.quarter == 1)]

    # Categorical codes
    for col in ["OP_CARRIER", "ORIGIN", "DEST"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Numerics
    numeric_cols = [
        "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DISTANCE", "CRS_ELAPSED_TIME",
        "ACTUAL_ELAPSED_TIME", "AIR_TIME", "CARRIER_DELAY", "WEATHER_DELAY",
        "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "OCCUPANCY_RATE",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Enforce CANCELLED in {0,1} when present
    if "CANCELLED" in df.columns:
        df["CANCELLED"] = df["CANCELLED"].fillna(0).clip(lower=0, upper=1).astype(int)

    return df


def ingest_tickets(csv_path: str, metadata_xlsx: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    df = _normalize_columns(df)

    # Normalize destination column name
    if "DEST" not in df.columns and "DESTINATION" in df.columns:
        df = df.rename(columns={"DESTINATION": "DEST"})

    # Scope to Q1 2019 when YEAR/QUARTER present
    if "YEAR" in df.columns:
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
        df = df[df["YEAR"] == 2019]
    if "QUARTER" in df.columns:
        df["QUARTER"] = pd.to_numeric(df["QUARTER"], errors="coerce").astype("Int64")
        df = df[df["QUARTER"] == 1]

    # Codes upper
    for col in ["ORIGIN", "DEST", "OP_CARRIER"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Fares and counts
    for col in ["ITIN_FARE", "PASSENGERS", "DISTANCE"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Roundtrip indicator if present
    if "ROUNDTRIP" in df.columns:
        df["ROUNDTRIP"] = pd.to_numeric(df["ROUNDTRIP"], errors="coerce").fillna(0).astype(int)

    return df


def write_processed(df: pd.DataFrame, out_path: str) -> None:
    _ensure_dirs(out_path)
    if out_path.lower().endswith(".parquet"):
        df.to_parquet(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)


