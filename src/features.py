from __future__ import annotations

import pandas as pd


def build_round_trip_id(df: pd.DataFrame, origin_col: str = "ORIGIN", dest_col: str = "DEST") -> pd.Series:
    origins = df[origin_col].astype(str).str.upper().str.strip()
    dests = df[dest_col].astype(str).str.upper().str.strip()
    pair = pd.concat([origins.rename("orig"), dests.rename("dest")], axis=1)
    first = pair.min(axis=1)
    second = pair.max(axis=1)
    return first.str.cat(second, sep="-")


def build_leg_direction(df: pd.DataFrame, origin_col: str = "ORIGIN", dest_col: str = "DEST") -> pd.Series:
    return (df[origin_col].astype(str).str.upper().str.strip()
            + ">" + df[dest_col].astype(str).str.upper().str.strip())


def on_time_flags(df: pd.DataFrame, dep_delay_col: str = "DEP_DELAY", arr_delay_col: str = "ARR_DELAY",
                  threshold_min: int = 0) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    if dep_delay_col in df.columns:
        out["on_time_dep"] = (pd.to_numeric(df[dep_delay_col], errors="coerce") <= threshold_min)
    else:
        out["on_time_dep"] = pd.Series(False, index=df.index)
    if arr_delay_col in df.columns:
        out["on_time_arr"] = (pd.to_numeric(df[arr_delay_col], errors="coerce") <= threshold_min)
    else:
        out["on_time_arr"] = pd.Series(False, index=df.index)
    out["on_time_both"] = out["on_time_dep"] & out["on_time_arr"]
    return out


def mask_cancelled(df: pd.DataFrame, cancelled_col: str = "CANCELLED") -> pd.Series:
    if cancelled_col not in df.columns:
        return pd.Series(True, index=df.index)
    return (pd.to_numeric(df[cancelled_col], errors="coerce").fillna(0).astype(int) == 0)



