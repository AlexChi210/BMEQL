from __future__ import annotations

import os
import pandas as pd

from src.ingest import ingest_airports, ingest_flights, ingest_tickets, write_processed
from src.quality import (
    check_nulls,
    check_duplicates,
    check_iata_format,
    check_cancelled_binary,
    check_occupancy_bounds,
)
from src.features import build_round_trip_id, build_leg_direction, on_time_flags, mask_cancelled


def main(project_root: str) -> None:
    data_dir = os.path.join(project_root, "data")
    raw = os.path.join(data_dir, "raw")
    proc = os.path.join(data_dir, "processed")
    docs = os.path.join(project_root, "docs")
    os.makedirs(proc, exist_ok=True)
    os.makedirs(docs, exist_ok=True)

    meta = os.path.join(data_dir, "Airline_Challenge_Metadata.xlsx")
    airports = ingest_airports(os.path.join(raw, "Airport_Codes.csv"), meta)
    flights = ingest_flights(os.path.join(raw, "Flights.csv"), meta)
    tickets = ingest_tickets(os.path.join(raw, "Tickets.csv"), meta)

    write_processed(airports, os.path.join(proc, "airports.csv"))
    write_processed(flights, os.path.join(proc, "flights.csv"))
    write_processed(tickets, os.path.join(proc, "tickets.csv"))

    issues = []
    issues.append(check_nulls(flights, []))
    issues.append(check_duplicates(airports, ["IATA_CODE"]))
    if "IATA_CODE" in airports.columns:
        issues.append(check_iata_format(airports["IATA_CODE"]))
    if "CANCELLED" in flights.columns:
        issues.append(check_cancelled_binary(flights["CANCELLED"]))
    if "OCCUPANCY_RATE" in flights.columns:
        issues.append(check_occupancy_bounds(flights["OCCUPANCY_RATE"]))
    issue_log = pd.concat([i for i in issues if i is not None and not i.empty], ignore_index=True) if any([i is not None and not i.empty for i in issues]) else pd.DataFrame()
    if not issue_log.empty:
        issue_log.to_csv(os.path.join(docs, "issue_log.csv"), index=False)

    flights_eng = flights.copy()
    flights_eng["round_trip_id"] = build_round_trip_id(flights_eng)
    flights_eng["leg_dir"] = build_leg_direction(flights_eng)
    ot = on_time_flags(flights_eng)
    flights_eng = pd.concat([flights_eng, ot], axis=1)
    valid_leg = mask_cancelled(flights_eng)
    write_processed(flights_eng, os.path.join(proc, "flights_engineered.csv"))

    legs = flights_eng[valid_leg].copy()
    by_route_dir = (
        legs.groupby(["round_trip_id", "leg_dir"]).size().reset_index(name="legs")
    )
    # completed round trips: min of directional leg counts per route
    rt_completed = (
        by_route_dir.groupby("round_trip_id")["legs"].min().rename("completed_round_trips")
    )
    busiest = rt_completed.sort_values(ascending=False).head(10).reset_index()
    busiest.to_csv(os.path.join(proc, "busiest_routes.csv"), index=False)

    tix_q1 = tickets.copy()
    if "ROUNDTRIP" in tix_q1.columns:
        tix_q1 = tix_q1[tix_q1["ROUNDTRIP"] == 1]
    tix_q1["round_trip_id"] = (
        tix_q1[["ORIGIN", "DEST"]]
        .astype(str)
        .stack()
        .groupby(level=0)
        .apply(lambda s: "-".join(sorted(s.str.upper().str.strip())))
    )
    tix_q1["itinerary_revenue"] = tix_q1.get("PASSENGERS", 0) * tix_q1.get("ITIN_FARE", 0.0)
    route_revenue = (
        tix_q1.groupby("round_trip_id", as_index=False)["itinerary_revenue"]
        .sum()
        .rename(columns={"itinerary_revenue": "total_revenue"})
    )
    route_revenue.to_csv(os.path.join(proc, "route_revenue.csv"), index=False)

    summary = rt_completed.rename_axis("round_trip_id").reset_index(name="completed_round_trips")
    summary = summary.merge(route_revenue, on="round_trip_id", how="left")
    punct = legs.groupby("round_trip_id")["on_time_arr"].mean().rename("on_time_arr_rate").reset_index()
    summary = summary.merge(punct, on="round_trip_id", how="left")
    summary["operating_cost"] = 0.0
    summary["operating_profit"] = summary["total_revenue"].fillna(0.0) - summary["operating_cost"]
    write_processed(summary, os.path.join(proc, "route_summary.csv"))

    sc = summary.copy()
    for col in ["operating_profit", "completed_round_trips", "on_time_arr_rate"]:
        std = sc[col].std(ddof=0)
        sc[col + "_z"] = (sc[col] - sc[col].mean()) / std if std and std > 0 else 0
    sc["score"] = 0.5 * sc["operating_profit_z"] + 0.3 * sc["completed_round_trips_z"] + 0.2 * sc["on_time_arr_rate_z"]
    top5 = sc.sort_values("score", ascending=False).head(5)
    top5.to_csv(os.path.join(proc, "recommended_routes.csv"), index=False)

    if not top5.empty:
        top5 = top5.copy()
        top5["profit_per_round_trip"] = (top5["operating_profit"] / top5["completed_round_trips"].replace(0, pd.NA)).fillna(0.0)
        aircraft_cost = 0
        top5["round_trips_to_breakeven"] = (aircraft_cost / top5["profit_per_round_trip"]).replace([pd.NA, pd.NaT, float("inf")], 0)
        top5.to_csv(os.path.join(proc, "breakeven_top5.csv"), index=False)

    print("Pipeline completed. Outputs written to:", proc)


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    main(root)


