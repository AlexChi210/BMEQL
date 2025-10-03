from __future__ import annotations

import os
import pandas as pd
import plotly.express as px


def main(project_root: str) -> None:
    data_dir = os.path.join(project_root, "data")
    proc = os.path.join(data_dir, "processed")
    docs = os.path.join(project_root, "docs")
    os.makedirs(docs, exist_ok=True)

    route_summary_path = os.path.join(proc, "route_summary.csv")
    if not os.path.exists(route_summary_path):
        raise FileNotFoundError(f"Missing {route_summary_path}. Run the pipeline first.")

    summary = pd.read_csv(route_summary_path)

    # Top 10 busiest routes
    busiest = summary.sort_values("completed_round_trips", ascending=False).head(10)
    fig1 = px.bar(
        busiest,
        x="round_trip_id",
        y="completed_round_trips",
        title="Top 10 busiest round-trip routes — Q1 2019 (excludes cancellations)",
        labels={"round_trip_id": "Route", "completed_round_trips": "Completed round trips"},
    )
    fig1.update_layout(xaxis_tickangle=-45)
    fig1.write_html(os.path.join(docs, "busiest_routes.html"), include_plotlyjs="cdn")

    # Top 10 most profitable routes
    profitable = summary.sort_values("operating_profit", ascending=False).head(10)
    fig2 = px.bar(
        profitable,
        x="round_trip_id",
        y="operating_profit",
        title="Top 10 most profitable routes — Q1 2019 (excludes aircraft purchase)",
        labels={"round_trip_id": "Route", "operating_profit": "Operating profit"},
    )
    fig2.update_layout(xaxis_tickangle=-45)
    fig2.write_html(os.path.join(docs, "most_profitable_routes.html"), include_plotlyjs="cdn")

    # Profit vs On-time scatter
    fig3 = px.scatter(
        summary,
        x="on_time_arr_rate",
        y="operating_profit",
        size="completed_round_trips",
        hover_name="round_trip_id",
        title="Profit vs On-time arrival rate — Q1 2019 (excludes cancellations)",
        labels={
            "on_time_arr_rate": "On-time arrival rate",
            "operating_profit": "Operating profit",
            "completed_round_trips": "Completed round trips",
        },
    )
    fig3.write_html(os.path.join(docs, "profit_vs_on_time.html"), include_plotlyjs="cdn")

    print("Charts written to:")
    print(os.path.join(docs, "busiest_routes.html"))
    print(os.path.join(docs, "most_profitable_routes.html"))
    print(os.path.join(docs, "profit_vs_on_time.html"))


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    main(root)



