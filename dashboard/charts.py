"""Plotly chart builders for the dashboard."""

import plotly.express as px
import pandas as pd


def neighbourhood_avg_price_bar(df: pd.DataFrame):
    """Horizontal bar chart of avg price by neighbourhood."""
    if df.empty:
        return None
    sorted_df = df.sort_values("avg_price", ascending=True)
    fig = px.bar(
        sorted_df,
        x="avg_price",
        y="neighbourhood",
        color="room_type",
        orientation="h",
        hover_data=["listing_count"],
        labels={"avg_price": "Avg Price ($)", "neighbourhood": "Neighbourhood"},
        title="Average Nightly Price by Neighbourhood",
    )
    fig.update_layout(
        yaxis=dict(dtick=1),
        height=max(400, len(sorted_df) * 28),
    )
    return fig


def top10_price_comparison_bar(df: pd.DataFrame, delta_type: str):
    """Grouped bar: listing price vs neighbourhood avg."""
    if df.empty:
        return None
    label = "Overpriced" if delta_type == "OVERPRICED" else "Underpriced"
    plot_df = df.head(20).copy()
    plot_df["listing_label"] = plot_df["listing_id"].astype(str)

    fig = px.bar(
        plot_df,
        x="listing_label",
        y=["price_amount", "neighbourhood_avg_price"],
        barmode="group",
        hover_data=["neighbourhood", "room_type", "price_delta_pct"],
        title=f"Top 10 {label} Listings, Price vs Neighbourhood Avg",
        labels={"value": "Price ($)", "listing_label": "Listing ID"},
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig


def compliance_trend_line(df: pd.DataFrame):
    """Line chart of compliance rate over time."""
    if df.empty or df["rows_count"].sum() == 0:
        return None
    plot_df = df.copy()
    plot_df["compliance_rate"] = (
        plot_df["compliance_data_count"] / plot_df["rows_count"] * 100
    ).round(1)
    fig = px.line(
        plot_df,
        x="month_start_date",
        y="compliance_rate",
        markers=True,
        title="Data Compliance Rate Over Time",
        labels={
            "month_start_date": "Month",
            "compliance_rate": "Compliance Rate (%)",
        },
    )
    fig.update_layout(yaxis_range=[0, 105])
    return fig
