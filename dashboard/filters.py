"""Sidebar filter widgets shared across dashboard pages."""

import streamlit as st

from dashboard.db import query_df
from dashboard.constants import ROOM_TYPES


def render_filters() -> dict:
    """Render sidebar filters and return selected values as a dict."""
    st.sidebar.header("Filters")

    cities = query_df(
        "SELECT DISTINCT city_name FROM dim_city WHERE is_active = 1 ORDER BY city_name"
    )
    if cities.empty:
        st.sidebar.warning("No cities loaded yet.")
        return {}

    city = st.sidebar.selectbox("City", cities["city_name"].tolist())

    months = query_df(
        """
        SELECT DISTINCT d.month_start_date
        FROM dim_date d
        JOIN fct_listing_monthly_snapshot f ON d.month_key = f.month_key
        JOIN dim_city c ON f.city_key = c.city_key
        WHERE c.city_name = ?
        ORDER BY d.month_start_date DESC
        """,
        (city,),
    )
    if months.empty:
        st.sidebar.warning("No data for this city.")
        return {"city": city}

    month = st.sidebar.selectbox("Snapshot Month", months["month_start_date"].tolist())

    room_type = st.sidebar.selectbox("Room Type", ROOM_TYPES)

    neighbourhoods = query_df(
        """
        SELECT DISTINCT neighbourhood
        FROM vw_rep_monthly_neighbourhood_avg_price
        WHERE city_name = ? AND month_start_date = ?
        ORDER BY neighbourhood
        """,
        (city, month),
    )
    selected_neighbourhoods = st.sidebar.multiselect(
        "Neighbourhoods (optional)",
        neighbourhoods["neighbourhood"].tolist() if not neighbourhoods.empty else [],
    )

    return {
        "city": city,
        "month": month,
        "room_type": room_type,
        "neighbourhoods": selected_neighbourhoods,
    }
