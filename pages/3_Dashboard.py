"""Page 3 — Interactive Dashboard (4 reporting views)."""

import os
import sys

import pandas as pd
import streamlit as st

_project_root = os.path.dirname(os.path.dirname(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dashboard.db import db_has_data, query_df
from dashboard.filters import render_filters
from dashboard.charts import (
    neighbourhood_avg_price_bar,
    top10_price_comparison_bar,
    compliance_trend_line,
)

st.title("Dashboard")

# ---------------------------------------------------------------------------
# Pre-requisite check
# ---------------------------------------------------------------------------
if not db_has_data():
    st.warning("No data loaded yet. Go to **Load Data** to ingest a dataset first.")
    if st.button("Go to Load Data"):
        st.switch_page("pages/2_Load_Data.py")
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar Filters
# ---------------------------------------------------------------------------
filters = render_filters()
if not filters or "month" not in filters:
    st.info("Select a city and month in the sidebar to begin.")
    st.stop()

city = filters["city"]
month = filters["month"]
room_type = filters["room_type"]
neighbourhoods = filters["neighbourhoods"]

# ---------------------------------------------------------------------------
# Helper to build neighbourhood WHERE clause
# ---------------------------------------------------------------------------
def _neighbourhood_clause(col: str = "neighbourhood") -> str:
    if not neighbourhoods:
        return ""
    placeholders = ", ".join(f"'{n}'" for n in neighbourhoods)
    return f" AND {col} IN ({placeholders})"


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Avg Price by Neighbourhood",
    "Top 10 Overpriced",
    "Top 10 Underpriced",
    "Data Compliance",
])

# ===== TAB 1: Monthly Neighbourhood Avg Price =============================
with tab1:
    st.info(
        "Average nightly price per neighbourhood, from "
        "`fct_neighbourhood_monthly_avg_price`. Geo-flagged listings are "
        "excluded. Room type 'ALL' combines all room types."
    )

    room_filter = "" if room_type == "ALL" else f" AND room_type = '{room_type}'"
    nbh_filter = _neighbourhood_clause()

    avg_df = query_df(
        f"""
        SELECT neighbourhood, neighbourhood_group, room_type,
               avg_price, listing_count, currency_code
        FROM   vw_rep_monthly_neighbourhood_avg_price
        WHERE  city_name = ? AND month_start_date = ?
               {room_filter} {nbh_filter}
        ORDER BY avg_price DESC
        """,
        (city, month),
    )

    if avg_df.empty:
        st.warning("No data for the selected filters.")
    else:
        fig = neighbourhood_avg_price_bar(avg_df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(avg_df, use_container_width=True, hide_index=True)

# ===== TAB 2: Top 10 Overpriced ==========================================
with tab2:
    st.info(
        "Top 10 listings priced highest above their neighbourhood average. "
        "Delta = listing price - neighbourhood avg. Compared per room type and "
        "against the combined 'ALL' average."
    )

    room_filter_top = "" if room_type == "ALL" else f" AND compared_against_room_type = '{room_type}'"
    nbh_filter_top = _neighbourhood_clause()

    over_df = query_df(
        f"""
        SELECT listing_id, property_type, room_type,
               compared_against_room_type, neighbourhood,
               price_amount, neighbourhood_avg_price,
               price_delta, price_delta_pct, rank_in_neighbourhood,
               currency_code
        FROM   vw_rep_monthly_top10_overpriced
        WHERE  city_name = ? AND month_start_date = ?
               {room_filter_top} {nbh_filter_top}
        ORDER BY price_delta DESC
        """,
        (city, month),
    )

    if over_df.empty:
        st.warning("No overpriced listings for the selected filters.")
    else:
        fig = top10_price_comparison_bar(over_df, "OVERPRICED")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(over_df, use_container_width=True, hide_index=True)

        # Listing detail
        listing_ids = over_df["listing_id"].tolist()
        selected_id = st.selectbox("Select a listing for details", listing_ids, key="over_detail")
        if selected_id:
            row = over_df[over_df["listing_id"] == selected_id].iloc[0]
            with st.expander(f"Listing {selected_id} — Details", expanded=True):
                c1, c2 = st.columns(2)
                c1.markdown(f"**Price:** ${row['price_amount']:,.2f}")
                c1.markdown(f"**Neighbourhood Avg:** ${row['neighbourhood_avg_price']:,.2f}")
                c1.markdown(f"**Delta:** ${row['price_delta']:,.2f} ({row['price_delta_pct']:.1f}%)")
                c2.markdown(f"**Neighbourhood:** {row['neighbourhood']}")
                c2.markdown(f"**Room Type:** {row['room_type']}")
                c2.markdown(f"**Compared Against:** {row['compared_against_room_type']}")
                st.caption(
                    f"This listing is priced at ${row['price_amount']:,.2f} vs "
                    f"neighbourhood avg ${row['neighbourhood_avg_price']:,.2f} for "
                    f"{row['compared_against_room_type']}, which is "
                    f"{row['price_delta_pct']:.1f}% above average."
                )

# ===== TAB 3: Top 10 Underpriced =========================================
with tab3:
    st.info(
        "Top 10 listings priced lowest below their neighbourhood average."
    )

    under_df = query_df(
        f"""
        SELECT listing_id, property_type, room_type,
               compared_against_room_type, neighbourhood,
               price_amount, neighbourhood_avg_price,
               price_delta, price_delta_pct, rank_in_neighbourhood,
               currency_code
        FROM   vw_rep_monthly_top10_underpriced
        WHERE  city_name = ? AND month_start_date = ?
               {room_filter_top} {nbh_filter_top}
        ORDER BY price_delta ASC
        """,
        (city, month),
    )

    if under_df.empty:
        st.warning("No underpriced listings for the selected filters.")
    else:
        fig = top10_price_comparison_bar(under_df, "UNDERPRICED")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(under_df, use_container_width=True, hide_index=True)

        listing_ids_u = under_df["listing_id"].tolist()
        selected_id_u = st.selectbox("Select a listing for details", listing_ids_u, key="under_detail")
        if selected_id_u:
            row = under_df[under_df["listing_id"] == selected_id_u].iloc[0]
            with st.expander(f"Listing {selected_id_u} — Details", expanded=True):
                c1, c2 = st.columns(2)
                c1.markdown(f"**Price:** ${row['price_amount']:,.2f}")
                c1.markdown(f"**Neighbourhood Avg:** ${row['neighbourhood_avg_price']:,.2f}")
                c1.markdown(f"**Delta:** ${row['price_delta']:,.2f} ({row['price_delta_pct']:.1f}%)")
                c2.markdown(f"**Neighbourhood:** {row['neighbourhood']}")
                c2.markdown(f"**Room Type:** {row['room_type']}")
                c2.markdown(f"**Compared Against:** {row['compared_against_room_type']}")
                st.caption(
                    f"This listing is priced at ${row['price_amount']:,.2f} vs "
                    f"neighbourhood avg ${row['neighbourhood_avg_price']:,.2f} for "
                    f"{row['compared_against_room_type']}, which is "
                    f"{abs(row['price_delta_pct']):.1f}% below average."
                )

# ===== TAB 4: Data Compliance =============================================
with tab4:
    st.info(
        "Monthly data quality snapshot. A row is compliant when price, "
        "neighbourhood, and room_type are all present. Geo-flagged rows "
        "are excluded from fact-layer counts."
    )

    comp_df = query_df(
        """
        SELECT month_start_date, city_name, rows_count,
               compliance_data_count, missing_price_count,
               missing_neighbourhood_cleansed_count, missing_room_type_count
        FROM   vw_rep_monthly_data_compliance
        WHERE  city_name = ?
        ORDER BY month_start_date DESC
        """,
        (city,),
    )

    if comp_df.empty:
        st.warning("No compliance data for this city.")
    else:
        # Current month KPIs
        current = comp_df[comp_df["month_start_date"] == month]
        if not current.empty:
            r = current.iloc[0]
            total = r["rows_count"] or 0
            compliant = r["compliance_data_count"] or 0
            rate = (compliant / total * 100) if total > 0 else 0

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Compliance Rate", f"{rate:.1f}%")
            k2.metric("Total Rows", f"{total:,}")
            k3.metric("Missing Price", f"{int(r['missing_price_count']):,}")
            k4.metric("Missing Neighbourhood", f"{int(r['missing_neighbourhood_cleansed_count']):,}")
            k5.metric("Missing Room Type", f"{int(r['missing_room_type_count']):,}")

        # Trend chart
        if len(comp_df) > 1:
            fig = compliance_trend_line(comp_df)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

        st.dataframe(comp_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Reconciliation Summary
# ---------------------------------------------------------------------------
st.divider()
with st.expander("Reconciliation Results"):
    st.caption("Cross-checks staging vs fact tables vs reporting views.")

    try:
        rec_avg = query_df(
            """
            SELECT match_status, COUNT(*) as cnt
            FROM rec_avg_price_comparison r
            JOIN dim_city c ON r.city_key = c.city_key
            JOIN dim_date d ON r.month_key = d.month_key
            WHERE c.city_name = ? AND d.month_start_date = ?
            GROUP BY match_status
            """,
            (city, month),
        )
        rec_delta = query_df(
            """
            SELECT match_status, COUNT(*) as cnt
            FROM rec_top10_price_delta_comparison r
            JOIN dim_city c ON r.city_key = c.city_key
            JOIN dim_date d ON r.month_key = d.month_key
            WHERE c.city_name = ? AND d.month_start_date = ?
            GROUP BY match_status
            """,
            (city, month),
        )
        rec_views = query_df(
            """
            SELECT view_name, match_status, fct_row_count, view_row_count
            FROM rec_reporting_view_comparison r
            JOIN dim_city c ON r.city_key = c.city_key
            JOIN dim_date d ON r.month_key = d.month_key
            WHERE c.city_name = ? AND d.month_start_date = ?
            """,
            (city, month),
        )

        r1, r2, r3 = st.columns(3)
        with r1:
            st.markdown("**Avg Price Reconciliation**")
            if not rec_avg.empty:
                st.dataframe(rec_avg, hide_index=True)
            else:
                st.caption("No data")
        with r2:
            st.markdown("**Top 10 Delta Reconciliation**")
            if not rec_delta.empty:
                st.dataframe(rec_delta, hide_index=True)
            else:
                st.caption("No data")
        with r3:
            st.markdown("**View Row Count Reconciliation**")
            if not rec_views.empty:
                st.dataframe(rec_views, hide_index=True)
            else:
                st.caption("No data")

        # Mismatch warning
        all_statuses = []
        for df in [rec_avg, rec_delta]:
            if not df.empty:
                all_statuses.extend(df["match_status"].tolist())
        if not rec_views.empty:
            all_statuses.extend(rec_views["match_status"].tolist())

        if any(s != "MATCH" for s in all_statuses):
            st.warning("Mismatches detected in reconciliation. Review the details above.")
        elif all_statuses:
            st.success("All reconciliation checks passed.")

    except Exception as e:
        st.warning(f"Could not load reconciliation data: {e}")
