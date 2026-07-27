"""Page 1, Home / Project Overview."""

import streamlit as st

# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------
st.title("ModularSnapshotETL")
st.markdown(
    "A monthly Airbnb pricing intelligence platform. Listing snapshots for "
    "100+ cities flow through a bronze/silver/gold pipeline, cleaned, "
    "deduplicated, and modelled with history, into a dashboard that surfaces "
    "neighbourhood pricing, over/underpriced listings, and data quality, "
    "month over month."
)

col_a, col_b, col_c, _ = st.columns([1, 1, 1, 2])
with col_a:
    if st.button("Load Data", use_container_width=True, type="primary"):
        st.switch_page("pages/2_Load_Data.py")
with col_b:
    if st.button("View Dashboard", use_container_width=True):
        st.switch_page("pages/3_Dashboard.py")
with col_c:
    if st.button("About this project", use_container_width=True):
        st.switch_page("pages/5_About.py")

st.divider()

# ---------------------------------------------------------------------------
# What the Pipeline Produces
# ---------------------------------------------------------------------------
st.header("What You Get")

c1, c2, c3 = st.columns(3)
with c1:
    st.markdown("**Neighbourhood Pricing**")
    st.caption("Average nightly price by neighbourhood and room type, every month.")
with c2:
    st.markdown("**Over/Underpriced Listings**")
    st.caption("Top 10 listings furthest above and below their neighbourhood average.")
with c3:
    st.markdown("**Data Compliance**")
    st.caption("A quality snapshot per city, missing prices, neighbourhoods, room types.")

st.caption("Curious how it's built? See the **About** page for the full pipeline walkthrough.")
