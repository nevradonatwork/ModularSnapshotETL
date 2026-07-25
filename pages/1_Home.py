"""Page 1 — Home / Project Overview."""

import streamlit as st

# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------
st.title("ModularSnapshotETL Data Platform")
st.markdown(
    "A layered data engineering platform that transforms raw Airbnb-style "
    "listing data into a Postgres (Neon) analytical warehouse — SQLite locally — "
    "with dimensional modelling, data quality controls, and BI-ready reporting views."
)

col_a, col_b, _ = st.columns([1, 1, 3])
with col_a:
    if st.button("Load Data", use_container_width=True):
        st.switch_page("pages/2_Load_Data.py")
with col_b:
    if st.button("View Dashboard", use_container_width=True):
        st.switch_page("pages/3_Dashboard.py")

st.divider()

# ---------------------------------------------------------------------------
# Architecture
# ---------------------------------------------------------------------------
st.header("Architecture")

cols = st.columns(5)
steps = [
    ("CSV Upload", "Raw Layer", "Immutable append-only landing zone (full audit trail)"),
    ("Raw", "Staging Layer", "Cleansed, deduplicated, geo-validated"),
    ("Staging", "Dimensions", "Conformed dims with SCD Type 2 history (host, listing)"),
    ("Dimensions", "Facts", "Monthly snapshots, neighbourhood averages, top-10 deltas, compliance"),
    ("Facts", "Reporting Views", "BI-ready — 4 views powering this dashboard"),
]
for i, (source, target, desc) in enumerate(steps):
    with cols[i]:
        st.markdown(f"**{target}**")
        st.caption(desc)

st.divider()

# ---------------------------------------------------------------------------
# What the Pipeline Produces
# ---------------------------------------------------------------------------
st.header("What the Pipeline Produces")

st.markdown("""
- **Monthly average price per night** by neighbourhood and room type
  `vw_rep_monthly_neighbourhood_avg_price`
- **Top 10 overpriced & underpriced listings** compared against neighbourhood averages
  `vw_rep_monthly_top10_overpriced` / `vw_rep_monthly_top10_underpriced`
- **Monthly data compliance report** — quality snapshot per city
  `vw_rep_monthly_data_compliance`
""")

st.divider()

# ---------------------------------------------------------------------------
# Production Mindset
# ---------------------------------------------------------------------------
st.header("Production Mindset Highlights")

p1, p2, p3 = st.columns(3)
with p1:
    st.markdown("**Idempotent Reruns**")
    st.caption("Safe to rerun any city/month — staging and facts are scoped-delete + insert.")
    st.markdown("**ETL Run Logging**")
    st.caption("Every run tracked in etl_run_log with timestamps, row counts, and error messages.")
with p2:
    st.markdown("**File Archiving**")
    st.caption("Processed files archived with timestamp for full traceability and reprocessing.")
    st.markdown("**Geo Validation**")
    st.caption("Listings validated against city bounding boxes. Flagged rows excluded from facts.")
with p3:
    st.markdown("**Data Compliance**")
    st.caption("Monthly quality snapshot tracking missing prices, neighbourhoods, and room types.")
    st.markdown("**Multi-City Design**")
    st.caption("Auto-discovers city folders. Zero code changes to add a new city.")

st.divider()

# ---------------------------------------------------------------------------
# Tech Stack
# ---------------------------------------------------------------------------
st.header("Tech Stack")

t1, t2 = st.columns(2)
with t1:
    st.markdown("""
    | Component | Technology |
    |-----------|-----------|
    | Language | Python 3.11 |
    | Database | Postgres (Neon) in production, SQLite locally |
    | Data Processing | Pandas |
    | Testing | pytest (70 tests) |
    | Scheduling | cron (2nd of each month) |
    | Dashboard | Streamlit + Plotly |
    """)
with t2:
    st.markdown("**Run locally:**")
    st.code("pip install -r requirements.txt\npython main.py", language="bash")
    st.markdown("**Launch dashboard:**")
    st.code("streamlit run app.py", language="bash")

st.divider()

# ---------------------------------------------------------------------------
# Table Lineage
# ---------------------------------------------------------------------------
st.header("Data Lineage")

st.code("""
listings.csv.gz
  -> raw_listings (append-only, immutable)
    -> stg_listings (cleansed, deduped, geo-flagged)
      -> dim_date, dim_city, dim_neighbourhood, dim_host (SCD2), dim_listing (SCD2)
      -> fct_listing_monthly_snapshot (base fact, geo-flagged excluded)
        -> fct_neighbourhood_monthly_avg_price (per room_type + ALL)
        -> fct_neighbourhood_monthly_top10_price_delta (OVERPRICED / UNDERPRICED)
        -> fct_data_compliance_monthly
          -> vw_rep_monthly_neighbourhood_avg_price
          -> vw_rep_monthly_top10_overpriced
          -> vw_rep_monthly_top10_underpriced
          -> vw_rep_monthly_data_compliance
""", language="text")
