"""Page 4, Data Dictionary & Quality Rules."""

import pandas as pd
import streamlit as st

from dashboard.data_dictionary import COLUMN_DICTIONARY, QUALITY_RULES, CITY_BOUNDARIES

st.title("Data Dictionary")

# ---------------------------------------------------------------------------
# Column Dictionary
# ---------------------------------------------------------------------------
st.header("Column Dictionary")
st.caption("All key columns across the pipeline layers, with types and validation rules.")

dict_df = pd.DataFrame(COLUMN_DICTIONARY)
st.dataframe(dict_df, use_container_width=True, hide_index=True, height=600)

st.divider()

# ---------------------------------------------------------------------------
# Data Quality Rules
# ---------------------------------------------------------------------------
st.header("Data Quality Rules")
st.caption("Validation and quality controls implemented in the pipeline.")

rules_df = pd.DataFrame(QUALITY_RULES)
st.dataframe(rules_df, use_container_width=True, hide_index=True)

st.divider()

# ---------------------------------------------------------------------------
# City Geo Boundaries
# ---------------------------------------------------------------------------
st.header("City Geo Boundaries")
st.caption(
    "Bounding boxes used for geo validation. Listings outside these bounds "
    "are flagged with `geo_out_of_city_flag = 1` in staging and excluded "
    "from fact-layer aggregates."
)

bounds_df = pd.DataFrame(CITY_BOUNDARIES)
st.dataframe(bounds_df, use_container_width=True, hide_index=True)

st.divider()

# ---------------------------------------------------------------------------
# Table Lineage
# ---------------------------------------------------------------------------
st.header("Table Lineage")
st.caption("Data flow from source CSV through all pipeline layers.")

st.code("""
listings.csv.gz
  -> raw_listings (append-only, immutable)
    -> stg_listings (cleansed, deduped, geo-flagged)
      -> dim_date, dim_city, dim_neighbourhood
      -> dim_host (SCD Type 2, tracks attribute changes)
      -> dim_listing (SCD Type 2, tracks attribute changes)
      -> fct_listing_monthly_snapshot (base fact, geo-flagged excluded)
        -> fct_neighbourhood_monthly_avg_price (per room_type + ALL)
        -> fct_neighbourhood_monthly_top10_price_delta (OVERPRICED / UNDERPRICED)
        -> fct_data_compliance_monthly
          -> vw_rep_monthly_neighbourhood_avg_price
          -> vw_rep_monthly_top10_overpriced
          -> vw_rep_monthly_top10_underpriced
          -> vw_rep_monthly_data_compliance
""", language="text")

st.divider()

# ---------------------------------------------------------------------------
# Layer Descriptions
# ---------------------------------------------------------------------------
st.header("Database Layers")

layers = {
    "Raw (`raw_`)": "Immutable landing storage. Source data exactly as received, with tracking metadata. Append-only, rerunning adds a new copy for full audit trail.",
    "Staging (`stg_`)": "Cleansed, standardised, and deduplicated data. Prices parsed to numeric, booleans normalised (t/f to 1/0), geo validation applied. Deduplicated by (city, snapshot_month, id).",
    "Dimension (`dim_`)": "Conformed dimensions: date, city, neighbourhood, host, listing. Hosts and listings use SCD Type 2 (track attribute changes over time with valid_from/valid_to).",
    "Fact (`fct_`)": "Analytical fact tables at listing + month + city grain. Neighbourhood averages broken down by room type + combined 'ALL'. Top-10 over/underpriced per neighbourhood. Compliance counts.",
    "Presentation (`vw_rep_`)": "BI-ready SQL views joining facts and dimensions with human-readable fields. These are the views powering this dashboard.",
    "Reconciliation (`rec_`)": "Post-pipeline integrity checks comparing staging vs fact tables vs reporting views. Catches silent data loss and join mismatches.",
    "ETL Logging (`etl_`)": "Pipeline execution tracking: run status, timestamps, row counts, source/archived file paths, and detailed error/warning log.",
}

for layer, desc in layers.items():
    st.markdown(f"**{layer}**")
    st.caption(desc)
