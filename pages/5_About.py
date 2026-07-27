"""Page 5, About this project."""

import streamlit as st

st.title("ModularSnapshotETL")
st.caption("A monthly Airbnb pricing intelligence pipeline, end to end.")

st.markdown("""
ModularSnapshotETL turns [Inside Airbnb](https://insideairbnb.com/get-the-data/)'s
monthly listing exports, 100+ cities, refreshed every month, into a queryable
pricing warehouse. Pick a city, pull the latest snapshot, and the pipeline
cleans it, models it with history, cross-checks its own output, and hands it
to a dashboard: neighbourhood pricing, the listings furthest above and below
their neighbourhood's average, and a monthly data-quality report per city.

Not a one-off script. A pipeline that's safe to rerun, safe to rerun for one
city without touching another, and honest when something doesn't add up.
""")

st.divider()

st.header("Why I Built It")
st.markdown("""
I wanted a project that shows real data engineering practice, not a toy
example, proper layering, slowly-changing dimensions, idempotent reruns,
reconciliation against its own output, and an actual production database, not
a script that dumps a CSV into a table and calls it done.

Inside Airbnb publishes a rolling monthly export for over a hundred cities,
which gave me a real problem to solve: how do you turn a recurring raw
export into an audited, queryable warehouse, month after month, city after
city, without duplicating data, losing history, or silently dropping rows
when something upstream changes.
""")

st.divider()

st.header("How It's Built")
st.markdown("""
The pipeline is Python and pandas underneath, no Airflow, no Spark, no ORM.
It follows a **medallion architecture**: each month's export lands in a
**bronze** layer, untouched and append-only. It's cleaned, deduplicated, and
geo-validated into **silver**. From there it's modelled into **gold**:
conformed dimensions, including SCD Type 2 history for hosts and listings, so
a changed attribute doesn't erase what it used to be, and fact tables at the
listing/month/city grain. A **metadata** layer tracks every run, reconciles
row counts and checksums between layers, and keeps a watermark of the last
successful load per table.

Postgres ([Neon](https://neon.tech)) backs the live dashboard so data
persists between restarts; the exact same code runs on SQLite locally and in
the test suite, through a small backend-agnostic connection layer.
""")

st.divider()

# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------
st.header("How the Pipeline Works")
st.caption("Five stages, from a raw monthly export to an interactive dashboard.")

stages = [
    ("01", "Ingestion", "A city's `listings.csv.gz` is fetched from Inside Airbnb "
     "(built-in scraper, or manual upload) and landed in the bronze layer, "
     "untouched, append-only, auditable. Nothing is cleaned or interpreted yet."),
    ("02", "Staging & Normalisation", "Listings are cleaned and deduplicated by "
     "`(city, snapshot_month, id)`, prices parsed out of display strings, and "
     "coordinates checked against each city's geographic bounding box. Rows "
     "outside it are flagged, never silently dropped."),
    ("03", "Dimensional Modelling", "Conformed dimensions for date, city, and "
     "neighbourhood, plus SCD Type 2 dimensions for hosts and listings, so "
     "the pipeline knows exactly when a host's response rate or a listing's "
     "room type changed, not just what it is today."),
    ("04", "Reconciliation & Audit", "Independently recomputed values are "
     "compared against the fact tables, and a generic row-count + checksum "
     "check runs at every load stage. A broken rerun or a silent join-key "
     "mismatch shows up in a table, not as a support ticket."),
    ("05", "Interactive Dashboard", "A Streamlit + Plotly dashboard surfaces "
     "neighbourhood pricing, the listings furthest above and below their "
     "neighbourhood average, and a monthly compliance snapshot, for every "
     "city that's been loaded."),
]

for num, title, desc in stages:
    c1, c2 = st.columns([1, 8])
    with c1:
        st.markdown(f"### {num}")
    with c2:
        st.markdown(f"**{title}**")
        st.caption(desc)

st.divider()

# ---------------------------------------------------------------------------
# Tech stack
# ---------------------------------------------------------------------------
st.header("Tech Stack")

t1, t2, t3, t4, t5 = st.columns(5)
with t1:
    st.markdown("**Backend**")
    st.caption("Python · pandas · psycopg2")
with t2:
    st.markdown("**Database**")
    st.caption("Postgres (Neon) · SQLite")
with t3:
    st.markdown("**Frontend**")
    st.caption("Streamlit · Plotly")
with t4:
    st.markdown("**Infra**")
    st.caption("Streamlit Cloud · Neon · GitHub Actions")
with t5:
    st.markdown("**Testing**")
    st.caption("pytest · 90+ tests")

st.divider()

# ---------------------------------------------------------------------------
# About the author
# ---------------------------------------------------------------------------
st.header("Nevra Donat")

st.markdown("""
I've been working in technology for over 20 years across different roles in
engineering. When I started my first role in software, we were still writing
code on notepads. Today I work as a senior data engineer and data architect,
based in London.

With Claude AI, I was able to take everything I've learned across two decades
in tech and build something I actually care about. That's what
ModularSnapshotETL is.
""")
