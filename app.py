"""ModularSnapshotETL Dashboard — Streamlit entry point."""

import streamlit as st

st.set_page_config(
    page_title="ModularSnapshotETL",
    page_icon=":material/bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Visitor logging: this module-level code reruns on every widget interaction
# and page switch, so it's guarded by st.session_state to log exactly one
# visitor_log row per browser session (insert once, then touch on every
# subsequent rerun). Never let a DB hiccup block the app from rendering.
try:
    from dashboard.db import get_connection
    from src import visitor_log

    _conn = get_connection()
    if "visitor_session_uuid" not in st.session_state:
        st.session_state.visitor_session_uuid = visitor_log.start_visit(_conn)
    else:
        visitor_log.touch_visit(_conn, st.session_state.visitor_session_uuid)
except Exception:
    pass

home = st.Page("pages/1_Home.py", title="Home", icon=":material/home:", default=True)
load = st.Page("pages/2_Load_Data.py", title="Load Data", icon=":material/upload:")
dash = st.Page("pages/3_Dashboard.py", title="Dashboard", icon=":material/bar_chart:")
dictionary = st.Page("pages/4_Data_Dictionary.py", title="Data Dictionary", icon=":material/menu_book:")

pg = st.navigation([home, load, dash, dictionary])
pg.run()
