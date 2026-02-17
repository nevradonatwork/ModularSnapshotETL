"""ModularSnapshotETL Dashboard — Streamlit entry point."""

import streamlit as st

st.set_page_config(
    page_title="ModularSnapshotETL",
    page_icon=":material/bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

home = st.Page("pages/1_Home.py", title="Home", icon=":material/home:", default=True)
load = st.Page("pages/2_Load_Data.py", title="Load Data", icon=":material/upload:")
dash = st.Page("pages/3_Dashboard.py", title="Dashboard", icon=":material/bar_chart:")
dictionary = st.Page("pages/4_Data_Dictionary.py", title="Data Dictionary", icon=":material/menu_book:")

pg = st.navigation([home, load, dash, dictionary])
pg.run()
