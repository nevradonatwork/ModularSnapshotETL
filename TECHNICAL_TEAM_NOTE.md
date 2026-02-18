# ModularSnapshotETL — Technical Team Note

Repository: https://github.com/nevradonatwork/ModularSnapshotETL  
Live demo dashboard: https://modularsnapshotetl.streamlit.app/  
Production architecture details: `PRODUCTION_DEPLOYMENT.md`

## What this project is
`ModularSnapshotETL` is a modular ETL pipeline for monthly Airbnb-style snapshot data. It loads raw files, applies staging validation/standardisation, builds dimensions and facts, and exposes BI-ready reporting views.

Key qualities:
- Layered data model: raw → staging → dimensions → facts → presentation views
- Idempotent monthly reruns per `(city, snapshot_month)`
- Data quality and reconciliation checks
- Streamlit dashboard for business consumption

## How the technical team can validate it quickly

### 1) Run automated tests (recommended first step)
From repo root:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

What this demonstrates:
- Core pipeline behavior is covered by automated tests
- SQLite-backed ETL transformations and reconciliation logic run end-to-end in test mode

### 2) Run pipeline locally

```bash
python main.py
```

Expected result:
- Creates/updates `ModularSnapshotETL.db`
- Processes city data from `dataset/`
- Populates dimensional/fact/reporting tables and ETL logs

### 3) Run dashboard locally

```bash
streamlit run app.py
```

Open the local URL shown by Streamlit (usually `http://localhost:8501`) to inspect charts and filters.

### 4) Validate hosted dashboard directly
If they only want a quick product review (no local setup), they can open:

https://modularsnapshotetl.streamlit.app/

This is the fastest way to see the reporting experience and check whether KPI/filter behavior matches expectations.

## What to read for deployment and operations
Please read `PRODUCTION_DEPLOYMENT.md` for:
- scheduler/orchestration assumptions
- file archival and traceability model
- run logging/monitoring approach (`etl_run_log`, `etl_error_log`)
- rerun/backfill strategy
- operational assumptions and production constraints

---
If useful, I can also provide a one-page “technical due diligence checklist” tailored for data/platform teams (security, scalability, observability, recovery, and cost controls).
