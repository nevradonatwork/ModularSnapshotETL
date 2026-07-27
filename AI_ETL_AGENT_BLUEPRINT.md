# AI ETL Agent Blueprint (Built on `ModularSnapshotETL`)

Yes, this is absolutely possible.

Your current project already has the right building blocks (staging, dimensions/facts, reconciliation tests, and dashboard layer). The best approach is to evolve it into a **multi-step AI workflow** that generates ETL code with guardrails instead of trying to do everything in one LLM prompt.

## 1) Target outcome

Input from user:
- Data dictionary (tables, columns, types, keys, definitions)
- Source file/API details (CSV, DB, API)
- Business metrics and dashboard requirements

Output from agent:
- Config + generated pipeline modules for a new domain
- Schema and transformation logic
- Data-quality checks + reconciliation tests
- Dashboard query layer and chart specs
- Runbook (how to schedule, monitor, and troubleshoot)

## 2) Recommended agent architecture

Use an **orchestrator + specialist agents** pattern.

1. **Planner Agent**
   - Converts requirements into a structured build plan.
   - Produces a domain spec JSON (`domain_name`, entities, metrics, grain, SCD rules, refresh cadence).

2. **Schema Agent**
   - Maps source columns to staging schema + dimensional model.
   - Proposes `dim_*` and `fct_*` tables and grain definitions.

3. **Transformation Agent**
   - Generates ingestion + cleaning + surrogate-key joins + aggregations.
   - Reuses your existing `src/` modules and patterns.

4. **Quality Agent**
   - Auto-generates tests:
     - schema validation
     - null/uniqueness checks
     - reconciliation checks (source vs fact aggregates)
   - Fails generation if quality gates fail.

5. **Dashboard Agent**
   - Builds semantic metric definitions and query helpers.
   - Generates chart config aligned with dashboard requirements.

6. **Reviewer/Critic Agent**
   - Runs static checks and unit tests.
   - Verifies SQL safety and performance constraints.

## 3) Add a "Domain Spec" contract first

Before code generation, force the agent to create a canonical spec, e.g.:

```json
{
  "domain": "retail_sales",
  "grain": "store_id x product_id x month",
  "dimensions": ["date", "store", "product"],
  "facts": ["sales_amount", "units", "avg_price"],
  "source": {
    "type": "csv",
    "files": ["sales.csv", "stores.csv", "products.csv"]
  },
  "quality_rules": [
    "sales_amount >= 0",
    "units >= 0",
    "sum(fact.sales_amount) ~= sum(source.sales_amount) by month"
  ],
  "dashboard": {
    "kpis": ["Revenue", "Units", "Average Price"],
    "filters": ["date", "store_region", "product_category"]
  }
}
```

This makes generation deterministic and testable.

## 4) How to adapt this repository

- Keep `src/` as reusable ETL engine code.
- Add `domains/<domain_name>/` folders for generated artifacts.
- Add templated files the agent can fill:
  - `domains/<domain>/schema.py`
  - `domains/<domain>/pipeline.py`
  - `domains/<domain>/tests/test_reconciliation.py`
  - `domains/<domain>/dashboard/*.py`
- Add `specs/<domain>.json` as the source of truth.

## 5) Agent workflow (practical)

1. Parse data dictionary + dashboard needs.
2. Build and confirm domain spec.
3. Generate scaffold from templates.
4. Generate SQL/Python transforms.
5. Generate tests and reconciliation rules.
6. Execute test suite.
7. If failing, self-correct (max N loops).
8. Return artifact summary + confidence report.

## 6) Guardrails you should enforce

- **No direct execution** of generated SQL without lint + explain checks.
- **Metric grain validation** to avoid double counting.
- **Reconciliation thresholds** (exact or tolerance-based).
- **PII policy checks** (detect sensitive columns and mask rules).
- **Cost/performance checks** (row-count estimates, indexes, partitioning suggestions).

## 7) Tech stack suggestion

- Orchestration: LangGraph / custom state machine
- LLM: GPT-class model with tool-calling
- Validation: pytest + Great Expectations (optional)
- SQL parsing/lint: sqlfluff
- Metadata and lineage: OpenMetadata / simple YAML registry first
- Storage target: start with SQLite/Postgres, expand later

## 8) MVP scope (2–3 weeks)

- Support one input mode: CSV + data dictionary JSON.
- Generate one star schema with 2–3 dims and 1 fact.
- Auto-generate and pass:
  - ingestion tests
  - dimension/fact tests
  - reconciliation tests
- Generate a dashboard-ready metric query layer.

## 9) What "good" looks like

A successful run should produce:
- Generated domain folder
- Green tests
- Reconciliation report
- Dashboard metric spec
- `RUNBOOK.md` for operation

## 10) Why this is feasible for your project

Your project already demonstrates:
- modular ETL structure,
- test coverage around dimensions/facts/reconciliation,
- dashboard integration patterns.

That means you are not starting from zero; you are mostly adding a **spec-driven generator + validation loop** around what you already built.
