# PO Tool

Backend for the Production Orders tool. Queries run against a Databricks SQL Warehouse via the Statement Execution API.

## Structure

```
src/
  core/          Databricks client, queue manager, audit logging, Excel export
  etransfer/     e-Transfer unified query, FI ref resolution, field-selectable export
  debit/         Terminal/merchant/transaction queries, debit export
config/          Settings (env vars) and FI institution mappings (YAML)
templates/       Excel export templates (DPO.xlsx, ETPO.xlsx)
mock_data/       Synthetic data generator and CSVs for Databricks upload
schema.sql       DDL for app-managed tables (run once in Databricks)
demo.ipynb       Interactive notebook for testing all flows
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and fill in your values:
   ```bash
   cp .env.example .env
   ```

3. Generate and upload mock data:
   ```bash
   cd mock_data && python generate_mock_data.py
   ```
   Then upload the CSVs from `mock_data/csv/` to Databricks via Catalog > Create Table > Upload File.

4. Open `demo.ipynb` and run:
   - **Cell 1** — connects to Databricks
   - **Cell 1b** — creates `query_queue`, `query_results`, `audit_log` tables
   - Remaining cells exercise each flow (e-Transfer search, debit lookup, export, etc.)
