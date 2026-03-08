# Mock Data Dictionary

Synthetic data for testing the PO-Order future state. No real PII — all generated via Faker (Canadian locale) with fixed seeds for reproducibility.

## Usage

```bash
# Generate CSVs
python3 generate_mock_data.py

# Load into DuckDB
python3 create_duckdb.py

# Upload CSVs to Databricks via UI: Catalog > Create Table > Upload File
```

## e-Transfer Tables

| Table | Rows | Description |
|---|---|---|
| `emt_transfers` | 100,000 | Unified transfer table (replaces legacy `emt_transfers_po` + `iemt_payments`) |
| `payment_fi_reference_number` | 200,000 | 2 per transfer: sender FI ref (seq=1) + recipient FI ref (seq=2) |
| `fraudulent_emt_transfers` | 500 | Transfer IDs flagged as fraudulent |
| `scammed_emt_transfers` | 300 | Transfer IDs with fraud category link |
| `fraud_categories` | 5 | Romance Scam, Investment Scam, Phishing, Employment Scam, Other |

## Debit Tables

| Table | Rows | Description |
|---|---|---|
| `idp_acquirers` | 15 | Payment acquirers (Moneris, TD Merchant Services, etc.) |
| `idp_mtd_terminals` | 500 | Merchant terminals with Canadian addresses |
| `idp_mtd_transactions` | 10,000 | Debit transactions linked to terminals |
| `issuers` | 10 | Card issuers (BMO, BNS, RBC, etc.) |
| `tsp_fpan_dpan_history` | 2,000 | Funding PAN to device PAN mappings |

## Data Characteristics

- **Date range**: 2021-01-01 to 2025-12-31
- **Status distribution**: 60% Completed, 20% Sent, 10% Cancelled, 10% Offline
- **Transfer types**: 70% QA, 20% MR, 10% AD
- **FI codes**: Evenly distributed across 14 institutions (CA000001-CA000815)
- **Identity pools**: ~20K sender and ~25K recipient identities reused across transfers, so email/phone/account searches return multiple hits
- **Relational integrity**: All foreign keys are valid (joins return 100%)
