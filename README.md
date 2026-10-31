# Customer Transaction Analytics Pipeline

> **HCL Data Engineering Hackathon** — Use Case: `Analyze_Customer_Transactions_UseCase`

A **metadata-driven, replay-safe** Bronze → Silver → Gold data pipeline built on **Google Cloud Platform** (Dataproc / Dataflow + BigQuery) that turns interleaved BankEvalDB extracts into trusted customer transaction insights.

---

## Table of Contents

- [Business Context](#business-context)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Day 0 → Day 1 → Day 2 Processing](#day-0--day-1--day-2-processing)
- [Data Quality Checks](#data-quality-checks)
- [KPI Outputs](#kpi-outputs)
- [SCD Type 2 — Customer & Account History](#scd-type-2--customer--account-history)
- [Onboarding a New Entity](#onboarding-a-new-entity)
- [Running Tests](#running-tests)
- [Assumptions](#assumptions)

---

## Business Context

The bank needs a governed analytics dataset for:
- Customer transaction behaviour analysis
- Personalised marketing (card cross-sell, product recommendations)
- Anomaly detection and compliance/risk review
- Loan portfolio health monitoring

The solution processes **Day 0** (full baseline) plus **Day 1** and **Day 2** incremental deltas
without duplication, producing reusable Gold KPIs that refresh correctly as new files arrive.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          SOURCE (GCS)                                     │
│    day0.txt   day1.txt   day2.txt   (interleaved multi-entity files)     │
└──────────────────────────────────────────────────────────────────────────┘
         │ Dataflow (Beam) OR Dataproc (PySpark)
         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER                                                             │
│  • InterleavedFileParser — entity-order independent                       │
│  • Add: _source_file, _batch_id, _load_timestamp, _row_hash             │
│  • Replay safety: skip rows with existing _row_hash                      │
│  • All columns ingested as STRING                                        │
│  Tables: bronze_customers, bronze_accounts, bronze_transactions, ...     │
└──────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER                                                             │
│  • DataCleansing   — schema-driven type casting, date parsing            │
│  • Deduplicator    — latest record wins per primary key                  │
│  • DQChecker       — 11 configurable validation rules                   │
│    ✓ not_null       ✓ deduplication    ✓ threshold                      │
│    ✓ referential    ✓ date_range       ✓ prefix_match                   │
│    ✓ structuring    ✓ dormant_burst                                      │
│  Output: silver_* (clean)  quarantine_* (failed + reason)               │
└──────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — Star Schema + SCD Type 2                                   │
│                                                                           │
│  Dimensions                                                               │
│    dim_customers (SCD-2)    dim_accounts (SCD-2)                        │
│    dim_branches  (SCD-1)    dim_accounttypes (SCD-1)                    │
│    dim_transactiontypes (SCD-1)                                          │
│                                                                           │
│  Facts                                                                    │
│    fact_transactions  [grain: TransactionID]                             │
│                                                                           │
│  KPI Tables (7 materialised outputs)                                     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
customer-transaction-analytics-pipeline/
├── README.md
├── requirements.txt
├── setup.py
├── config/
│   ├── entities.yaml            # Entity definitions: PK, SCD type, required fields
│   ├── validation_rules.yaml    # DQ rule registry (11 rules)
│   └── schemas/                 # Per-entity JSON schemas (BigQuery types)
│       ├── customers.json
│       ├── accounts.json
│       ├── transactions.json
│       └── ... (6 more)
├── data/
│   ├── day0.txt                 # Full baseline (10 entities)
│   ├── day1.txt                 # Incremental: Customers + Accounts delta
│   └── day2.txt                 # Incremental: Customers + Accounts delta
├── src/
│   ├── parser/
│   │   └── interleaved_parser.py   # Generic [ENTITY:Name] block parser
│   ├── metadata/
│   │   └── registry.py             # Config-driven metadata registry
│   ├── bronze/
│   │   └── ingestion.py            # Replay-safe PySpark Bronze ingestion
│   ├── silver/
│   │   ├── cleansing.py            # Type casting & whitespace trimming
│   │   ├── deduplication.py        # PK-based deduplication
│   │   └── dq_checks.py            # 11 DQ rule implementations
│   ├── gold/
│   │   ├── dimensions.py           # SCD Type 2 dimension builder
│   │   ├── facts.py                # fact_transactions builder
│   │   └── kpis.py                 # 7 KPI outputs
│   └── utils/
│       ├── bq_utils.py             # BigQuery helpers
│       └── gcs_utils.py            # GCS upload/download helpers
├── pipeline/
│   ├── dataflow_pipeline.py        # Apache Beam / Cloud Dataflow pipeline
│   ├── dataproc_pipeline.py        # PySpark / Cloud Dataproc pipeline
│   └── run_pipeline.sh             # Shell script: submit Day 0/1/2 jobs
├── sql/
│   ├── metadata/                   # Entity registry + rule registry DDL
│   ├── bronze/                     # Bronze table DDL
│   ├── silver/                     # Silver table + quarantine DDL
│   └── gold/                       # Dimension, fact, KPI SQL (11 files)
├── tests/
│   ├── test_parser.py              # Parser unit tests
│   ├── test_bronze.py              # Bronze ingestion unit tests
│   ├── test_silver.py              # Silver DQ + dedup tests (PySpark)
│   └── test_gold.py                # Gold SCD-2 logic tests (PySpark)
└── docs/
    ├── architecture.md             # Detailed architecture + layer descriptions
    └── assumptions.md              # Profiling findings, decisions, onboarding guide
```

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python | 3.9+ |
| Apache Spark (PySpark) | 3.5+ |
| Google Cloud SDK (`gcloud`) | latest |
| GCP Project with BigQuery + Dataproc/Dataflow + GCS enabled | — |
| Service account with BigQuery Admin + Dataproc Worker + GCS roles | — |

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/madhuvamsi006/customer-transaction-analytics-pipeline.git
cd customer-transaction-analytics-pipeline
pip install -r requirements.txt
```

### 2. Configure GCP environment

```bash
export PROJECT_ID="my-gcp-project"
export REGION="us-central1"
export GCS_BUCKET="bank-analytics-temp"
export BQ_DATASET="bank_analytics"

# Authenticate
gcloud auth application-default login
```

### 3. Create BigQuery dataset and tables

```bash
bq mk --dataset ${PROJECT_ID}:${BQ_DATASET}

# Run DDL files in order
for sql in sql/metadata/*.sql sql/bronze/*.sql sql/silver/*.sql sql/gold/*.sql; do
  bq query --use_legacy_sql=false < "$sql"
done
```

### 4. Upload source data

```bash
gsutil cp data/day0.txt gs://${GCS_BUCKET}/data/day0.txt
gsutil cp data/day1.txt gs://${GCS_BUCKET}/data/day1.txt
gsutil cp data/day2.txt gs://${GCS_BUCKET}/data/day2.txt
```

### 5. Run the pipeline (all three days)

```bash
chmod +x pipeline/run_pipeline.sh
# Edit GCP variables at the top of the script first
./pipeline/run_pipeline.sh
```

Or run each day individually:

```bash
spark-submit pipeline/dataproc_pipeline.py \
  --input_path data/day0.txt \
  --batch_id BATCH_D0 \
  --load_day 0 \
  --bq_dataset bank_analytics \
  --gcs_bucket ${GCS_BUCKET} \
  --project ${PROJECT_ID} \
  --effective_date 2024-01-01
```

---

## Day 0 → Day 1 → Day 2 Processing

| Day | File Content | Action |
|-----|-------------|--------|
| **Day 0** | Full baseline: 10 entities including Customers (10), Accounts (12), Transactions (23), Branches, AccountTypes, TransactionTypes, Loans, LoanPayments, Cards | Full load → Bronze/Silver/Gold |
| **Day 1** | Customers (4 rows: 2 updated + 2 new: 369, 370) + Accounts (4 rows: 2 updated + 2 new: 481, 482) | SCD-2 MERGE: expire changed, insert new versions + new keys |
| **Day 2** | Customers (4 rows: 2 updated + 2 new: 371, 372) + Accounts (4 rows: 2 updated + 2 new: 483, 484) | SCD-2 MERGE: same pattern |

**Replay safety**: Re-submitting any day's file produces zero duplicate business records.
Each row carries a SHA-256 hash; existing hashes are filtered before writing.

---

## Data Quality Checks

| Rule ID | Name | Entity | Type | Action |
|---------|------|--------|------|--------|
| VR001 | duplicate_customer | Customers | deduplication | quarantine |
| VR002 | missing_email | Customers | not_null | flag |
| VR003 | missing_phone | Customers | not_null | flag |
| VR004 | duplicate_transaction | Transactions | deduplication | suppress |
| VR005 | currency_mismatch | Transactions | referential | flag |
| VR006 | invalid_transaction_date | Transactions | date_range | flag |
| VR007 | sortcode_integrity | Accounts | prefix_match | flag |
| VR008 | negative_balance_overdraft | Accounts | threshold | flag |
| VR009 | high_value_transaction | Transactions | threshold (>₹100K) | flag |
| VR010 | structuring_detection | Transactions | structuring | flag |
| VR011 | dormant_reactivation | Transactions | dormant_burst | flag |

Failed rows are written to `quarantine_*` tables with `_dq_rule_id`, `_dq_rule_name`, and `_dq_failure_reason`.

**Day 0 profiling findings:**
- Customer 3 (Carol Williams): missing email → VR002 flagged
- T021 is a duplicate of T001 (same ReferenceNo REF001) → VR004 suppressed
- T022: transaction on closed account 104 → VR006 flagged
- T023: INR transaction on USD account 202 → VR005 flagged
- T018, T019 on Account 101: near-threshold (₹99K / ₹98.5K) via Branch/ATM within 2 days → VR010 structuring flag
- T005, T009, T013, T020: exceed ₹100K → VR009 flagged
- LP004: failed loan payment → surfaced in KPI 6

---

## KPI Outputs

| KPI Table | Business Question |
|-----------|------------------|
| `kpi_top5_customers` | Who moved the most money through their accounts? |
| `kpi_product_revenue` | Which account types contribute most transaction value? |
| `kpi_inactive_accounts` | Which accounts have had no transaction activity in the last 90 days? |
| `kpi_suspicious_transactions` | Which transactions exceed ₹100,000 or show structuring patterns? |
| `kpi_customer_scd2_history` | How have customer attributes changed across Day 0 → 1 → 2? |
| `kpi_loan_portfolio_health` | Which customers/branches show loan or repayment risk? |
| `kpi_card_adoption` | Which customers have active cards? Who are cross-sell candidates? |

All KPI tables are overwritten on each pipeline run so they always reflect the current state.

---

## SCD Type 2 — Customer & Account History

Entities `Customers` and `Accounts` maintain full version history.

```
CustomerID | CustomerType | eff_start_date | eff_end_date | is_current
----------------------------------------------------------------------
1          | Retail       | 2024-01-01     | 2024-01-15   | FALSE      ← Day 0 version
1          | Premium      | 2024-01-15     | NULL         | TRUE       ← Day 1 update
```

**Surrogate key**: `SHA-256(CustomerID|eff_start_date)[:16]` — deterministic, reproducible.

Query current state: `WHERE is_current = TRUE`
Query historical state at a date: `WHERE eff_start_date <= '2024-01-10' AND (eff_end_date > '2024-01-10' OR eff_end_date IS NULL)`

---

## Onboarding a New Entity

No core code changes required. Three config-only steps:

**Step 1** — Add to `config/entities.yaml`:
```yaml
InsurancePolicies:
  primary_key: PolicyID
  required_fields: [PolicyID, CustomerID, PolicyType, StartDate]
  scd_type: 1
  target_table: insurance_policies
```

**Step 2** — Create `config/schemas/insurance_policies.json`:
```json
{
  "entity": "InsurancePolicies",
  "fields": [
    {"name": "PolicyID", "type": "STRING", "nullable": false},
    {"name": "CustomerID", "type": "INTEGER", "nullable": false},
    {"name": "PolicyType", "type": "STRING", "nullable": true},
    {"name": "StartDate", "type": "DATE", "nullable": false}
  ]
}
```

**Step 3** — Optionally add DQ rules to `config/validation_rules.yaml`.

The parser, Bronze ingestion, Silver cleansing, and DQ checker all pick up the new entity automatically on the next run.

---

## Running Tests

```bash
# Parser tests (no Spark required)
pytest tests/test_parser.py tests/test_bronze.py -v

# All tests (requires local Spark)
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Assumptions

See [`docs/assumptions.md`](docs/assumptions.md) for full details including:
- Interleaved file format
- Day 1/2 transaction delta assumption
- Structuring detection cash proxy
- Currency mismatch handling
- SCD-2 effective date usage
- Complete Day 0 profiling findings

---

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Cloud Dataproc (PySpark) / Cloud Dataflow (Apache Beam) |
| Processing | PySpark 3.5 / Apache Beam 2.55 |
| Storage | Google Cloud Storage (landing) + BigQuery (all layers) |
| Configuration | YAML + JSON (zero hardcoded entity logic) |
| Testing | pytest |
| Language | Python 3.9+ |

---

*HCL Data Engineering Hackathon — Use Case ID: Analyze_Customer_Transactions_UseCase*
