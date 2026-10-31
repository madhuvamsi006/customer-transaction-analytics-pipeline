# Architecture — Customer Transaction Analytics Pipeline

## Data Flow

```
Source Files (GCS)
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (raw ingestion, replay-safe)                     │
│                                                                  │
│  InterleavedFileParser ──► BronzeIngestion                      │
│   • Detects [ENTITY:Name] blocks (any order)                    │
│   • Adds: _source_file, _batch_id, _load_timestamp, _row_hash  │
│   • Skips rows whose _row_hash already exists (idempotent)      │
│                                                                  │
│  Output: bronze_customers, bronze_accounts, bronze_transactions │
│          bronze_branches, bronze_accounttypes, ...              │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (cleansing, DQ, deduplication)                   │
│                                                                  │
│  DataCleansing     — schema-driven type casting, date parsing   │
│  Deduplicator      — keep latest by primary key + _load_ts      │
│  DQChecker         — applies all rules from validation_rules.yaml│
│    • not_null, deduplication, threshold                         │
│    • referential, date_range, prefix_match                      │
│    • structuring_detection, dormant_burst                       │
│                                                                  │
│  Outputs:                                                        │
│    silver_* (clean)   quarantine_* (failed rows + reason)       │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (star schema, SCD Type 2, KPIs)                    │
│                                                                  │
│  Dimensions                                                      │
│    dim_customers  (SCD-2)   dim_accounts  (SCD-2)              │
│    dim_branches   (SCD-1)   dim_accounttypes (SCD-1)           │
│    dim_transactiontypes (SCD-1)                                 │
│                                                                  │
│  Facts                                                           │
│    fact_transactions — grain: 1 row per TransactionID           │
│    Surrogate keys from all dimensions                           │
│                                                                  │
│  KPI Tables (materialised, refresh on each pipeline run)        │
│    kpi_top5_customers        kpi_product_revenue                │
│    kpi_inactive_accounts     kpi_suspicious_transactions        │
│    kpi_customer_scd2_history kpi_loan_portfolio_health          │
│    kpi_card_adoption                                            │
└─────────────────────────────────────────────────────────────────┘
```

## GCP Services

| Service | Role |
|---------|------|
| **Cloud Storage (GCS)** | Source file landing zone; Spark temp storage for BQ writes |
| **Cloud Dataproc** | Managed Spark cluster; runs `dataproc_pipeline.py` |
| **Cloud Dataflow** | Managed Beam runner; runs `dataflow_pipeline.py` for Bronze ingestion at scale |
| **BigQuery** | All layer storage: Bronze (STRING), Silver (typed), Gold (star schema + KPIs) |
| **Cloud Logging** | Pipeline logs from Spark/Beam jobs |

## Replay-Safety Mechanism

Each Bronze row carries a `_row_hash` computed as SHA-256 of all raw field values.

Before writing, the pipeline queries the existing bronze table for all known hashes.
Incoming rows whose hash already exists are filtered out, making re-processing any file
completely idempotent — no duplicate business records are created.

## SCD Type 2 Approach

Entities `Customers` and `Accounts` use SCD Type 2.

**Day 0 (baseline load)**
- All records inserted with `eff_start_date = load date`, `eff_end_date = NULL`, `is_current = TRUE`.
- Surrogate key = SHA-256(natural_key | eff_start_date)[:16].

**Day 1 / Day 2 (incremental)**
1. Incoming records are compared to the current dimension (`is_current = TRUE`).
2. **New key**: INSERT with `is_current=TRUE`.
3. **Changed key** (any tracked column differs): EXPIRE old record (`eff_end_date = today`, `is_current=FALSE`); INSERT new version.
4. **Unchanged key**: no action — existing record retained.

Result: full version history is queryable; prior versions always accessible via `eff_start_date` / `eff_end_date`.

## Configuration-Driven Design

All entity-specific behavior lives in `config/`:

| File | Purpose |
|------|---------|
| `entities.yaml` | Primary keys, SCD type, tracked columns, required fields |
| `validation_rules.yaml` | DQ rules with parameters (thresholds, ref entities, etc.) |
| `schemas/*.json` | Column names and BigQuery types per entity |

**Adding a new entity** (zero core code changes):
1. Add entry to `entities.yaml`.
2. Add `schemas/<entity>.json`.
3. Add any DQ rules to `validation_rules.yaml`.
4. The parser, Bronze, Silver, and Gold layers automatically handle the new entity.

## Incremental Load Decision Matrix

| File contains entity | Day 0 | Day 1 | Day 2 |
|---------------------|-------|-------|-------|
| Customers | ✓ Full | ✓ Delta | ✓ Delta |
| Accounts | ✓ Full | ✓ Delta | ✓ Delta |
| Transactions | ✓ Full | ✗ Not present | ✗ Not present |
| Branches, AccountTypes, etc. | ✓ Full | ✗ Not present | ✗ Not present |

Day 1 and Day 2 files are Customers + Accounts only. Transactions come only from Day 0 baseline.
When future transaction deltas arrive, the same metadata-driven, replay-safe pattern applies.
