# Assumptions, Decisions & Profiling Findings

## 1. Source File Format

**Assumption**: Interleaved entity files use `[ENTITY:EntityName]` as block delimiters.
The line immediately after the delimiter is the CSV header for that entity block.
Blank lines between blocks are ignored.  Entity blocks may appear in any order within a file.

## 2. Day 1 / Day 2 Transaction Assumption

The Day 1 and Day 2 supplied files contain only `Customers` and `Accounts` blocks.
They do not contain a `Transactions` block.

**Decision**: Transaction analysis uses the Day 0 baseline only.
When future transaction delta files arrive, they will be processed using the same
metadata-driven, replay-safe pattern — no code changes required, only a new file.

## 3. Structuring Detection — Cash Proxy

The source data does not include a "cash transaction" flag.

**Proxy assumption**: Deposit transactions via `Channel IN ('Branch', 'ATM')` are treated
as potential cash-equivalent transactions for structuring analysis.
Online/Wire channels are excluded because they leave electronic audit trails
that regulators treat differently.

Threshold parameters (configurable in `validation_rules.yaml`):
- Near-threshold band: ₹95,000 – ₹100,000 (95 % of the ₹100,000 reporting threshold)
- Rolling window: 3 days
- Minimum count to flag: 2 transactions

## 4. High-Value Transaction Threshold

**Threshold**: ₹100,000 (configurable via `VR009.threshold_value` in `validation_rules.yaml`).
This matches a common compliance reporting level; adjust per jurisdiction.

## 5. Currency Handling

Account 202 holds USD; its transactions should also be in USD.
Transaction T023 is flagged by `VR005` (currency mismatch) because it records `INR`
against a `USD` account.

No currency conversion is performed in this pipeline.
All KPI amounts are in the transaction's reported currency. Cross-currency aggregations
should be handled downstream with an FX rate lookup table (out of scope here).

## 6. Inactive Account Definition

An account is considered "inactive" if its last transaction date is more than 90 days ago
(or if it has never transacted). "Active" account status in the master data is a separate
dimension attribute and does not imply recent transactional activity.

## 7. Overdraft / Negative Balance

Account 104 (CustomerID=4, Status=Inactive) has Balance=-500 with AccountType=AT002 (Checking,
OverdraftLimit=1000). Since |-500| < 1000 this does NOT exceed the overdraft limit and passes DQ.
An account would only be flagged if `abs(balance) > overdraft_limit`.

## 8. Duplicate Transaction (T021)

Transaction T021 has the same ReferenceNo as T001 (`REF001`) and identical Amount/Channel.
`VR004` (duplicate_transaction) flags this. The duplicate is suppressed in Silver; T001 is kept.

## 9. SCD-2 Effective Date

The effective date for SCD-2 versioning is taken from the `--effective_date` pipeline argument,
defaulting to the current date. For Day 0 this is set to `2024-01-01`; Day 1 `2024-01-15`;
Day 2 `2024-02-01`. This allows controlled backdating for historical loads.

## 10. Surrogate Key Generation

Surrogate keys are the first 16 hex characters of `SHA-256(natural_key|eff_start_date)`.
This is deterministic and reproducible — re-running with the same inputs yields the same keys.

## 11. Onboarding a New Entity

To add, say, `InsurancePolicies`:

1. Add to `config/entities.yaml`:
   ```yaml
   InsurancePolicies:
     primary_key: PolicyID
     required_fields: [PolicyID, CustomerID, PolicyType]
     scd_type: 1
     target_table: insurance_policies
   ```
2. Create `config/schemas/insurance_policies.json` with field definitions.
3. Add any DQ rules to `config/validation_rules.yaml`.
4. Include `[ENTITY:InsurancePolicies]` blocks in source files.

No changes to `interleaved_parser.py`, `bronze/ingestion.py`, `silver/`, or `gold/` are needed.
The pipeline automatically discovers the new entity from the config.

## 13. Profiling Findings from Day 0

| Finding | Entity | Impact | Handled By |
|---------|--------|--------|------------|
| Missing email for Customer 3 (Carol) | Customers | VR002 flag | Silver quarantine |
| T021 duplicate of T001 (same ReferenceNo+Amount) | Transactions | VR004 suppress | Silver dedup |
| T022: withdrawal on closed account (AccountID=104) | Transactions | VR006 flag | Silver DQ |
| T023: USD account with INR transaction (AccountID=202) | Transactions | VR005 flag | Silver DQ |
| T018, T019: Near-threshold deposits on Account 101 within 2 days | Transactions | VR010 flag | Silver DQ |
| T005, T009, T013, T020: exceed ₹100K | Transactions | VR009 flag | Silver DQ → Gold KPI |
| Account 104 Balance=-500, OverdraftLimit=1000 (within limit) | Accounts | No flag | Pass |
| LP004: Failed loan payment for Loan L004 | LoanPayments | Repayment risk | Gold KPI |
