# Engineering Decision Record

## Context

The case study requires a reliable, auditable pipeline for daily asset-management inputs and trusted portfolio/compliance outputs. Synthetic data only is used.

## Decisions

### 1. Layering
Use Bronze raw tables, Silver accepted/rejected records, and Gold business outputs. Raw inputs are retained with run ID, source filename and source row number.

### 2. Business keys and idempotency
- Transaction business key: `TRANSACTION_ID` in this prototype.
- File-delivery key: SHA-256 hash over the five input file hashes.
- Accepted transaction table has a primary key on transaction ID.
- Identical duplicates are counted and ignored; conflicting records are quarantined.
- In production, use a source event ID or composite business key if transaction IDs are not globally unique, and define correction/version handling with the business.

### 3. Temporal logic
- Position snapshot: latest snapshot on or before the business date.
- Transactions: after the selected snapshot date and on or before the business date.
- Price: latest price on or before the business date, maximum age three calendar days.
- Security master: effective on the business date for valuation; effective on trade date for transaction acceptance.
- Restriction interval: inclusive effective-from/effective-to dates and status ACTIVE.

### 4. Missing or stale prices
Withhold valuation rows with missing, stale, non-positive or currency-mismatched prices. Write a data-quality exception. A production publication policy should decide whether any exception blocks all output or only affected portfolios.

### 5. Late and corrected data
Do not overwrite accepted transactions silently. Compare payload hashes; quarantine conflicting records and require an explicit correction/version process. A production implementation should support as-of restatements and downstream re-publication rules.

### 6. Schema evolution
Validate required column names and types at ingestion. Reject unexpected schema versions until reviewed. Add versioned schemas and backward-compatible mappings for production.

### 7. Failure recovery
Persist run metadata before processing. Keep raw inputs. Use run-scoped Gold outputs so a failed run can be retried without deleting another run's data. Production publication should atomically switch a published-run pointer only after validation and reconciliation pass.

### 8. Security
Credentials come from environment variables, not source code. Use least-privilege DB grants, secret management, TLS/wallet configuration, controlled log access and retention. Synthetic data only.

### 9. Observability
Record run status, elapsed time, counts, duplicate/reject counts, data-quality exceptions, freshness and error text. Production should add structured logs, metrics, alert thresholds and dashboards.

### 10. Scaling
The prototype targets a small assessment dataset. For higher volume:
- Stream CSVs and use bounded `executemany()` batches.
- Index join and date columns based on execution plans.
- Partition large transaction history by business date where justified.
- Avoid row-by-row database round trips; move validations into set-based SQL or bulk operations.
- Benchmark realistic distributions and concurrency before selecting parallel/distributed processing.

## Known limitations

This is a teaching prototype. It does not implement FX conversion, corporate actions, settlement/lot accounting, accrued interest, portfolio-level atomic publication, concurrency locking, or full schema-version management. These must be addressed before production use.
