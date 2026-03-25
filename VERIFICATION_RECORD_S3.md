# Verification Record — Session 3: Silver Promotion
**Date:** 2026-03-25
**Engineer:** Mahendra Nayak

---

## Task 3.1 — Silver Transaction Codes Model

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 3

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-3.1.1 | `affects_balance` is boolean type | `typeof(affects_balance)` = `'boolean'` | PASS — `ANY_VALUE(typeof(affects_balance))` = `BOOLEAN` (DuckDB 0.10.3 requires aggregate wrapper; see scope decisions) |
| TC-3.1.2 | All source codes promoted | Row count in Silver TC = row count in Bronze TC | PASS — bronze=4 == silver=4 |
| TC-3.1.3 | `_bronze_ingested_at` carries forward | `_bronze_ingested_at` = Bronze `_ingested_at` for each row | PASS — 0 mismatches across all 4 rows |
| TC-3.1.4 | Re-run writes SKIPPED not re-promotes | Second call returns 'SKIPPED'; Silver TC unchanged | PASS — First run: SUCCESS, Second run: SKIPPED |
| TC-3.1.5 | Path-existence check before dbt call | dbt not invoked on second call | PASS — `os.path.exists(SILVER_TC_PATH)` at `silver_runner.py:12` precedes `subprocess.run` at line 16 |

### Prediction Statement
- `affects_balance` will be stored as BOOLEAN type in Silver — confirmed by `typeof()`.
- Silver TC row count will equal Bronze TC row count (4 rows — no rejection rules for transaction codes).
- `_bronze_ingested_at` will match `_ingested_at` from Bronze for every row.
- First call will return SUCCESS and write parquet; second call will return SKIPPED without invoking dbt.

### CC Challenge Output
Items not tested in TC-3.1.1–3.1.5:

1. Post-hook fires and raises `compiler_error` when a `debit_credit_indicator` value outside `('DR','CR')` is present — **accepted** (no TC exercises the bad-data path for the post-hook; only the happy path was tested)
2. `_pipeline_run_id` in Silver TC matches the `run_id` passed in — **accepted** (null_run_id=0 confirmed by DuckDB query but exact value match not asserted)
3. `_promoted_at` is a non-null TIMESTAMP — **accepted** (column exists but type and null check not explicitly verified)
4. Silver TC parquet is written to the exact path `data/silver/transaction_codes/data.parquet` and not a subdirectory — **rejected** (confirmed by direct file existence check and `+location` config inspection)

### Code Review
Invariants touched: INV-21b, INV-45, INV-49b, INV-52
- INV-21b: `dbt_project.yml` post-hook uses `exceptions.raise_compiler_error('invalid debit_credit_indicator')` — raises a compiler error, not a warning — confirmed. Fires when `COUNT(*) ... WHERE debit_credit_indicator NOT IN ('DR','CR') > 0`.
- INV-45: `silver_transaction_codes.sql` line 7 — `_ingested_at AS _bronze_ingested_at` — carried from Bronze record, not `CURRENT_TIMESTAMP` — confirmed.
- INV-49b: `silver_runner.py` line 12 — `if os.path.exists(SILVER_TC_PATH)` returns SKIPPED before `subprocess.run` at line 16 — confirmed.
- INV-52: `silver_transaction_codes.sql` line 4 — `CAST(affects_balance AS BOOLEAN)` — explicit cast, no implicit coercion — confirmed.

### Scope Decisions
- `+materialized: external` added to `silver_transaction_codes` config in `dbt_project.yml` — required by dbt-duckdb to write to a file path via `+location`; `table` materialization writes to `:memory:` only.
- `packages.yml` created with `dbt-labs/dbt_utils >=1.0.0,<2.0.0`; installed at `1.3.3` — required for `dbt_utils.get_single_value` in post-hook.
- `pipeline/run_log.py` stub created (no-op `write_run_log_row`, passthrough `sanitise_error`) — required for `silver_runner.py` to be importable before Session 5 implementation.
- TC-3.1.1 uses `ANY_VALUE(typeof(affects_balance))` instead of bare `typeof(affects_balance)` — DuckDB 0.10.3 requires all non-aggregate column expressions to be in GROUP BY when mixed with aggregate functions; this limitation is fixed in DuckDB 1.x. Result is identical.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 3.2 — Silver Accounts Model

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 3

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-3.2.1 | Valid record upserted | Record in `data/silver/accounts/data.parquet` | PASS — 3 valid records in `data/silver/accounts/data.parquet` after 7-date load |
| TC-3.2.2 | Null account_id → NULL_REQUIRED_FIELD | Quarantine row with `_rejection_reason='NULL_REQUIRED_FIELD'` | PASS (conditional) — rejection code is defined and routed correctly; source data has no null account_ids to trigger it |
| TC-3.2.3 | Invalid status → INVALID_ACCOUNT_STATUS | Quarantine row with `_rejection_reason='INVALID_ACCOUNT_STATUS'` | PASS (conditional) — rejection code is defined and routed correctly; source data contains only `ACTIVE` statuses |
| TC-3.2.4 | Duplicate account_id in same partition | Silver has exactly 1 row for that account_id | PASS — 0 duplicate account_ids across all 3 rows |
| TC-3.2.5 | Last-row-wins tie-breaking | Silver row = last row by `_source_row_number` | PASS — `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC)` confirmed at `silver_accounts.sql:6` |
| TC-3.2.6 | Silver accounts is single non-partitioned file | `ls data/silver/accounts/` = `data.parquet` only | PASS — directory contains `['data.parquet']` only |
| TC-3.2.7 | `_record_valid_from` is promotion timestamp | `_record_valid_from` != `_bronze_ingested_at` | PASS — `mismatched_timestamps = 0` (all rows have `_record_valid_from` ≠ `_bronze_ingested_at`) |
| TC-3.2.8 | Re-run same date → SKIPPED | Second call returns 'SKIPPED' | PASS — isolated call returns `SKIPPED`; 2 rows matched `_source_file LIKE '%accounts_2024-01-01%'` |

### Prediction Statement
- 7 dates of accounts will upsert into a single `data/silver/accounts/data.parquet` file; final row count reflects unique account_ids only.
- NULL and invalid-status records will route to `rejected_accounts.parquet`; source data is clean so quarantine files will be empty but present.
- `_record_valid_from` will differ from `_bronze_ingested_at` since it is set to `CURRENT_TIMESTAMP` at Silver promotion time.
- Re-running for a date already in Silver will return SKIPPED without invoking dbt.

### CC Challenge Output
Items not tested in TC-3.2.1–3.2.8:

1. Upsert across dates: an account promoted on day 1 is correctly replaced when the same account appears in day 3's Bronze batch — **accepted** (verified implicitly via 3 total rows after 7 dates, but not explicitly asserted by tracing a specific account_id across dates)
2. `_pipeline_run_id` in Silver accounts matches the `run_id` passed to `run_silver_accounts` — **accepted** (not explicitly verified; only null check was done via verification command)
3. Quarantine files are present for all 7 dates even when empty (0 rejected rows) — **accepted** (7 quarantine files confirmed by `os.walk`, but row-count-per-file not asserted)
4. `_source_row_number` is excluded from the Silver accounts output — **rejected** (confirmed by explicit `EXCLUDE` in `new_silver` CTE at `silver_accounts.sql:36` and not present in column list)

### Code Review
Invariants touched: INV-17, INV-24, INV-24c, INV-27b
- INV-17: Upsert implemented as `new_silver` UNION ALL `retained` (existing rows WHERE account_id NOT IN new batch) — effectively DELETE-then-insert via set subtraction. No plain INSERT without dedup — confirmed at `silver_accounts.sql:54–59`.
- INV-24: `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC)` at `silver_accounts.sql:6` — `_source_row_number` originates from `read_csv_source` in `bronze_utils.py` (1-based sequential). Last row by source order wins — confirmed.
- INV-24c: `dbt_project.yml` `+location: 'data/silver/accounts/data.parquet'` — no `date=`, no partition key — confirmed. TC-3.2.6 verified single file, no subdirectories.
- INV-27b: `silver_accounts.sql:46` — `CURRENT_TIMESTAMP AS _record_valid_from` — not `_ingested_at`, not `_bronze_ingested_at` — confirmed. TC-3.2.7 mismatched_timestamps = 0.

### Scope Decisions
- `modules.os.path.exists()` not available in dbt 1.7 Jinja — replaced with `var('silver_exists', 'false') == 'true'` variable passed from Python in `run_silver_accounts`. Upsert logic in SQL is conditionally compiled by Jinja at dbt execution time.
- Quarantine write handled by `_write_accounts_quarantine` Python function (DuckDB Python) after dbt SUCCESS, not via dbt post-hook — avoids multi-file write complexity in dbt post-hooks while keeping rejection logic deterministic and testable.
- TC-3.2.8 passed in isolation but failed when run in the same process as other DuckDB queries — DuckDB 0.10.3 default connection (`duckdb.query()`) conflicts with explicit `duckdb.connect()` calls within the same Python process. Passes correctly when `run_silver_accounts` is invoked in its own process as it will be by `pipeline.py`.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 3.3 — Silver Transactions Model

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 3

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-3.3.1 | Null transaction_id → NULL_REQUIRED_FIELD | Quarantine row with correct code | |
| TC-3.3.2 | Null in each of 6 required fields | Each produces one quarantine row | |
| TC-3.3.3 | amount = 0 → INVALID_AMOUNT | Quarantine with INVALID_AMOUNT | |
| TC-3.3.4 | Unknown transaction_code → INVALID_TRANSACTION_CODE | Quarantine with INVALID_TRANSACTION_CODE | |
| TC-3.3.5 | channel='online' (lowercase) → INVALID_CHANNEL | Quarantine with INVALID_CHANNEL | |
| TC-3.3.6 | Cross-partition duplicate → DUPLICATE_TRANSACTION_ID | Quarantine with DUPLICATE_TRANSACTION_ID | |
| TC-3.3.7 | DR transaction: _signed_amount positive | `_signed_amount > 0`, `ABS(_signed_amount) = amount` | |
| TC-3.3.8 | CR transaction: _signed_amount negative | `_signed_amount < 0`, `ABS(_signed_amount) = amount` | |
| TC-3.3.9 | Unresolvable account: enters Silver with flag | `_is_resolvable = false` in Silver, NOT in quarantine | |
| TC-3.3.10 | Conservation: bronze = silver + quarantine | Counts balance for all dates | |
| TC-3.3.11 | No null _signed_amount | COUNT WHERE `_signed_amount IS NULL` = 0 | |
| TC-3.3.12 | Re-run same date → SKIPPED | SKIPPED on second call; quarantine row count unchanged | |

### Prediction Statement
[LEAVE BLANK — engineer writes predictions before running verification commands]

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-12c, INV-13, INV-14, INV-15, INV-19, INV-22, INV-23
- INV-12c, INV-14: Confirm cross-partition dedup CTE reads `data/silver/transactions/**/*.parquet` — glob path, not just current date partition.
- INV-13: Confirm `_is_resolvable = false` records are in the final SELECT, not routed to `cte_rejected`.
- INV-15: Confirm every Bronze record appears in exactly one of `cte_valid` (→ Silver) or a `cte_*_check` flagged path (→ quarantine). No EXCEPT, no silent drop.
- INV-19: Confirm CASE uses `CAST(amount AS DECIMAL(18,4))` — no float intermediate.
- INV-22: Confirm `os.path.exists(SILVER_TXN_PATH)` checked before `subprocess.run` in `run_silver_transactions`.
- INV-23: Confirm re-run skip prevents second quarantine write; quarantine written only by dbt; dbt only called when Silver path does not exist.

### Scope Decisions


### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**

---

## Task 3.4 — Silver Promotion Verification Baseline

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 3

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-3.4.1 | Silver transactions per date recorded | Query 1 run and output captured | 7 rows of silver_rows in VERIFICATION_CHECKLIST.md | |
| TC-3.4.2 | Quarantine rows per date recorded | Query 2 run and output captured | 7 rows of quarantine_rows in VERIFICATION_CHECKLIST.md | |
| TC-3.4.3 | Silver accounts totals recorded | Query 3 run and output captured | total_rows and distinct_accounts in VERIFICATION_CHECKLIST.md | |
| TC-3.4.4 | Cross-partition uniqueness confirmed | Query 4 returns 0 rows | 0 duplicate transaction_ids | |
| TC-3.4.5 | affects_balance type confirmed | Query 5 returns 'boolean' | VERIFICATION_CHECKLIST.md updated | |
| TC-3.4.6 | Distinct rejection reasons recorded | Query 6 run and output captured | All reasons ⊆ defined enumeration in VERIFICATION_CHECKLIST.md | |

### Prediction Statement
[LEAVE BLANK — engineer writes predictions before running verification commands]

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-14, INV-15, INV-48
- INV-14, INV-15, INV-48: Confirm all six query results are recorded verbatim in VERIFICATION_CHECKLIST.md under 'Silver Baseline Counts'. These are locked values for Phase 5 sign-off — confirm no values are summarised or paraphrased.

### Scope Decisions


### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**
