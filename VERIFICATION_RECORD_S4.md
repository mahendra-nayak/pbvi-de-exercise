**Session:** S4 — Gold Models
**Date:** 2026-03-26
**Engineer:** Mahendra Nayak

---

## Task 4.1 — Gold Daily Summary Model

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 4

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-4.1.1 | One row per resolvable date | 7 rows in Gold daily summary | PASS — `gold_rows = 7` |
| TC-4.1.2 | total_signed_amount matches Silver | `gold.total_signed_amount = SUM(_signed_amount WHERE _is_resolvable=true)` for that date | PASS — `match = true` for all 7 dates; values range from -30.0000 to -800.0000 |
| TC-4.1.3 | unresolvable_count is 0 not NULL when none | `unresolvable_count = 0` | PASS (conditional) — source data has 1 unresolvable per date; `null_unresolvable = 0` confirms COALESCE is applied |
| TC-4.1.4 | unresolvable_count > 0 when some exist | `unresolvable_count > 0` | PASS — all 7 dates show `unresolvable_count = 1` |
| TC-4.1.5 | transactions_by_type has no empty entries | STRUCT has only types present on that date | PASS — 2024-01-01: `{PAYMENT=1, PURCHASE=2}`; 2024-01-03: `{PAYMENT=1, PURCHASE=1, INTEREST=1}`; no zero-count entries |
| TC-4.1.6 | _pipeline_run_id non-null | COUNT WHERE NULL = 0 | PASS — `null_run_id = 0` |
| TC-4.1.7 | Unresolvable excluded from total_transactions | total_transactions = resolvable-only count | PASS — `total_transactions = 3` for all dates (4 Silver rows - 1 unresolvable = 3 resolvable) |

### Prediction Statement
- 7 dates of Silver transactions → 7 rows in Gold daily (all dates have at least 1 resolvable record).
- `total_signed_amount` will match Silver SUM per date (DECIMAL arithmetic, no float).
- Source data has 1 unresolvable record per date → `unresolvable_count = 1` for all dates; COALESCE ensures non-null.
- `transactions_by_type` MAP will contain only types present — no hardcoded zero entries.
- `total_transactions = 3` for all dates (4 Silver rows per date minus 1 unresolvable).

### CC Challenge Output
Items not tested in TC-4.1.1–4.1.7:

1. `_source_period_start` = MIN(transaction_date) and `_source_period_end` = MAX(transaction_date) across all Gold rows — **accepted** (window function columns present but values not explicitly verified)
2. `online_transactions + instore_transactions` = `total_transactions` for every row (all resolvable records are ONLINE or IN_STORE) — **rejected** (confirmed by source data: channel is always ONLINE or IN_STORE for valid Silver records; channel check is enforced at Silver promotion)
3. Re-running `dbt run --select gold_daily_summary` overwrites the staging file (not accumulates rows) — **accepted** (external materialization rewrites the file on each run; not explicitly verified by row count comparison across two runs)
4. `_computed_at` is a non-null TIMESTAMP — **accepted** (column exists; null check not explicitly verified)

### Code Review
Invariants touched: INV-25, INV-26, INV-29, INV-51
- INV-25: `gold_daily_summary.sql` reads only from `data/silver/transactions/**/*.parquet` and `data/silver/transaction_codes/data.parquet` — no Bronze or source CSV paths — confirmed.
- INV-26: `WHERE _is_resolvable = true` applied in `pass1`, `type_counts`, and `pass3` CTEs before `GROUP BY transaction_date` — not as a HAVING clause — confirmed at `gold_daily_summary.sql`.
- INV-29: `pass1` drives the final rows via `FROM pass1 p1 LEFT JOIN pass2 ... LEFT JOIN pass3` — dates with zero resolvable records produce no row in `pass1` and therefore no row in output. No `COALESCE` on `pass1` result — confirmed.
- INV-51: `transactions_by_type` built from `MAP(LIST(transaction_type), LIST(type_count))` over `GROUP BY transaction_date, transaction_type` — no hardcoded type names — confirmed. Output verified: 2 or 3 entries per date matching only present types.

### Scope Decisions
- `MAP_AGG(key, value)` not available in DuckDB 0.10.3 — replaced with `MAP(LIST(transaction_type), LIST(type_count))` aggregate pattern. Result is identical: a `map(varchar, bigint)` with one entry per distinct transaction type per date.
- `transaction_type` not present in Silver transactions schema (silver_transactions.sql selects only `debit_credit_indicator` from the TC join, not `transaction_type`) — Gold model joins Silver transactions to `data/silver/transaction_codes/data.parquet` to resolve `transaction_type`. This is consistent with INV-32 which explicitly states the Gold weekly model joins to silver_transaction_codes.
- Gold model writes to staging path `data/gold/.tmp_daily_summary.parquet` — `pipeline.py` Task 4.3 will perform the atomic rename to `data/gold/daily_summary/data.parquet`. Verification queries in this task read from the staging path directly.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 4.2 — Gold Weekly Account Summary Model

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 4

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-4.2.1 | `week_start_date` always Monday | `DAYOFWEEK(week_start_date)` = 1 for all rows | |
| TC-4.2.2 | `week_end_date` = `week_start_date` + 6 | `(week_end_date - week_start_date)` = 6 for all rows | |
| TC-4.2.3 | `closing_balance` null when no account record | `closing_balance IS NULL` | |
| TC-4.2.4 | `avg_purchase_amount` null when no purchases | `avg_purchase_amount IS NULL` | |
| TC-4.2.5 | `total_purchases` matches Silver COUNT | Matches `COUNT(*) FILTER (WHERE transaction_type='PURCHASE' AND _is_resolvable=true)` for that account/week | |
| TC-4.2.6 | One row per (account_id, week_start_date) | No duplicate `(account_id, week_start_date)` pairs | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-27, INV-30, INV-32b
- INV-27: Confirm closing balance subquery uses `LIMIT 1` with `ORDER BY _record_valid_from DESC` and no `COALESCE` — null must propagate when no rows match
- INV-30: Confirm `DATE_TRUNC('week', ...)` is used (not `'isoweek'`) — verify Monday-start behaviour against dbt-duckdb 1.7.x adapter docs
- INV-32b: Confirm `AVG(CASE WHEN transaction_type='PURCHASE' THEN _signed_amount END)` — no COALESCE wrapping the AVG

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**

---

## Task 4.3 — Gold Atomic Rename in `pipeline.py`

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 4

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-4.3.1 | Successful run: staging renamed to canonical | `data/gold/daily_summary/data.parquet` exists; staging absent | |
| TC-4.3.2 | Failed dbt run: canonical unchanged | Prior canonical file intact; staging removed | |
| TC-4.3.3 | Each Gold dir has exactly one .parquet file | `ls data/gold/daily_summary/ | wc -l` = 1 | |
| TC-4.3.4 | SUCCESS row in run log after rename | Run log has SUCCESS for both Gold models | |
| TC-4.3.5 | `_pipeline_run_id` in Gold = run_id passed in | All Gold rows carry matching run_id | |

### Prediction Statement

### CC Challenge Output
[Paste CC's response to: 'What did you not test in this task?'
For each item: accepted (added case) / rejected (reason).]

### Code Review
Invariants touched: INV-08, INV-28, INV-43b, INV-46, INV-47
- INV-28: Confirm `os.rename()` is in `pipeline.py` — not in any dbt model post-hook or SQL file
- INV-08: Confirm post-rename assertion verifies exactly one `.parquet` file in each Gold directory — no residual `.tmp_` files after rename
- INV-46 (derived from INV-28): Confirm staging cleanup on failure does not touch the canonical path — only `staging_path` is removed
- INV-43b: Confirm `_pipeline_run_id` in Gold rows matches the `run_id` passed into `run_gold()`
- INV-47: Confirm SUCCESS run log rows are written for both Gold models after rename

### Scope Decisions

### Verification Verdict
[ ] All planned cases passed
[ ] CC challenge reviewed
[ ] Code review complete (invariant-touching)
[ ] Scope decisions documented

**Status:**
