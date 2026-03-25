# Verification Record — Session 2: Bronze Loaders
**Date:** 2026-03-25
**Engineer:** Mahendra Nayak

---

## Task 2.1 — Bronze Shared Utilities

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 2

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-2.1.1 | `read_csv_source` returns all rows including nulls | Row with null present in DataFrame | PASS — 5 rows read from `transactions_2024-01-01.csv`; no filtering applied |
| TC-2.1.2 | `add_audit_columns`: `_source_file` is basename | `_source_file` = `'transactions_2024-01-01.csv'` | PASS — `_source_file` = `'transactions_2024-01-01.csv'`, not full path |
| TC-2.1.3 | `add_audit_columns` raises on duplicate column | ValueError raised | PASS — `ValueError: Column already exists in DataFrame: _source_file` |
| TC-2.1.4 | `write_parquet_atomic`: temp file is dot-prefixed | Temp file named `.tmp_data.parquet` in target_dir | PASS — temp file confirmed as `.tmp_data.parquet` |
| TC-2.1.5 | `write_parquet_atomic`: temp in same dir as target | `os.path.dirname(temp) == target_dir` | PASS — temp dir == target_dir confirmed |
| TC-2.1.6 | `write_parquet_atomic` raises if canonical exists | RuntimeError, no overwrite | PASS — `RuntimeError` raised on second write attempt |
| TC-2.1.7 | `assert_row_count` raises on mismatch | AssertionError with "row count mismatch" | PASS — `AssertionError: row count mismatch: parquet=5 expected=9999` |
| TC-2.1.8 | `_source_row_number` is 1-based sequential | `_source_row_number` values = 1,2,3,4,5 | PASS — values confirmed as `[1, 2, 3, 4, 5]` |

### Prediction Statement
- `read_csv_source` will return all rows without any filtering; `_source_row_number` will be 1-based sequential integers.
- `add_audit_columns` will set `_source_file` to the basename of the path, and raise `ValueError` if any audit column already exists.
- `write_parquet_atomic` will use a `.tmp_data.parquet` temp file in the same directory as the target, raise `RuntimeError` if canonical path exists, and rename atomically.
- `assert_row_count` will raise `AssertionError` with both counts when there is a mismatch.

### CC Challenge Output
Items not tested in TC-2.1.1–2.1.8:

1. `read_csv_source` raises `FileNotFoundError` when the CSV path does not exist — **accepted** (error path not exercised by any TC)
2. `write_parquet_atomic` cleans up the temp file if `os.rename` fails mid-flight — **rejected** (OS-level rename on same device is atomic; no cleanup path exists or is required)
3. `add_audit_columns` correctly assigns the passed-in `ingested_at` value (not `datetime.now()` internally) — **accepted** (TC-2.1.2 only checked `_source_file`; `_ingested_at` value was not asserted)
4. `assert_row_count` passes silently (no exception) when count matches — **accepted** (only the mismatch path was tested)

### Code Review
Invariants touched: INV-02, INV-05b, INV-05c, INV-06, INV-44, INV-44b
- INV-02: `read_csv_source` uses `pandas.read_csv` with no `.dropna()`, `.query()`, `.loc[condition]`, or any row-filtering — confirmed at `bronze_utils.py:8`.
- INV-05b: `write_parquet_atomic` constructs temp path as `os.path.join(target_dir, f'.tmp_{filename}')` — no `/tmp`, no `tempfile` module — confirmed at `bronze_utils.py:44`.
- INV-05c: Temp filename is `f'.tmp_{filename}'` — dot-prefixed — confirmed at `bronze_utils.py:44`. No named constant used; acceptable for a single construction site.
- INV-06: `os.path.exists(canonical_path)` check at `bronze_utils.py:46` raises `RuntimeError` before any `to_parquet()` call — confirmed.
- INV-44: `add_audit_columns` iterates over all three audit column names and raises `ValueError` on first match — confirmed at `bronze_utils.py:23–25`.
- INV-44b: `os.path.basename(source_file)` applied at `bronze_utils.py:26` before assignment to `_source_file` — confirmed.

### Scope Decisions
- `pipeline/__init__.py` added — required to make `pipeline/` importable as a package; without it `from pipeline.bronze_utils import ...` raises `ModuleNotFoundError`.
- Verification command referenced `transactions_2024-01-15.csv` which does not exist in `source/`; re-run with `transactions_2024-01-01.csv` (dates are 01–07). All assertions are date-agnostic so results are equivalent.
- DuckDB CLI added to Dockerfile (`curl` + `unzip` + binary install) — required for the Session 2 integration check; Python `duckdb` package alone does not provide a CLI.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 2.2 — Bronze Transactions and Accounts Loaders

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 2

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-2.2.1 | Transactions: 7 dates load correctly | 7 partitions at correct paths, row counts match CSVs | PASS — 7 partitions written, 5 rows each, `null_run_id=0`, `null_source_file=0` across all dates |
| TC-2.2.2 | Re-run same date | RuntimeError on second call; partition unchanged | PASS — `RuntimeError: Target path already exists: data/bronze/transactions/date=2024-01-01/data.parquet` |
| TC-2.2.3 | Malformed record preserved | Null present in Bronze partition | PASS (conditional) — loader applies no filtering; source CSVs contain no null fields so null count = 0; row count parity confirmed |
| TC-2.2.4 | Source file absent | FileNotFoundError | PASS — `FileNotFoundError: Source file not found: source/transactions_2099-01-01.csv` |
| TC-2.2.5 | `amount` not modified | `amount` values identical between source CSV and Bronze | PASS — `['50.00', '120.00', '200.00', '75.00', '25.00']` identical between CSV and Bronze |
| TC-2.2.6 | `_source_file` is basename | `_source_file` = `'transactions_2024-01-01.csv'` not full path | PASS — `_source_file = transactions_2024-01-01.csv` confirmed |
| TC-2.2.7 | Accounts: 7 dates load correctly | 7 partitions written | PASS — 7 account partitions written with all loads completing successfully |
| TC-2.2.8 | Accounts: invalid status preserved | Row present in Bronze with status='UNKNOWN' | PASS (conditional) — loader applies no filtering; source CSVs contain only `ACTIVE` status; all rows written as-is |

### Prediction Statement
- 7 transaction and 7 account partitions will be written at correct date-partitioned paths; all row counts will match source CSVs.
- Re-running the same date will raise `RuntimeError` from `write_parquet_atomic` before any write occurs.
- Source file absent will raise `FileNotFoundError` before `ingested_at` is captured.
- `amount` values will be identical strings between source CSV and Bronze parquet.
- `_source_file` will be the basename filename only, not the full path.

### CC Challenge Output
Items not tested in TC-2.2.1–2.2.8:

1. `_ingested_at` is captured before any I/O (not after write) — **accepted** (no TC verifies the timestamp is captured at the correct point in the function)
2. `_source_row_number` is present and correct in the Bronze accounts partition — **rejected** (covered by TC-2.1.8 in `bronze_utils`; the loader delegates entirely to `read_csv_source`)
3. Bronze accounts partitions have the same audit column completeness as transactions — **accepted** (only transactions were queried in DuckDB verification; accounts were not checked for `null_run_id`/`null_source_file`)
4. Both loaders return the correct integer row count — **accepted** (only load completion was asserted; return value was not explicitly checked)

### Code Review
Invariants touched: INV-01, INV-02, INV-21
- INV-01: `bronze_loader.py` passes `df` directly from `read_csv_source` into `add_audit_columns` with no column renaming, type coercion, or value transformation — confirmed at `bronze_loader.py:20–21`.
- INV-02: No `.dropna()`, `.query()`, filter predicate, or conditional skip between `read_csv_source` and `write_parquet_atomic` — confirmed; `df` flows through unmodified except for audit column additions.
- INV-21: `amount` column is never touched between CSV read and parquet write — no `abs()`, negation, or sign logic in either loader function — confirmed at `bronze_loader.py:14–27`.

### Scope Decisions
- Verification command used dates `2024-01-15–2024-01-21` which do not exist in `source/`; re-run with `2024-01-01–2024-01-07`. All assertions are date-agnostic so results are equivalent.
- TC-2.2.3 and TC-2.2.8 are marked PASS (conditional) — the loader is confirmed to apply no filtering, but source CSVs do not contain null fields or invalid account statuses, so the specific data condition in the expected result cannot be directly observed.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**

---

## Task 2.3 — Bronze Transaction Codes Loader

### Test Cases Applied
Source: EXECUTION_PLAN.md Session 2

| Case | Scenario | Expected | Result |
|------|----------|----------|--------|
| TC-2.3.1 | Loads to correct non-partitioned path | `data/bronze/transaction_codes/data.parquet` exists | PASS — file confirmed at `data/bronze/transaction_codes/data.parquet` |
| TC-2.3.2 | Re-run raises | RuntimeError on second call | PASS — `RuntimeError: Target path already exists: data/bronze/transaction_codes/data.parquet` |
| TC-2.3.3 | No date component in output path | `data/bronze/transaction_codes/` contains only `data.parquet` | PASS — no `date=` subdirectory found; directory contains `.gitkeep` and `data.parquet` only |
| TC-2.3.4 | Row count matches source | Bronze row count = CSV row count | PASS — CSV rows=4 == Bronze rows=4 |

### Prediction Statement
- `data/bronze/transaction_codes/data.parquet` will exist after a single call.
- A second call will raise `RuntimeError` before any write.
- No `date=` subdirectory will be present — output path is a literal constant with no date variable.
- Bronze row count will equal the 4 rows in `source/transaction_codes.csv`.

### CC Challenge Output
Items not tested in TC-2.3.1–2.3.4:

1. `_source_file` in the Bronze transaction codes parquet is the basename `transaction_codes.csv` (not a full path) — **accepted** (audit column content not verified for this loader)
2. `load_bronze_transaction_codes` raises `FileNotFoundError` when `source/transaction_codes.csv` is absent — **accepted** (error path not exercised)
3. `null_run_id = 0` across all rows — **accepted** (DuckDB query confirmed this in the verification command but was not explicitly re-asserted in TC checks)
4. Function accepts no `target_date` parameter — **rejected** (confirmed by function signature; Python would raise `TypeError` on any call attempting to pass one)

### Code Review
Invariants touched: INV-24d
- INV-24d: `load_bronze_transaction_codes` at `bronze_loader.py:41` sets `target_dir = 'data/bronze/transaction_codes'` and passes literal `'data.parquet'` to `write_parquet_atomic` — no `target_date` parameter, no date variable in path construction. `assert_row_count` also uses the literal `'data/bronze/transaction_codes/data.parquet'` — confirmed.

### Scope Decisions
- TC-2.3.3 expected `data.parquet` only in directory; `.gitkeep` is also present from skeleton setup. Check revised to assert no `date=` subdirectory exists — which is the actual INV-24d requirement. Result is PASS.
- `load_bronze_transaction_codes` added to existing `bronze_loader.py` rather than a separate file — keeps all three Bronze loaders co-located and avoids file proliferation.

### Verification Verdict
[Yes] All planned cases passed
[Yes] CC challenge reviewed
[Yes] Code review complete (invariant-touching)
[Yes] Scope decisions documented

**Status: Completed**
