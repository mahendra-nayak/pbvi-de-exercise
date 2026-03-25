# EXECUTION_PLAN.md
## Credit Card Financial Transactions Lake
**Version:** 2.0
**Phase:** 3 — Execution Planning
**Sources:** Requirements Brief v1.0 · ARCHITECTURE.md v1.0 · INVARIANTS.md v2.0

---

## Section 1 — Resolved Open Questions

The following six questions were open before Phase 3 could proceed. Each is closed here with a concrete decision that directly governs at least one task's CC prompt or verification command.

---

### RQ-1 — Watermark Advancement Timing

**Question:** After which layer does the watermark advance? What is the exact recovery behaviour on failure?

**Decision:** The watermark advances as the **last statement** in the per-date processing loop in `pipeline.py`, after both Gold dbt models have completed and `pipeline.py` has confirmed dbt exit code 0 and performed the Gold atomic rename. The sequence for each date is: Bronze accounts → Bronze transactions → Silver accounts → Silver transactions → Gold (both models, both renames) → `write_watermark(date)`. If any step raises before `write_watermark` executes, the watermark stays at the last fully completed date. On the next run, `max(start_date, watermark + 1 day)` determines the resume point, and any layers whose output paths already exist are SKIPPED.

**Source:** ARCHITECTURE.md §8 `pipeline/control.parquet` constraint ("advances only after Bronze, Silver, and Gold all complete with status SUCCESS"); D-8; R-2 mitigation.

---

### RQ-2 — Accounts Upsert Definition of "Latest"

**Question:** If two delta records for the same `account_id` arrive in the same Bronze partition, which record wins in Silver?

**Decision:** The record appearing **last by source CSV row order** wins. The Bronze accounts loader adds a `_source_row_number` column (sequential integer, 1-based, assigned by `pandas.DataFrame.reset_index(drop=False)` before writing). The `silver_accounts` dbt model uses `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC)` to select the last record per `account_id` when deduplicating within a single Bronze partition. This makes the selection deterministic and reproducible given the same Bronze input.

**Source:** INVARIANTS.md INV-24 ("last record by source file row order"); ARCHITECTURE.md D-10.

---

### RQ-3 — Historical Pipeline Re-Run Behaviour

**Question:** If the historical pipeline is invoked for a date range already fully processed, does it error, produce a no-op, or attempt an idempotent overwrite?

**Decision:** **Idempotent no-op.** The historical pipeline resumes from `max(start_date, watermark + 1 day)`. If `watermark >= end_date`, `pipeline.py` prints `"nothing to do: all dates already processed (watermark={watermark})"` and exits 0. No layers are touched. No run log rows are written. This is not an error state — it is the normal terminal state of a re-invoked historical pipeline.

**Source:** ARCHITECTURE.md D-8; INVARIANTS.md INV-35.

---

### RQ-4 — Incremental Pipeline Behaviour When No Source File Exists

**Question:** If `source/transactions_{watermark+1}.csv` does not exist, what does the incremental pipeline do?

**Decision:** `pipeline.py` exits 0 with the message `"no source file for {date} — pipeline is current"`. The watermark is not advanced. No run log row is written (there is no model execution to log). No layer files are touched. This is the normal terminal state of an up-to-date incremental pipeline — it is not an error. The incremental pipeline checks for the transactions file specifically; absence of the accounts file for the same date is also a prerequisite and treated the same way.

**Source:** ARCHITECTURE.md D-8; INVARIANTS.md INV-36.

---

### RQ-5 — Silver Transaction Codes Loading Conditionality

**Question:** Is Silver TC loading conditional on historical mode only, or does the incremental pipeline also check for its presence?

**Decision:** Both pipeline paths check for the Silver TC file's existence via `os.path.exists('silver/transaction_codes/data.parquet')` before any potential promotion. If the file exists, **both** historical and incremental paths write a SKIPPED run log row and proceed. If the file does not exist (only possible on first historical run), promotion executes. In practice, after historical initialisation, every incremental run writes a SKIPPED row for `silver_transaction_codes`. The incremental pipeline does **not** include `silver_transaction_codes` in its dbt model selection list — the SKIPPED row is written by `pipeline.py` based on the path-existence result alone, without invoking dbt.

**Source:** INVARIANTS.md INV-24b; ARCHITECTURE.md A-3.

---

### RQ-6 — Gold Recomputation Strategy

**Question:** Full recompute on every run, or incremental append/merge for closed weeks?

**Decision:** **Full recompute on every run.** Gold dbt models read all Silver transactions partitions on every invocation (`read_parquet('silver/transactions/**/*.parquet', union_by_name=true)`) and compute all aggregations from scratch. The prior canonical Gold file remains at the canonical path until the new computation is fully written and `pipeline.py` performs the atomic rename. A late-arriving Silver record from a prior date is automatically reflected in Gold on the next run. There is no closed-week detection, no incremental Gold append, and no partial recomputation. This is stated explicitly in ARCHITECTURE.md §8 for both Gold models: "Full recompute on each run. Atomic overwrite."

**Impact:** CC prompts for Gold models must explicitly state the `**/*.parquet` source glob. The atomic rename in `pipeline.py` must handle the case where no prior canonical file exists (first run) and the case where one does (subsequent runs). Both cases use `os.rename(staging_path, canonical_path)`.

**Source:** ARCHITECTURE.md §8 `gold_daily_summary` and `gold_weekly_account_summary`; D-6; INV-28.

---

## Section 2 — Session Overview

| Session | Name | Goal | Tasks | Est. Duration |
|---------|------|------|-------|---------------|
| S1 | Infrastructure | `docker compose build` succeeds; `dbt debug` passes; `pipeline.py --help` runs; directory skeleton present | 3 | 2–3 h |
| S2 | Bronze Loaders | All 3 Bronze loaders write correct Parquet partitions for all 7 dates; row counts match source CSVs; audit columns present; idempotency verified | 3 | 3–4 h |
| S3 | Silver Promotion | All Silver models promoted; Silver + quarantine = Bronze; no duplicate `transaction_id`; all rejection codes valid; `affects_balance` boolean; `_signed_amount` signed correctly | 4 | 5–6 h |
| S4 | Gold Models | Both Gold models produce correct aggregations; atomic rename by `pipeline.py`; `unresolvable_count` present; ISO week boundaries correct | 3 | 3–4 h |
| S5 | Pipeline Orchestration and Sign-Off | Full `pipeline.py` (historical + incremental + watermark control + PID + run log); end-to-end verification; idempotency confirmed; Phase 8 sign-off conditions met | 5 | 5–6 h |

---

## Section 3 — Per-Session Detail

---

## Session 1 — Infrastructure

**Session Goal**
`docker compose build` succeeds. `docker compose run --rm pipeline dbt debug --profiles-dir /app/dbt_project` exits 0. `docker compose run --rm pipeline python pipeline.py --help` exits 0. The `data/` directory skeleton exists. The `source/` mount is read-only inside the container. No Bronze, Silver, or Gold code exists yet.

**Integration Check**
```bash
docker compose build --no-cache 2>&1 | tail -1 && \
docker compose run --rm pipeline bash -c "
  python pipeline.py --help &&
  dbt debug --profiles-dir /app/dbt_project --project-dir /app/dbt_project &&
  python -c \"import os; assert os.stat('source').st_dev == os.stat('data').st_dev, 'FAIL: mounts differ'\" &&
  echo 'S1 INTEGRATION PASS'
"
```

---

### Task 1.1 — Repository Skeleton, Docker Compose, and Requirements

**Description**
Creates every file needed before any Python or SQL is written: `docker-compose.yml`, `Dockerfile`, `requirements.txt`, the `data/` directory tree with `.gitkeep` files, and `.gitignore`. Does NOT write `pipeline.py`, any dbt files, or any loader. The `source/` bind-mount must use the `ro` flag.

**CC Prompt**
```
Create the project skeleton for a Python/dbt/DuckDB pipeline. No Python logic
and no SQL logic — skeleton files only.

Files to create:

docker-compose.yml:
  Service named 'pipeline', image python:3.11-slim (or built from Dockerfile).
  Bind-mounts:
    ./source  → /app/source  (read-only: 'ro')
    ./data    → /app/data    (read-write)
    ./        → /app         (read-write, for code)
  working_dir: /app
  environment: PYTHONPATH=/app

Dockerfile:
  FROM python:3.11-slim
  WORKDIR /app
  COPY requirements.txt .
  RUN pip install --no-cache-dir -r requirements.txt
  COPY . .

requirements.txt:
  dbt-core==1.7.*
  dbt-duckdb==1.7.*
  duckdb==0.10.*
  pyarrow==15.*
  pandas==2.*

Create these empty directories (with .gitkeep files so git tracks them):
  data/bronze/transactions/
  data/bronze/accounts/
  data/bronze/transaction_codes/
  data/silver/transactions/
  data/silver/accounts/
  data/silver/transaction_codes/
  data/silver/quarantine/
  data/gold/daily_summary/
  data/gold/weekly_account_summary/
  data/pipeline/

.gitignore:
  __pycache__/
  *.pyc
  *.duckdb
  *.duckdb.wal
  .env
  data/**/*.parquet

Do NOT create pipeline.py. Do NOT create any dbt files.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-1.1.1 | Build succeeds | `docker compose build` | Exit 0 | — |
| TC-1.1.2 | source/ is read-only | `touch /app/source/x.txt` inside container | Permission denied, exit non-zero | INV-04 |
| TC-1.1.3 | data/ is writable | `touch /app/data/x.txt` inside container | Exit 0, file created | — |
| TC-1.1.4 | source/ and data/ on same mount device | `python -c "import os; assert os.stat('source').st_dev == os.stat('data').st_dev"` | Exit 0 | INV-05b, A-2 |

**Verification Command**
```bash
docker compose build --no-cache && \
docker compose run --rm pipeline bash -c "touch source/test.txt" 2>&1 | grep -i "read-only\|permission" && \
docker compose run --rm pipeline python -c "
import os
assert os.stat('source').st_dev == os.stat('data').st_dev, 'FAIL: different devices'
print('PASS: same device')
" && \
docker compose run --rm pipeline ls data/bronze/transactions data/silver/quarantine data/pipeline
```

**Invariant Flag**
- **INV-04** — `docker-compose.yml`: `source/` volume must have `read_only: true` or `:ro` suffix. Code review confirms no `rw` on the source mount.
- **INV-05b, A-2** — Both mounts must resolve to the same Docker overlay device. The `st_dev` check above is the machine-readable confirmation.

---

### Task 1.2 — dbt Project Configuration and Model Stubs

**Description**
Creates the dbt project directory with `dbt_project.yml`, `profiles.yml`, and one stub `.sql` file per model (each containing `SELECT 1 AS stub`). `dbt debug` must pass after this task. Does NOT write any SQL logic. Does NOT write `pipeline.py`.

**CC Prompt**
```
Create the dbt project configuration. All model files are stubs — do not write
any SQL logic yet.

Files to create:

dbt_project/dbt_project.yml:
  name: cc_transactions_lake
  version: '1.0.0'
  config-version: 2
  profile: cc_transactions_lake
  model-paths: ['models']
  models:
    cc_transactions_lake:
      silver:
        +materialized: table
      gold:
        +materialized: table

dbt_project/profiles.yml:
  cc_transactions_lake:
    target: dev
    outputs:
      dev:
        type: duckdb
        path: ':memory:'
        threads: 1

Model stub files (each containing exactly: SELECT 1 AS stub):
  dbt_project/models/silver/silver_transaction_codes.sql
  dbt_project/models/silver/silver_accounts.sql
  dbt_project/models/silver/silver_transactions.sql
  dbt_project/models/gold/gold_daily_summary.sql
  dbt_project/models/gold/gold_weekly_account_summary.sql

Do NOT write any SQL logic in model files.
Do NOT configure unique_key or incremental materialisation anywhere.
Do NOT write pipeline.py.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-1.2.1 | `dbt debug` passes | `dbt debug --profiles-dir /app/dbt_project` | Exit 0 | — |
| TC-1.2.2 | No incremental config anywhere | `grep -r "unique_key\|incremental" dbt_project/` | Zero matches | INV-49b |
| TC-1.2.3 | Five stub model files exist | `ls dbt_project/models/silver/ dbt_project/models/gold/` | 5 `.sql` files | — |

**Verification Command**
```bash
docker compose run --rm pipeline bash -c "
  dbt debug --profiles-dir /app/dbt_project --project-dir /app/dbt_project
" && \
grep -rn "unique_key\|incremental" dbt_project/ \
  && echo "FAIL: forbidden config found" \
  || echo "PASS: no forbidden config"
```

**Invariant Flag**
- **INV-49b** — No `unique_key` or `materialized: incremental` in any dbt config. Code review confirms `dbt_project.yml` uses only `table` materialisation.

---

### Task 1.3 — `pipeline.py` Skeleton

**Description**
Creates `pipeline.py` with: argument parser (`--historical`, `--incremental`, `--reset-watermark`), UUID generation at startup, and `--help` output. No data processing logic. No Bronze, Silver, Gold, or watermark calls. Produces the single file `pipeline.py` at the project root.

**CC Prompt**
```
Create pipeline.py at the project root. This is the skeleton only — no data
processing logic yet.

Requirements:

1. At the top of main(), before any other logic:
   import uuid
   run_id = str(uuid.uuid4())
   (run_id must be the first assignment in main())

2. Argument parser (argparse) with three mutually exclusive sub-commands:
   --historical --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   --incremental   (no arguments)
   --reset-watermark YYYY-MM-DD --confirm   (--confirm is a flag, not a value)

3. If no sub-command is provided, print help and exit 0.

4. For each sub-command, print:
   f"[{sub-command}] run_id={run_id} (not yet implemented)"
   and exit 0.

Do NOT implement any Bronze, Silver, Gold, watermark, PID file,
or run log logic. This task creates the skeleton only.
Do NOT import any pipeline submodules.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-1.3.1 | `--help` exits 0 | `python pipeline.py --help` | Exit 0, help text printed | — |
| TC-1.3.2 | UUID printed on any invocation | `python pipeline.py --incremental` | Output contains a valid UUID v4 | INV-43a |
| TC-1.3.3 | run_id is first assignment in main() | Code review | No logic before `run_id = str(uuid.uuid4())` | INV-43a |

**Verification Command**
```bash
docker compose run --rm pipeline python pipeline.py --help && \
docker compose run --rm pipeline python pipeline.py --incremental \
  | grep -E '[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}' \
  && echo "PASS: UUID present" || echo "FAIL: UUID not found"
```

**Invariant Flag**
- **INV-43a** — Code review must confirm: `run_id = str(uuid.uuid4())` is the first executable line in `main()`. No argument parsing, no file I/O, no imports before it.

---

## Session 2 — Bronze Loaders

**Session Goal**
All three Bronze loaders write correct, atomic, immutable Parquet partitions. For every entity and date, the Bronze Parquet row count equals the source CSV row count. Audit columns `_source_file`, `_ingested_at`, `_pipeline_run_id` are present and correctly populated. Re-running any loader for an already-present partition raises without writing. Source files are never modified.

**Integration Check**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
-- Row count parity: Bronze transactions vs source CSVs
SELECT
  regexp_extract(filename, 'date=([0-9-]+)', 1) AS date,
  COUNT(*) AS bronze_rows
FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true)
GROUP BY 1
ORDER BY 1;

-- Audit column completeness across all Bronze partitions
SELECT
  COUNT(*) FILTER (WHERE _source_file IS NULL)   AS null_source_file,
  COUNT(*) FILTER (WHERE _ingested_at IS NULL)   AS null_ingested_at,
  COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL) AS null_run_id
FROM read_parquet('data/bronze/transactions/**/*.parquet');
"
```
Expected: 7 rows with non-zero `bronze_rows`; all three null counts = 0.

---

### Task 2.1 — Bronze Shared Utilities

**Description**
Creates `pipeline/bronze_utils.py` with four functions used by all Bronze loaders: `read_csv_source`, `add_audit_columns`, `write_parquet_atomic`, `assert_row_count`. Does NOT call any of these functions — no loader logic here. Does NOT import from other pipeline modules. Returns are typed.

**CC Prompt**
```
Create pipeline/bronze_utils.py with exactly the following four functions.
No other functions. No Bronze loader logic.

1. def read_csv_source(csv_path: str) -> pandas.DataFrame
   Open csv_path in read mode ('r'). Read ALL rows — no filtering, no dropna,
   no drop_duplicates. Add column _source_row_number as 1-based integer row
   position (df.reset_index() before rename; 1-indexed).
   Raise FileNotFoundError if csv_path does not exist.

2. def add_audit_columns(
       df: pandas.DataFrame,
       source_file: str,
       run_id: str,
       ingested_at: datetime
   ) -> pandas.DataFrame
   Add to df (raise ValueError if any already exists):
     _source_file = os.path.basename(source_file)   — filename only, not path
     _ingested_at = ingested_at                      — passed in, not generated here
     _pipeline_run_id = run_id                       — passed in, not generated here
   Return df.

3. def write_parquet_atomic(
       df: pandas.DataFrame,
       target_dir: str,
       filename: str = 'data.parquet'
   ) -> int
   Canonical path = os.path.join(target_dir, filename)
   Temp path = os.path.join(target_dir, f'.tmp_{filename}')   — dot-prefixed
   Raise RuntimeError if canonical path already exists (do not overwrite).
   os.makedirs(target_dir, exist_ok=True)
   Write df to temp path using df.to_parquet(temp_path, index=False).
   os.rename(temp_path, canonical_path)   — atomic
   Return len(df).

4. def assert_row_count(parquet_path: str, expected: int) -> None
   count = duckdb.query(
       f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
   ).fetchone()[0]
   If count != expected:
       raise AssertionError(
           f"row count mismatch: parquet={count} expected={expected}"
       )

Do NOT generate UUIDs, timestamps, or run_id inside any function.
Do NOT call these functions from this file.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-2.1.1 | `read_csv_source` returns all rows including nulls | CSV with null in required field | Row with null present in DataFrame | INV-02 |
| TC-2.1.2 | `add_audit_columns`: `_source_file` is basename | `source_file='/app/source/transactions_2024-01-15.csv'` | `_source_file` = `'transactions_2024-01-15.csv'` | INV-44b |
| TC-2.1.3 | `add_audit_columns` raises on duplicate column | df with `_source_file` already present | ValueError raised | INV-44 |
| TC-2.1.4 | `write_parquet_atomic`: temp file is dot-prefixed | Any call | Temp file named `.tmp_data.parquet` in target_dir | INV-05c |
| TC-2.1.5 | `write_parquet_atomic`: temp in same dir as target | Any call | `os.path.dirname(temp) == target_dir` | INV-05b |
| TC-2.1.6 | `write_parquet_atomic` raises if canonical exists | Call when target present | RuntimeError, no overwrite | INV-06 |
| TC-2.1.7 | `assert_row_count` raises on mismatch | parquet=5 rows, expected=6 | AssertionError with "row count mismatch" | INV-03 |
| TC-2.1.8 | `_source_row_number` is 1-based sequential | CSV with 5 rows | _source_row_number values = 1,2,3,4,5 | RQ-2 |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
import pandas as pd, os, tempfile
from pipeline.bronze_utils import (
    read_csv_source, add_audit_columns, write_parquet_atomic, assert_row_count
)
from datetime import datetime, timezone
import uuid

# TC-2.1.1: nulls pass through
df = read_csv_source('source/transactions_2024-01-15.csv')
assert df.shape[0] > 0, 'empty DataFrame'
print(f'PASS TC-2.1.1: {df.shape[0]} rows read')

# TC-2.1.2: basename
df2 = add_audit_columns(df.copy(), 'source/transactions_2024-01-15.csv', str(uuid.uuid4()), datetime.now(timezone.utc))
assert df2['_source_file'].iloc[0] == 'transactions_2024-01-15.csv', 'FAIL: full path in _source_file'
print('PASS TC-2.1.2: basename correct')

# TC-2.1.6: raises on existing path
import os, tempfile
td = tempfile.mkdtemp(dir='data')
write_parquet_atomic(df2, td, 'data.parquet')
try:
    write_parquet_atomic(df2, td, 'data.parquet')
    print('FAIL TC-2.1.6: should have raised')
except RuntimeError:
    print('PASS TC-2.1.6: raises on existing path')
import shutil; shutil.rmtree(td)
print('ALL PASS')
"
```

**Invariant Flag**
- **INV-02** — `read_csv_source`: code review confirms no `.dropna()`, `.query()`, `.loc[condition]`, or any row-filtering construct.
- **INV-05b** — `write_parquet_atomic`: confirm `os.path.join(target_dir, ...)` for temp path — no `/tmp`.
- **INV-05c** — Confirm temp filename constant begins with `.tmp_` — named constant preferred (`TEMP_PREFIX = '.tmp_'`).
- **INV-06** — Confirm existence check raises before any `to_parquet()` call.
- **INV-44, INV-44b** — `add_audit_columns`: confirm `os.path.basename()` applied; confirm pre-existing column raises.

---

### Task 2.2 — Bronze Transactions and Accounts Loaders

**Description**
Creates `pipeline/bronze_loader.py` with two public functions: `load_bronze_transactions(target_date, run_id)` and `load_bronze_accounts(target_date, run_id)`. Both use `bronze_utils` exclusively for I/O. Both write to date-partitioned paths. Neither applies any filtering or type coercion to source data. The `_source_row_number` column added by `read_csv_source` is carried through to Bronze (it is a source-fidelity column, not an audit column).

**CC Prompt**
```
Create pipeline/bronze_loader.py with two public functions.

Both functions follow the same pattern:
  1. Capture ingested_at = datetime.now(timezone.utc) once, before any I/O.
  2. Call read_csv_source(csv_path) → df
  3. Call add_audit_columns(df, csv_path, run_id, ingested_at) → df
  4. Call write_parquet_atomic(df, target_dir) → row_count
  5. Call assert_row_count(canonical_path, row_count)
  6. Return row_count

def load_bronze_transactions(target_date: str, run_id: str) -> int:
  csv_path   = f'source/transactions_{target_date}.csv'
  target_dir = f'data/bronze/transactions/date={target_date}'
  Raise FileNotFoundError if csv_path does not exist (before any other logic).

def load_bronze_accounts(target_date: str, run_id: str) -> int:
  csv_path   = f'source/accounts_{target_date}.csv'
  target_dir = f'data/bronze/accounts/date={target_date}'
  Raise FileNotFoundError if csv_path does not exist.

Rules:
- Do NOT apply any filter, type coercion, or transformation to source data.
- Do NOT generate run_id or ingested_at inside the loader — they are passed in.
- Do NOT call any dbt commands.
- The amount field must be written exactly as read from the CSV — no sign change.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-2.2.1 | Transactions: 7 dates load correctly | Each of 7 source CSVs | 7 partitions at correct paths, row counts match CSVs | INV-01, INV-03 |
| TC-2.2.2 | Re-run same date | Call twice with same target_date | RuntimeError on second call; partition unchanged | INV-06, INV-07 |
| TC-2.2.3 | Malformed record preserved | CSV row with null transaction_id | Null present in Bronze partition | INV-02 |
| TC-2.2.4 | Source file absent | Non-existent date | FileNotFoundError | — |
| TC-2.2.5 | `amount` not modified | Any date | `amount` values identical between source CSV and Bronze | INV-21 |
| TC-2.2.6 | `_source_file` is basename | Any date | `_source_file` = `'transactions_2024-01-15.csv'` not full path | INV-44b |
| TC-2.2.7 | Accounts: 7 dates load correctly | Each of 7 source CSVs | 7 partitions written | INV-03 |
| TC-2.2.8 | Accounts: invalid status preserved | CSV row with status='UNKNOWN' | Row present in Bronze with status='UNKNOWN' | INV-02 |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
import uuid
from datetime import datetime, timezone
from pipeline.bronze_loader import load_bronze_transactions, load_bronze_accounts

run_id = str(uuid.uuid4())
dates = ['2024-01-15','2024-01-16','2024-01-17','2024-01-18','2024-01-19','2024-01-20','2024-01-21']
for d in dates:
    load_bronze_transactions(d, run_id)
    load_bronze_accounts(d, run_id)
print('All loads complete')
" && \
docker compose run --rm pipeline duckdb :memory: -c "
SELECT
  regexp_extract(filename, 'date=([0-9-]+)', 1) AS date,
  COUNT(*) AS rows,
  COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL) AS null_run_id,
  COUNT(*) FILTER (WHERE _source_file IS NULL) AS null_source_file
FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true)
GROUP BY 1 ORDER BY 1;
"
```
Expected: 7 rows; `null_run_id` = 0; `null_source_file` = 0 for all dates.

**Invariant Flag**
- **INV-01** — Confirm no column renaming, type coercion, or value transformation before `write_parquet_atomic`.
- **INV-02** — Confirm `read_csv_source` return is passed directly to `add_audit_columns` with no intermediate filter.
- **INV-21** — Confirm `amount` column is not passed through `abs()`, negation, or any conditional before write.

---

### Task 2.3 — Bronze Transaction Codes Loader

**Description**
Creates `load_bronze_transaction_codes(run_id)` in `pipeline/bronze_loader.py` (adds to the file from Task 2.2). Writes to the non-date-partitioned path `data/bronze/transaction_codes/data.parquet`. No `target_date` parameter. Does NOT write to any date-partitioned path.

**CC Prompt**
```
Add the following function to pipeline/bronze_loader.py:

def load_bronze_transaction_codes(run_id: str) -> int:
  csv_path   = 'source/transaction_codes.csv'
  target_dir = 'data/bronze/transaction_codes'
  Raise FileNotFoundError if csv_path does not exist.

  ingested_at = datetime.now(timezone.utc)
  df = read_csv_source(csv_path)
  df = add_audit_columns(df, csv_path, run_id, ingested_at)
  row_count = write_parquet_atomic(df, target_dir, 'data.parquet')
  assert_row_count('data/bronze/transaction_codes/data.parquet', row_count)
  return row_count

The output path must be the literal 'data/bronze/transaction_codes/data.parquet'.
Do NOT accept a target_date parameter.
Do NOT construct any date-partitioned path.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-2.3.1 | Loads to correct non-partitioned path | `source/transaction_codes.csv` | `data/bronze/transaction_codes/data.parquet` exists | INV-24d |
| TC-2.3.2 | Re-run raises | Call twice | RuntimeError on second call | INV-06 |
| TC-2.3.3 | No date component in output path | After load | `data/bronze/transaction_codes/` contains only `data.parquet` | INV-24d |
| TC-2.3.4 | Row count matches source | Any run | Bronze row count = CSV row count | INV-03 |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
import uuid
from pipeline.bronze_loader import load_bronze_transaction_codes
rows = load_bronze_transaction_codes(str(uuid.uuid4()))
print(f'Loaded {rows} rows')
" && \
docker compose run --rm pipeline duckdb :memory: -c "
SELECT COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL) AS null_run_id
FROM read_parquet('data/bronze/transaction_codes/data.parquet');
" && \
docker compose run --rm pipeline bash -c "
ls data/bronze/transaction_codes/
" | grep -v "^date=" && echo "PASS: no date-partitioned subdirectory" || echo "FAIL"
```
Expected: `rows` > 0; `null_run_id` = 0; directory listing shows `data.parquet` only.

**Invariant Flag**
- **INV-24d** — Code review must confirm: the string `'data/bronze/transaction_codes/data.parquet'` is a literal constant in the function — no variable named `date` or `target_date` appears in the path.

---

## Session 3 — Silver Promotion

**Session Goal**
All Silver models produce correct output. For each date: `silver_transactions` row count + quarantine row count = Bronze transactions row count. No `transaction_id` appears more than once across all Silver transactions partitions. Every quarantine record has a rejection reason from the defined list. `silver_accounts` has one row per `account_id`. `silver_transaction_codes.affects_balance` is boolean type. `_signed_amount` is non-null on all Silver transaction records and `ABS(_signed_amount) = amount`.

**Integration Check**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
-- Conservation: silver + quarantine = bronze for each date
SELECT
  b.date,
  b.bronze_rows,
  s.silver_rows,
  q.quarantine_rows,
  (b.bronze_rows = s.silver_rows + q.quarantine_rows) AS balanced
FROM (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS bronze_rows
  FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true) GROUP BY 1
) b
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS silver_rows
  FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true) GROUP BY 1
) s ON b.date = s.date
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS quarantine_rows
  FROM read_parquet('data/silver/quarantine/**/*.parquet', filename=true) GROUP BY 1
) q ON b.date = q.date
ORDER BY 1;
"
```
Expected: 7 rows, `balanced` = true for all dates.

---

### Task 3.1 — Silver Transaction Codes Model

**Description**
Implements `dbt_project/models/silver/silver_transaction_codes.sql`. Reads from `data/bronze/transaction_codes/data.parquet`. Casts `affects_balance` to BOOLEAN. Validates `debit_credit_indicator` values are exactly `DR` or `CR` after promotion (post-hook assertion). Writes to `data/silver/transaction_codes/data.parquet`. Also creates `pipeline/silver_runner.py` with `run_silver_transaction_codes(run_id)` — the function that checks path-existence, optionally invokes dbt, and writes the run log row.

**CC Prompt**
```
Implement two things:

(A) dbt_project/models/silver/silver_transaction_codes.sql

The model reads from read_parquet('data/bronze/transaction_codes/data.parquet').

SELECT:
  transaction_code,
  transaction_type,
  description,
  CAST(affects_balance AS BOOLEAN) AS affects_balance,
  debit_credit_indicator,
  _source_file,
  _ingested_at          AS _bronze_ingested_at,
  _pipeline_run_id,
  CURRENT_TIMESTAMP     AS _promoted_at

Configure in dbt_project/dbt_project.yml under the silver section:
  silver_transaction_codes:
    +file_format: parquet
    +location: 'data/silver/transaction_codes/data.parquet'

Add a dbt post-hook that raises a compiler error if any
debit_credit_indicator value is not exactly 'DR' or 'CR':
  post-hook: >
    {{ exceptions.raise_compiler_error('invalid debit_credit_indicator')
       if execute and
       dbt_utils.get_single_value('SELECT COUNT(*) FROM read_parquet(\'data/silver/transaction_codes/data.parquet\') WHERE debit_credit_indicator NOT IN (\'DR\',\'CR\')')
       | int > 0 }}

(B) pipeline/silver_runner.py

Add function:
  def run_silver_transaction_codes(run_id: str) -> str:
    SILVER_TC_PATH = 'data/silver/transaction_codes/data.parquet'
    If os.path.exists(SILVER_TC_PATH):
      write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER', status='SKIPPED')
      return 'SKIPPED'
    started_at = datetime.now(timezone.utc)
    result = subprocess.run(
      ['dbt', 'run', '--select', 'silver_transaction_codes',
       '--vars', json.dumps({'run_id': run_id}),
       '--profiles-dir', '/app/dbt_project',
       '--project-dir', '/app/dbt_project'],
      capture_output=True, text=True
    )
    completed_at = datetime.now(timezone.utc)
    if result.returncode == 0:
      write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                        started_at=started_at, completed_at=completed_at,
                        status='SUCCESS')
      return 'SUCCESS'
    else:
      write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                        started_at=started_at, completed_at=completed_at,
                        status='FAILED', error_message=sanitise_error(result.stderr))
      raise RuntimeError('silver_transaction_codes dbt run failed')

For write_run_log_row and sanitise_error, import from pipeline.run_log
(that module will be created in Session 5; use a placeholder import with
a NotImplementedError stub for now so this file is importable).

Do NOT implement write_run_log_row yet — stub it.
Do NOT advance the watermark.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-3.1.1 | `affects_balance` is boolean type | After promotion | `duckdb: SELECT typeof(affects_balance) FROM read_parquet('data/silver/transaction_codes/data.parquet') LIMIT 1` → `'boolean'` | INV-52 |
| TC-3.1.2 | All source codes promoted | Valid Bronze TC | Row count in Silver TC = row count in Bronze TC | INV-03 |
| TC-3.1.3 | `_bronze_ingested_at` carries forward | After promotion | `_bronze_ingested_at` = Bronze `_ingested_at` for each row | INV-45 |
| TC-3.1.4 | Re-run writes SKIPPED not re-promotes | Call `run_silver_transaction_codes` twice | Second call returns 'SKIPPED'; Silver TC unchanged | INV-24b |
| TC-3.1.5 | Path-existence check before dbt call | Call when Silver TC exists | dbt not invoked on second call | INV-49b |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
import uuid
from pipeline.silver_runner import run_silver_transaction_codes
r = run_silver_transaction_codes(str(uuid.uuid4()))
print(f'First run: {r}')
r2 = run_silver_transaction_codes(str(uuid.uuid4()))
print(f'Second run: {r2}')
assert r2 == 'SKIPPED', 'FAIL: second run should be SKIPPED'
" && \
docker compose run --rm pipeline duckdb :memory: -c "
SELECT typeof(affects_balance)                                       AS affects_balance_type,
       COUNT(*)                                                       AS total_rows,
       COUNT(*) FILTER (WHERE debit_credit_indicator NOT IN ('DR','CR')) AS bad_dc_indicator,
       COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL)              AS null_run_id
FROM read_parquet('data/silver/transaction_codes/data.parquet');
"
```
Expected: `affects_balance_type` = `'boolean'`; `bad_dc_indicator` = 0; `null_run_id` = 0.

**Invariant Flag**
- **INV-21b** — Post-hook assertion raises on any `debit_credit_indicator` outside `('DR','CR')`. Code review confirms the post-hook uses `exceptions.raise_compiler_error`, not a warning.
- **INV-45** — Code review: `_bronze_ingested_at` selected as `_ingested_at` from Bronze — not `CURRENT_TIMESTAMP`.
- **INV-49b** — `run_silver_transaction_codes`: `os.path.exists(SILVER_TC_PATH)` called before any `subprocess.run`.
- **INV-52** — Explicit `CAST(affects_balance AS BOOLEAN)` — no implicit coercion.

---

### Task 3.2 — Silver Accounts Model

**Description**
Implements `dbt_project/models/silver/silver_accounts.sql`. Reads from the Bronze accounts partition for `target_date`. Applies two rejection rules (NULL_REQUIRED_FIELD, INVALID_ACCOUNT_STATUS). Upserts valid records into `data/silver/accounts/data.parquet` using deterministic tie-breaking (`_source_row_number DESC`). Writes rejected records to the date-partitioned quarantine path. Adds `run_silver_accounts(target_date, run_id)` to `pipeline/silver_runner.py`.

**CC Prompt**
```
Implement two things:

(A) dbt_project/models/silver/silver_accounts.sql

Variables available: run_id, target_date.
Source: read_parquet('data/bronze/accounts/date={{ var("target_date") }}/data.parquet')

Implement as CTEs:

WITH bronze AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC) AS rn
  FROM read_parquet('data/bronze/accounts/date={{ var("target_date") }}/data.parquet')
),
deduplicated AS (
  SELECT * EXCLUDE (rn) FROM bronze WHERE rn = 1
),
null_check AS (
  SELECT *,
    CASE WHEN account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
           OR open_date IS NULL OR credit_limit IS NULL
           OR current_balance IS NULL OR billing_cycle_start IS NULL
           OR billing_cycle_end IS NULL OR account_status IS NULL
           OR TRIM(CAST(account_status AS VARCHAR)) = ''
         THEN 'NULL_REQUIRED_FIELD' END AS _rejection_reason
  FROM deduplicated
),
status_check AS (
  SELECT *,
    CASE WHEN _rejection_reason IS NULL
              AND account_status NOT IN ('ACTIVE','SUSPENDED','CLOSED')
         THEN 'INVALID_ACCOUNT_STATUS'
         ELSE _rejection_reason END AS _rejection_reason_final
  FROM null_check
),
valid AS (SELECT * EXCLUDE (_rejection_reason, _rejection_reason_final)
          FROM status_check WHERE _rejection_reason_final IS NULL),
rejected AS (SELECT * FROM status_check WHERE _rejection_reason_final IS NOT NULL)

For valid records: upsert to data/silver/accounts/data.parquet
  (read existing if present, DELETE matching account_ids, INSERT new rows)
  Output columns:
    account_id, open_date, credit_limit, current_balance,
    billing_cycle_start, billing_cycle_end, account_status,
    _source_file (from Bronze), _bronze_ingested_at (Bronze _ingested_at),
    _pipeline_run_id = '{{ var("run_id") }}',
    _record_valid_from = CURRENT_TIMESTAMP
  Do NOT carry _source_row_number into Silver.

For rejected records: write to
  data/silver/quarantine/date={{ var("target_date") }}/rejected_accounts.parquet
  Columns: all source columns + _source_file, _pipeline_run_id,
           _rejected_at = CURRENT_TIMESTAMP, _rejection_reason = _rejection_reason_final

Configure output in dbt_project.yml:
  silver_accounts: +location: 'data/silver/accounts/data.parquet'

(B) Add to pipeline/silver_runner.py:

def run_silver_accounts(target_date: str, run_id: str) -> str:
  Check: if data/silver/accounts/data.parquet exists AND contains rows
         WHERE _source_file LIKE f'%accounts_{target_date}%':
    write_run_log_row(run_id, 'silver_accounts', 'SILVER', status='SKIPPED')
    return 'SKIPPED'
  [invoke dbt, write run log SUCCESS or FAILED, same pattern as Task 3.1]
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-3.2.1 | Valid record upserted | Account with all fields valid | Record in `data/silver/accounts/data.parquet` | INV-16a |
| TC-3.2.2 | Null account_id → NULL_REQUIRED_FIELD | Bronze row with null account_id | Quarantine row with `_rejection_reason='NULL_REQUIRED_FIELD'` | INV-16a |
| TC-3.2.3 | Invalid status → INVALID_ACCOUNT_STATUS | Bronze row with status='DELINQUENT' | Quarantine row with `_rejection_reason='INVALID_ACCOUNT_STATUS'` | INV-16b |
| TC-3.2.4 | Duplicate account_id in same partition | Two rows same account_id | Silver has exactly 1 row for that account_id | INV-17, INV-24 |
| TC-3.2.5 | Last-row-wins tie-breaking | Two rows same account_id, different balances | Silver row = last row by `_source_row_number` | INV-24, RQ-2 |
| TC-3.2.6 | Silver accounts is single non-partitioned file | After 7 dates promoted | `ls data/silver/accounts/` = `data.parquet` only | INV-24c |
| TC-3.2.7 | `_record_valid_from` is promotion timestamp | After promotion | `_record_valid_from` != `_bronze_ingested_at` | INV-27b |
| TC-3.2.8 | Re-run same date → SKIPPED | Call twice for same date | Second call returns 'SKIPPED' | INV-22 equivalent |

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
-- No duplicate account_ids
SELECT account_id, COUNT(*) AS cnt
FROM read_parquet('data/silver/accounts/data.parquet')
GROUP BY 1 HAVING COUNT(*) > 1;

-- Rejection reason codes valid
SELECT DISTINCT _rejection_reason
FROM read_parquet('data/silver/quarantine/**/*.parquet')
WHERE filename LIKE '%rejected_accounts%';

-- _record_valid_from is not equal to _bronze_ingested_at
SELECT COUNT(*) AS mismatched_timestamps
FROM read_parquet('data/silver/accounts/data.parquet')
WHERE _record_valid_from = _bronze_ingested_at;
"
```
Expected: no rows from first query (no duplicates); rejection reasons ⊆ `{'NULL_REQUIRED_FIELD','INVALID_ACCOUNT_STATUS'}`; `mismatched_timestamps` = 0 (they should differ since promotion happens after ingestion).

**Invariant Flag**
- **INV-17** — Upsert must DELETE existing row then INSERT, or use `INSERT OR REPLACE` — not INSERT only. Code review confirms no plain `INSERT` without delete.
- **INV-24** — `ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC)` — confirm `_source_row_number` comes from Bronze via `read_csv_source`.
- **INV-24c** — `dbt_project.yml` location: `data/silver/accounts/data.parquet` — no partition key.
- **INV-27b** — `_record_valid_from = CURRENT_TIMESTAMP` — code review confirms not `_bronze_ingested_at`.

---

### Task 3.3 — Silver Transactions Model

**Description**
Implements `dbt_project/models/silver/silver_transactions.sql`. All five rejection rules. Cross-partition deduplication (`transaction_id` uniqueness check against all existing Silver partitions). Sign assignment via `debit_credit_indicator`. `_is_resolvable` flag via LEFT JOIN to `data/silver/accounts/data.parquet`. Quarantine writes to `data/silver/quarantine/date={target_date}/rejected.parquet`. Adds `run_silver_transactions(target_date, run_id)` to `pipeline/silver_runner.py`.

**CC Prompt**
```
Implement dbt_project/models/silver/silver_transactions.sql.
Variables: run_id, target_date.

Source: read_parquet('data/bronze/transactions/date={{ var("target_date") }}/data.parquet')

Implement as sequential CTEs — each CTE either passes a record forward or flags
it for quarantine. A record cannot appear in both paths.

CTE order:
  cte_null_check: flag WHERE any of (transaction_id, account_id,
    transaction_date, amount, transaction_code, channel) IS NULL
    OR TRIM(CAST(... AS VARCHAR)) = ''
    → _rejection_reason = 'NULL_REQUIRED_FIELD'

  cte_amount_check: on records passing null check:
    flag WHERE TRY_CAST(amount AS DECIMAL(18,4)) IS NULL OR
               CAST(amount AS DECIMAL(18,4)) <= 0
    → _rejection_reason = 'INVALID_AMOUNT'

  cte_code_check: on records passing amount check:
    LEFT JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
      ON bronze.transaction_code = tc.transaction_code
    flag WHERE tc.transaction_code IS NULL
    → _rejection_reason = 'INVALID_TRANSACTION_CODE'

  cte_channel_check: on records passing code check:
    flag WHERE channel NOT IN ('ONLINE', 'IN_STORE')  (exact, case-sensitive)
    → _rejection_reason = 'INVALID_CHANNEL'

  cte_dedup_check: on records passing channel check:
    LEFT JOIN read_parquet('data/silver/transactions/**/*.parquet',
                           union_by_name=true) existing
      ON bronze.transaction_id = existing.transaction_id
    flag WHERE existing.transaction_id IS NOT NULL
    → _rejection_reason = 'DUPLICATE_TRANSACTION_ID'
    (If no Silver partitions exist yet, read_parquet glob returns empty — that is correct.)

  cte_valid: records passing all five checks.

  cte_signed: JOIN cte_valid to tc on transaction_code:
    _signed_amount = CASE
      WHEN tc.debit_credit_indicator = 'DR' THEN CAST(amount AS DECIMAL(18,4))
      WHEN tc.debit_credit_indicator = 'CR' THEN -CAST(amount AS DECIMAL(18,4))
      ELSE NULL  -- should never happen given cte_code_check passed
    END

  cte_resolvable: LEFT JOIN cte_signed to
    read_parquet('data/silver/accounts/data.parquet') acct ON account_id:
    _is_resolvable = (acct.account_id IS NOT NULL)
    Records with _is_resolvable = false enter Silver (do NOT quarantine them).

Final SELECT (to Silver output path):
  All Bronze source columns (excluding _source_row_number) +
  _source_file (from Bronze), _bronze_ingested_at (Bronze _ingested_at),
  _pipeline_run_id = '{{ var("run_id") }}',
  _promoted_at = CURRENT_TIMESTAMP,
  _is_resolvable,
  _signed_amount

Configure: +location: 'data/silver/transactions/date={{ var("target_date") }}/data.parquet'

Quarantine output (separate model or second materialisation in same file):
  Write UNION ALL of all rejected CTEs to
  data/silver/quarantine/date={{ var("target_date") }}/rejected.parquet
  Columns: all source columns + _source_file, _pipeline_run_id,
           _rejected_at = CURRENT_TIMESTAMP, _rejection_reason

Add dbt post-hook:
  Assert SELECT COUNT(*) FROM silver_transactions_output WHERE _signed_amount IS NULL = 0
  Raise compiler error if non-zero.

(B) Add to pipeline/silver_runner.py:
  def run_silver_transactions(target_date: str, run_id: str) -> str:
    SILVER_TXN_PATH = f'data/silver/transactions/date={target_date}/data.parquet'
    If os.path.exists(SILVER_TXN_PATH):
      write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                        status='SKIPPED',
                        records_rejected=None)
      return 'SKIPPED'
    [invoke dbt, write run log with records_written and records_rejected
     where records_rejected = quarantine row count (NOT unresolvable rows)]
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-3.3.1 | Null transaction_id → NULL_REQUIRED_FIELD | Bronze row with null transaction_id | Quarantine row with correct code | INV-12a |
| TC-3.3.2 | Null in each of 6 required fields | One row per null field | Each produces one quarantine row | INV-12a |
| TC-3.3.3 | amount = 0 → INVALID_AMOUNT | Bronze row with amount=0 | Quarantine with INVALID_AMOUNT | INV-12b |
| TC-3.3.4 | Unknown transaction_code → INVALID_TRANSACTION_CODE | Bronze row with code='ZZZ' | Quarantine with INVALID_TRANSACTION_CODE | INV-12d |
| TC-3.3.5 | channel='online' (lowercase) → INVALID_CHANNEL | Bronze row | Quarantine with INVALID_CHANNEL | INV-12e |
| TC-3.3.6 | Cross-partition duplicate → DUPLICATE_TRANSACTION_ID | transaction_id already in prior Silver partition | Quarantine with DUPLICATE_TRANSACTION_ID | INV-12c, INV-14 |
| TC-3.3.7 | DR transaction: _signed_amount positive | DR transaction_code | `_signed_amount > 0`, `ABS(_signed_amount) = amount` | INV-19, INV-50 |
| TC-3.3.8 | CR transaction: _signed_amount negative | CR transaction_code | `_signed_amount < 0`, `ABS(_signed_amount) = amount` | INV-19, INV-50 |
| TC-3.3.9 | Unresolvable account: enters Silver with flag | account_id not in Silver accounts | `_is_resolvable = false` in Silver, NOT in quarantine | INV-13 |
| TC-3.3.10 | Conservation: bronze = silver + quarantine | Any date after promotion | Counts balance | INV-15 |
| TC-3.3.11 | No null _signed_amount | All 7 dates promoted | COUNT WHERE _signed_amount IS NULL = 0 | INV-20 |
| TC-3.3.12 | Re-run same date → SKIPPED | Call twice | SKIPPED on second call; quarantine row count unchanged | INV-22, INV-23 |

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
-- TC-3.3.11: no null _signed_amount
SELECT COUNT(*) AS null_signed_amount
FROM read_parquet('data/silver/transactions/**/*.parquet')
WHERE _signed_amount IS NULL;

-- TC-3.3.7/8: magnitude invariant
SELECT COUNT(*) AS magnitude_mismatch
FROM read_parquet('data/silver/transactions/**/*.parquet')
WHERE ABS(_signed_amount) != CAST(amount AS DECIMAL(18,4));

-- TC-3.3.6: cross-partition uniqueness
SELECT transaction_id, COUNT(*) AS cnt
FROM read_parquet('data/silver/transactions/**/*.parquet')
GROUP BY 1 HAVING COUNT(*) > 1;

-- TC-3.3.10: conservation for each date
SELECT
  b.date,
  b.bronze_rows,
  s.silver_rows,
  q.quarantine_rows,
  (b.bronze_rows = s.silver_rows + q.quarantine_rows) AS balanced
FROM (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS bronze_rows
  FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true) GROUP BY 1
) b
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS silver_rows
  FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true) GROUP BY 1
) s ON b.date = s.date
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS quarantine_rows
  FROM read_parquet('data/silver/quarantine/**/*.parquet', filename=true) GROUP BY 1
) q ON b.date = q.date
ORDER BY 1;
"
```
Expected: `null_signed_amount` = 0; `magnitude_mismatch` = 0; no rows from uniqueness query; `balanced` = true for all 7 dates.

**Invariant Flag**
- **INV-12c, INV-14** — Cross-partition dedup CTE reads `data/silver/transactions/**/*.parquet`. Code review confirms glob path, not just the current date partition.
- **INV-13** — Code review: `_is_resolvable = false` records are in the final SELECT, not routed to `cte_rejected`.
- **INV-15** — Code review: every record from Bronze appears in exactly one of `cte_valid` (→ Silver) or one of the `cte_*_check` flagged paths (→ quarantine). No EXCEPT, no silent drop.
- **INV-19** — CASE uses `CAST(amount AS DECIMAL(18,4))` — no float intermediate.
- **INV-22** — `os.path.exists(SILVER_TXN_PATH)` checked before `subprocess.run` in `run_silver_transactions`.
- **INV-23** — Re-run skip prevents second quarantine write. Code review confirms quarantine is written only by dbt, and dbt is only called when Silver path does not exist.

---

### Task 3.4 — Silver Promotion Verification Baseline

**Description**
No new code. Promotes all 7 dates through Silver, then records exact row counts in `VERIFICATION_CHECKLIST.md`. These counts become the Phase 5 sign-off baseline. Also confirms `affects_balance` type and rejection reason enumeration.

**CC Prompt**
```
This is a verification-only task. Do not write any code.

Run the following DuckDB queries and record the output in VERIFICATION_CHECKLIST.md
under the section 'Silver Baseline Counts'. These are locked values for Phase 5
sign-off.

Query 1 — Silver transactions per date:
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS silver_rows
  FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true)
  GROUP BY 1 ORDER BY 1;

Query 2 — Quarantine rows per date (transactions):
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS quarantine_rows
  FROM read_parquet('data/silver/quarantine/**/*.parquet', filename=true)
  WHERE filename NOT LIKE '%rejected_accounts%'
  GROUP BY 1 ORDER BY 1;

Query 3 — Silver accounts total and distinct account_ids:
  SELECT COUNT(*) AS total_rows, COUNT(DISTINCT account_id) AS distinct_accounts
  FROM read_parquet('data/silver/accounts/data.parquet');

Query 4 — Cross-partition uniqueness (expected: 0 rows):
  SELECT transaction_id, COUNT(*) AS cnt
  FROM read_parquet('data/silver/transactions/**/*.parquet')
  GROUP BY 1 HAVING COUNT(*) > 1;

Query 5 — affects_balance type:
  SELECT typeof(affects_balance) AS type
  FROM read_parquet('data/silver/transaction_codes/data.parquet') LIMIT 1;

Query 6 — Distinct rejection reasons:
  SELECT DISTINCT _rejection_reason
  FROM read_parquet('data/silver/quarantine/**/*.parquet');

Record all results in VERIFICATION_CHECKLIST.md. These are inputs to Session 5
sign-off — do not re-derive during sign-off.
```

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date,
       COUNT(*) AS silver_rows
FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true)
GROUP BY 1 ORDER BY 1;

SELECT typeof(affects_balance) AS affects_balance_type
FROM read_parquet('data/silver/transaction_codes/data.parquet') LIMIT 1;

SELECT DISTINCT _rejection_reason
FROM read_parquet('data/silver/quarantine/**/*.parquet');
"
```

**Invariant Flag**
- **INV-14, INV-15, INV-48** — This task's query results are the Phase 5 sign-off baseline. Any deviation in Phase 5 indicates a pipeline regression.

---

## Session 4 — Gold Models

**Session Goal**
`gold_daily_summary.parquet` contains exactly one row per `transaction_date` where at least one `_is_resolvable = true` record exists in Silver. `gold_weekly_account_summary.parquet` contains one row per `(account_id, week_start_date)` with correct ISO week boundaries. Both files exist at exactly one canonical path each. `unresolvable_count` is non-null for every daily summary row. `closing_balance` is null (not zero) when no Silver accounts record exists. Both Gold files are written atomically by `pipeline.py`.

**Integration Check**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
-- Gold daily: total_signed_amount matches Silver for each date
SELECT
  g.transaction_date,
  g.total_signed_amount            AS gold_sum,
  s.silver_sum,
  (g.total_signed_amount = s.silver_sum) AS match
FROM read_parquet('data/gold/daily_summary/data.parquet') g
JOIN (
  SELECT transaction_date,
         SUM(_signed_amount) AS silver_sum
  FROM read_parquet('data/silver/transactions/**/*.parquet')
  WHERE _is_resolvable = true
  GROUP BY 1
) s ON g.transaction_date = s.transaction_date
ORDER BY 1;

-- Gold weekly: week_start_date is always Monday (ISO DOW 1)
SELECT
  MIN(DAYOFWEEK(week_start_date)) AS min_dow,
  MAX(DAYOFWEEK(week_start_date)) AS max_dow,
  COUNT(*) AS total_rows
FROM read_parquet('data/gold/weekly_account_summary/data.parquet');
"
```
Expected: `match` = true for all dates; `min_dow` = `max_dow` = 1 (Monday); `total_rows` > 0.

---

### Task 4.1 — Gold Daily Summary Model

**Description**
Implements `dbt_project/models/gold/gold_daily_summary.sql`. Reads all Silver transactions partitions. Filters to `_is_resolvable = true` before aggregating. Includes `unresolvable_count` (non-null, zero if no unresolvable rows for that date). `transactions_by_type` built dynamically from `GROUP BY transaction_type` — no hardcoded type names. Writes to staging path `data/gold/.tmp_daily_summary.parquet`; `pipeline.py` performs the atomic rename.

**CC Prompt**
```
Implement dbt_project/models/gold/gold_daily_summary.sql.
Variable: run_id.

Source: read_parquet('data/silver/transactions/**/*.parquet', union_by_name=true)

Full recompute — read all Silver partitions every time.

Two-pass pattern:

Pass 1 (resolvable aggregation — WHERE _is_resolvable = true):
  GROUP BY transaction_date:
    total_transactions    : COUNT(*)
    total_signed_amount   : SUM(_signed_amount)
    online_transactions   : COUNT(*) FILTER (WHERE channel = 'ONLINE')
    instore_transactions  : COUNT(*) FILTER (WHERE channel = 'IN_STORE')

Pass 2 (unresolvable count — WHERE _is_resolvable = false):
  GROUP BY transaction_date:
    unresolvable_count    : COUNT(*)

Pass 3 (transactions_by_type — WHERE _is_resolvable = true):
  For each transaction_date, produce a MAP or STRUCT containing
  one entry per distinct transaction_type present on that date.
  Use GROUP BY transaction_date, transaction_type then aggregate
  into a struct using DuckDB's struct_pack or map_from_entries.
  Do NOT use a pivot with hard-coded type names.

Final JOIN passes 1, 2, 3 on transaction_date.
COALESCE(unresolvable_count, 0) AS unresolvable_count  (zero, not null)

Output columns:
  transaction_date, total_transactions, total_signed_amount,
  transactions_by_type, online_transactions, instore_transactions,
  unresolvable_count,
  _computed_at = CURRENT_TIMESTAMP,
  _pipeline_run_id = '{{ var("run_id") }}',
  _source_period_start = MIN(transaction_date) OVER (),
  _source_period_end   = MAX(transaction_date) OVER ()

Configure: +location: 'data/gold/.tmp_daily_summary.parquet'
(pipeline.py renames to data/gold/daily_summary/data.parquet after dbt exit 0)

Dates with zero resolvable transactions must produce NO row.
Use DECIMAL arithmetic throughout — no FLOAT intermediates.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-4.1.1 | One row per resolvable date | 7 dates | 7 rows in Gold daily summary | INV-29 |
| TC-4.1.2 | total_signed_amount matches Silver | Any date | `gold.total_signed_amount = SUM(_signed_amount WHERE _is_resolvable=true)` for that date | INV-31 |
| TC-4.1.3 | unresolvable_count is 0 not NULL when none | Date with no unresolvable records | `unresolvable_count = 0` | OQ-1/RQ resolved |
| TC-4.1.4 | unresolvable_count > 0 when some exist | Date with flagged records | `unresolvable_count > 0` | INV-26, RQ-1 |
| TC-4.1.5 | transactions_by_type has no empty entries | Date with only PURCHASE and FEE | STRUCT has 2 entries, not 5 | INV-51 |
| TC-4.1.6 | _pipeline_run_id non-null | Any run | COUNT WHERE NULL = 0 | INV-40, INV-47 |
| TC-4.1.7 | Unresolvable excluded from total_transactions | Silver has both | total_transactions = resolvable-only count | INV-26 |

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
SELECT
  COUNT(*) AS gold_rows,
  COUNT(*) FILTER (WHERE unresolvable_count IS NULL) AS null_unresolvable,
  COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL)   AS null_run_id,
  COUNT(*) FILTER (WHERE total_transactions = 0)     AS zero_total
FROM read_parquet('data/gold/daily_summary/data.parquet');

-- total_signed_amount reconciliation
SELECT
  g.transaction_date,
  g.total_signed_amount,
  SUM(t._signed_amount) AS silver_sum,
  (g.total_signed_amount = SUM(t._signed_amount)) AS match
FROM read_parquet('data/gold/daily_summary/data.parquet') g
JOIN read_parquet('data/silver/transactions/**/*.parquet') t
  ON g.transaction_date = t.transaction_date AND t._is_resolvable = true
GROUP BY g.transaction_date, g.total_signed_amount
ORDER BY 1;
"
```
Expected: `null_unresolvable` = 0; `null_run_id` = 0; `zero_total` = 0; `match` = true for all dates.

**Invariant Flag**
- **INV-25** — Code review: no `read_parquet('data/bronze/...')` or `read_csv_auto('source/...')` anywhere in this model.
- **INV-26** — Code review: `WHERE _is_resolvable = true` in the base CTE, before `GROUP BY` — not a HAVING clause.
- **INV-29** — No `COALESCE(..., 0)` on the `GROUP BY transaction_date` result — the WHERE clause naturally omits dates with no resolvable records.
- **INV-51** — STRUCT/MAP built from `GROUP BY transaction_type` aggregation — no hardcoded type names.

---

### Task 4.2 — Gold Weekly Account Summary Model

**Description**
Implements `dbt_project/models/gold/gold_weekly_account_summary.sql`. ISO week boundaries (Monday = `DATE_TRUNC('week', ...)`). `closing_balance` via correlated subquery or lateral join — null if no Silver accounts record. `avg_purchase_amount` null (not zero) when no PURCHASE records. Writes to staging path; `pipeline.py` performs atomic rename.

**CC Prompt**
```
Implement dbt_project/models/gold/gold_weekly_account_summary.sql.
Variable: run_id.

Sources:
  txn : read_parquet('data/silver/transactions/**/*.parquet', union_by_name=true)
  acct: read_parquet('data/silver/accounts/data.parquet')

Filter base: WHERE txn._is_resolvable = true

ISO week boundaries (DuckDB 'week' truncation is Monday-start):
  week_start_date = DATE_TRUNC('week', transaction_date)::DATE
  week_end_date   = (DATE_TRUNC('week', transaction_date) + INTERVAL 6 DAYS)::DATE

GROUP BY week_start_date, week_end_date, account_id:
  total_purchases   : COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE')
  avg_purchase_amount : AVG(CASE WHEN transaction_type = 'PURCHASE'
                                 THEN _signed_amount END)
    -- AVG over all-NULL = NULL; do NOT COALESCE to 0
  total_payments    : SUM(CASE WHEN transaction_type = 'PAYMENT'
                               THEN _signed_amount ELSE 0 END)
  total_fees        : SUM(CASE WHEN transaction_type = 'FEE'
                               THEN _signed_amount ELSE 0 END)
  total_interest    : SUM(CASE WHEN transaction_type = 'INTEREST'
                               THEN _signed_amount ELSE 0 END)

closing_balance:
  Scalar subquery per (account_id, week_end_date):
  (SELECT current_balance
   FROM read_parquet('data/silver/accounts/data.parquet')
   WHERE account_id = outer.account_id
     AND _record_valid_from <= outer.week_end_date
   ORDER BY _record_valid_from DESC
   LIMIT 1)
  Result is NULL if no row exists — do NOT COALESCE to 0.

Audit columns:
  _computed_at     = CURRENT_TIMESTAMP
  _pipeline_run_id = '{{ var("run_id") }}'

Configure: +location: 'data/gold/.tmp_weekly_account_summary.parquet'
(pipeline.py renames to data/gold/weekly_account_summary/data.parquet after dbt exit 0)
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-4.2.1 | `week_start_date` always Monday | Any row | `DAYOFWEEK(week_start_date)` = 1 for all rows | INV-30 |
| TC-4.2.2 | `week_end_date` = `week_start_date` + 6 | Any row | `(week_end_date - week_start_date)` = 6 for all rows | INV-30 |
| TC-4.2.3 | `closing_balance` null when no account record | Account with no Silver accounts entry | `closing_balance IS NULL` | INV-27 |
| TC-4.2.4 | `avg_purchase_amount` null when no purchases | Account-week with zero PURCHASE rows | `avg_purchase_amount IS NULL` | INV-32b |
| TC-4.2.5 | `total_purchases` matches Silver COUNT | Any account-week | Matches `COUNT(*) FILTER (WHERE transaction_type='PURCHASE' AND _is_resolvable=true)` for that account/week | INV-32 |
| TC-4.2.6 | One row per (account_id, week_start_date) | Any output | No duplicate `(account_id, week_start_date)` pairs | INV-30 |

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
SELECT
  MIN(DAYOFWEEK(week_start_date)) AS min_dow,
  MAX(DAYOFWEEK(week_start_date)) AS max_dow,
  MIN(week_end_date - week_start_date) AS min_span,
  MAX(week_end_date - week_start_date) AS max_span,
  COUNT(*) FILTER (WHERE avg_purchase_amount = 0) AS zero_avg_purchase,
  COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL) AS null_run_id
FROM read_parquet('data/gold/weekly_account_summary/data.parquet');

-- Duplicate (account_id, week_start_date) check
SELECT account_id, week_start_date, COUNT(*) AS cnt
FROM read_parquet('data/gold/weekly_account_summary/data.parquet')
GROUP BY 1, 2 HAVING COUNT(*) > 1;
"
```
Expected: `min_dow` = `max_dow` = 1; `min_span` = `max_span` = 6; `zero_avg_purchase` = 0; `null_run_id` = 0; no rows from duplicate check.

**Invariant Flag**
- **INV-27** — Closing balance subquery: `LIMIT 1` with `ORDER BY _record_valid_from DESC`. No `COALESCE`. Code review confirms null propagation when no rows match.
- **INV-30** — `DATE_TRUNC('week', ...)` in DuckDB returns Monday. Code review confirms `'week'` is used, not `'isoweek'` — verify against dbt-duckdb 1.7.x adapter documentation.
- **INV-32b** — `AVG(CASE WHEN transaction_type='PURCHASE' THEN _signed_amount END)` — no COALESCE wrapping the AVG.

---

### Task 4.3 — Gold Atomic Rename in `pipeline.py`

**Description**
Adds `run_gold(run_id)` to `pipeline.py`. Invokes both Gold dbt models (targeting staging paths), then performs atomic rename for each upon dbt exit code 0. Writes SUCCESS/FAILED run log rows for each model. Cleans up staging files on failure. The prior canonical Gold file is not touched on failure.

**CC Prompt**
```
Add function run_gold(run_id: str) -> None to pipeline.py.

STAGING = {
  'gold_daily_summary': (
    'data/gold/.tmp_daily_summary.parquet',
    'data/gold/daily_summary/data.parquet',
  ),
  'gold_weekly_account_summary': (
    'data/gold/.tmp_weekly_account_summary.parquet',
    'data/gold/weekly_account_summary/data.parquet',
  ),
}

For each (model_name, (staging_path, canonical_path)):
  os.makedirs(os.path.dirname(canonical_path), exist_ok=True)
  started_at = datetime.now(timezone.utc)
  result = subprocess.run(
    ['dbt', 'run', '--select', model_name,
     '--vars', json.dumps({'run_id': run_id}),
     '--profiles-dir', '/app/dbt_project',
     '--project-dir', '/app/dbt_project'],
    capture_output=True, text=True
  )
  completed_at = datetime.now(timezone.utc)

  If result.returncode == 0 and os.path.exists(staging_path):
    os.rename(staging_path, canonical_path)          -- atomic
    records_written = (duckdb.query(
      f"SELECT COUNT(*) FROM read_parquet('{canonical_path}')"
    ).fetchone()[0])
    write_run_log_row(run_id, model_name, 'GOLD', started_at, completed_at,
                      status='SUCCESS', records_written=records_written)
  Else:
    if os.path.exists(staging_path):
      os.remove(staging_path)                        -- clean up only staging
    write_run_log_row(run_id, model_name, 'GOLD', started_at, completed_at,
                      status='FAILED', error_message=sanitise_error(result.stderr))
    raise RuntimeError(f'{model_name} dbt run failed')

Rules:
- os.rename() is called by pipeline.py, NOT inside the dbt model.
- The canonical_path must not be touched on failure — only staging_path is removed.
- After rename: assert exactly one .parquet file exists in each Gold directory.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-4.3.1 | Successful run: staging renamed to canonical | Normal run | `data/gold/daily_summary/data.parquet` exists; staging absent | INV-28, INV-08 |
| TC-4.3.2 | Failed dbt run: canonical unchanged | Simulate non-zero exit | Prior canonical file intact; staging removed | INV-28 |
| TC-4.3.3 | Each Gold dir has exactly one .parquet file | After any successful run | `ls data/gold/daily_summary/ | wc -l` = 1 | INV-08 |
| TC-4.3.4 | SUCCESS row in run log after rename | Normal run | Run log has SUCCESS for both Gold models | INV-47 |
| TC-4.3.5 | `_pipeline_run_id` in Gold = run_id passed in | Any run | All Gold rows carry matching run_id | INV-40, INV-43b |

**Verification Command**
```bash
docker compose run --rm pipeline duckdb :memory: -c "
SELECT COUNT(*) AS gold_daily_rows,
       COUNT(*) FILTER (WHERE _pipeline_run_id IS NULL) AS null_run_id
FROM read_parquet('data/gold/daily_summary/data.parquet');
" && \
docker compose run --rm pipeline bash -c "
  ls data/gold/daily_summary/*.parquet | wc -l
  ls data/gold/weekly_account_summary/*.parquet | wc -l
" | grep "^1$" && echo "PASS: exactly one file in each Gold dir" || echo "FAIL"
```

**Invariant Flag**
- **INV-28** — Code review must confirm: `os.rename()` is in `pipeline.py`, not in the dbt model's post-hook or SQL. The dbt model writes only to the staging path.
- **INV-08** — Post-rename assertion: each `data/gold/{entity}/` directory contains exactly one `.parquet` file. Code review confirms no residual `.tmp_` files after rename.
- **INV-46** — Derived from INV-28: staging cleanup on failure does not touch canonical path.

---

## Session 5 — Pipeline Orchestration and Sign-Off

**Session Goal**
`pipeline.py` manages the complete pipeline lifecycle: PID file, run log, watermark control, `--reset-watermark` command, historical pipeline (all 7 dates end-to-end with watermark-resume on re-run), and incremental pipeline (no-op when no source file). Phase 8 sign-off conditions from Brief §10 are all met. Running the full pipeline twice produces identical Bronze, Silver, quarantine, and Gold output.

**Integration Check**
```bash
# Run historical, verify watermark, run again (should be no-op), run incremental
docker compose run --rm pipeline python pipeline.py \
  --historical --start-date 2024-01-15 --end-date 2024-01-21 && \
docker compose run --rm pipeline duckdb :memory: -c "
  SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');
" && \
docker compose run --rm pipeline python pipeline.py \
  --historical --start-date 2024-01-15 --end-date 2024-01-21 && \
docker compose run --rm pipeline python pipeline.py --incremental && \
docker compose run --rm pipeline duckdb :memory: -c "
  -- Watermark still 2024-01-21 after all three runs
  SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');
  -- All second-run log rows are SKIPPED or SUCCESS (no new FAILED)
  SELECT status, COUNT(*) FROM read_parquet('data/pipeline/run_log.parquet')
  GROUP BY 1 ORDER BY 1;
"
```

---

### Task 5.1 — PID File Lifecycle

**Description**
Adds PID file management to `pipeline.py`: write at startup before any argument parsing, remove on clean exit via `try/finally`, remove on SIGTERM, live-process detection, stale-PID recovery. Run_id generation must remain the first line of `main()`.

**CC Prompt**
```
Modify pipeline.py to add PID file management.

PID_FILE = 'data/pipeline/pipeline.pid'

Insert the following block in main(), immediately after run_id generation and
before argument parsing:

1. Stale/live check:
   If os.path.exists(PID_FILE):
     pid = int(open(PID_FILE).read().strip())
     try:
       os.kill(pid, 0)
       # Process alive
       print(f"Error: pipeline already running (PID {pid})", file=sys.stderr)
       sys.exit(1)
     except ProcessLookupError:
       os.remove(PID_FILE)   # stale: process dead
     except PermissionError:
       print(f"Error: pipeline already running (PID {pid})", file=sys.stderr)
       sys.exit(1)

2. Write PID: open(PID_FILE, 'w').write(str(os.getpid()))

3. Register SIGTERM handler (before any data processing):
   import signal
   def _sigterm(signum, frame):
     if os.path.exists(PID_FILE): os.remove(PID_FILE)
     sys.exit(0)
   signal.signal(signal.SIGTERM, _sigterm)

4. Wrap remaining main() logic in try/finally:
   try:
     [all pipeline logic]
   finally:
     if os.path.exists(PID_FILE): os.remove(PID_FILE)

Ordering in main():
  run_id = str(uuid.uuid4())     # line 1 — must stay first
  [PID check and write]          # lines 2–N
  [argument parsing]             # after PID write
  [pipeline logic in try/finally]

Do NOT move run_id after any of the PID logic.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-5.1.1 | PID file written before any processing | `--smoke-test-sleep 2` in background | `cat data/pipeline/pipeline.pid` returns a valid PID | INV-41a |
| TC-5.1.2 | PID file removed on clean exit | Any completed run | `ls data/pipeline/pipeline.pid` → No such file | INV-41b |
| TC-5.1.3 | SIGTERM removes PID file | Send SIGTERM during sleep | PID file absent after signal | INV-41c |
| TC-5.1.4 | Concurrent invocation blocked | Two simultaneous starts | Second exits with "already running" to stderr | INV-41d |
| TC-5.1.5 | Stale PID recovered | Write PID file with dead PID, then start | Startup succeeds; stale file overwritten | INV-41e |

**Verification Command**
```bash
# TC-5.1.4: concurrent invocation
docker compose run --rm pipeline bash -c "
  python pipeline.py --incremental &
  sleep 1
  python pipeline.py --incremental 2>&1 | grep 'already running' \
    && echo 'PASS: blocked' || echo 'FAIL: not blocked'
  wait
" && \
# TC-5.1.5: stale PID recovery
docker compose run --rm pipeline bash -c "
  echo '999999' > data/pipeline/pipeline.pid
  python pipeline.py --incremental \
    && echo 'PASS: stale PID recovered' || echo 'FAIL'
"
```

**Invariant Flag**
- **INV-41a** — PID write is before `parser.parse_args()`. Code review: no `args = ...` before `open(PID_FILE, 'w')`.
- **INV-41b** — `finally` block: unconditional `os.remove(PID_FILE)` guarded by `os.path.exists`.
- **INV-41c** — `signal.signal(signal.SIGTERM, _sigterm)` registered before any Bronze/Silver/Gold call.
- **INV-41e** — `ProcessLookupError` path deletes PID file and continues; does not exit 1.

---

### Task 5.2 — Run Log Writer and `pipeline/run_log.py`

**Description**
Creates `pipeline/run_log.py` with `write_run_log_row` and `sanitise_error`. Atomic append (read-all / append / write-temp / rename). Enum validation for `pipeline_type` and `layer`. `error_message` sanitisation (no paths, max 500 chars). This resolves the stub import in `pipeline/silver_runner.py` from Task 3.1.

**CC Prompt**
```
Create pipeline/run_log.py with the following:

RUN_LOG_PATH = 'data/pipeline/run_log.parquet'
TEMP_LOG_PATH = 'data/pipeline/.tmp_run_log.parquet'

VALID_PIPELINE_TYPES = frozenset({'HISTORICAL', 'INCREMENTAL'})
VALID_LAYERS         = frozenset({'BRONZE', 'SILVER', 'GOLD'})
VALID_STATUSES       = frozenset({'SUCCESS', 'FAILED', 'SKIPPED'})

def sanitise_error(raw: str, max_chars: int = 500) -> str:
  # Remove substrings starting with '/' (filesystem paths)
  import re
  cleaned = re.sub(r'/\S*', '', raw).strip()
  if len(cleaned) > max_chars:
    cleaned = cleaned[:max_chars - len('[truncated]')] + '[truncated]'
  return cleaned or 'unknown error'

def write_run_log_row(
    run_id: str,
    model_name: str,
    layer: str,
    *,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    pipeline_type: str,
    status: str,
    records_processed: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,  # Silver only
    error_message: str | None = None
) -> None:
  Validate:
    pipeline_type in VALID_PIPELINE_TYPES — raise ValueError otherwise
    layer in VALID_LAYERS                 — raise ValueError otherwise
    status in VALID_STATUSES              — raise ValueError otherwise
    If status == 'FAILED' and error_message is None: raise ValueError
    If status != 'FAILED' and error_message is not None: raise ValueError
    If layer != 'SILVER' and records_rejected is not None: raise ValueError

  new_row = pandas.DataFrame([{
    'run_id': run_id, 'pipeline_type': pipeline_type,
    'model_name': model_name, 'layer': layer,
    'started_at': started_at, 'completed_at': completed_at,
    'status': status,
    'records_processed': records_processed,
    'records_written': records_written,
    'records_rejected': records_rejected,
    'error_message': error_message,
  }])

  Atomic append:
    if os.path.exists(RUN_LOG_PATH):
      existing = pandas.read_parquet(RUN_LOG_PATH)
      combined = pandas.concat([existing, new_row], ignore_index=True)
    else:
      combined = new_row
    combined.to_parquet(TEMP_LOG_PATH, index=False)
    os.rename(TEMP_LOG_PATH, RUN_LOG_PATH)

Update pipeline/silver_runner.py to replace the stub import with
  from pipeline.run_log import write_run_log_row, sanitise_error
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-5.2.1 | First write creates file with 1 row | Call once | `read_parquet(RUN_LOG_PATH)` has 1 row | INV-37 |
| TC-5.2.2 | Second write appends, not overwrites | Call twice | File has 2 rows, both present | INV-37 |
| TC-5.2.3 | error_message required on FAILED | status='FAILED', error_message=None | ValueError raised | INV-39 |
| TC-5.2.4 | error_message forbidden on SUCCESS | status='SUCCESS', error_message='err' | ValueError raised | INV-39 |
| TC-5.2.5 | Filesystem path stripped from error | `sanitise_error('/app/data/file.parquet: not found')` | No '/' substring in output | INV-39 |
| TC-5.2.6 | Truncation at 500 chars | 600-char input | Output ≤ 500 chars, ends with '[truncated]' | INV-39 |
| TC-5.2.7 | records_rejected forbidden on BRONZE layer | layer='BRONZE', records_rejected=5 | ValueError raised | INV-39b |
| TC-5.2.8 | Atomic write: temp in same dir as canonical | Any write | `.tmp_run_log.parquet` in `data/pipeline/` | INV-37b |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
from pipeline.run_log import write_run_log_row, sanitise_error
from datetime import datetime, timezone
import duckdb, os

now = datetime.now(timezone.utc)
write_run_log_row('test-run-1', 'test_model', 'BRONZE',
                  started_at=now, completed_at=now,
                  pipeline_type='HISTORICAL', status='SUCCESS',
                  records_written=100)
write_run_log_row('test-run-1', 'test_model_2', 'SILVER',
                  started_at=now, completed_at=now,
                  pipeline_type='HISTORICAL', status='SUCCESS',
                  records_written=90, records_rejected=10)

r = duckdb.query(\"SELECT COUNT(*) FROM read_parquet('data/pipeline/run_log.parquet')\").fetchone()[0]
assert r == 2, f'Expected 2 rows, got {r}'

cleaned = sanitise_error('/app/data/pipeline/control.parquet: not found', max_chars=500)
assert '/' not in cleaned, f'Path not stripped: {cleaned}'
print('PASS: 2 rows, path stripped correctly')
" && \
docker compose run --rm pipeline duckdb :memory: -c "
SELECT COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE run_id IS NULL) AS null_run_id
FROM read_parquet('data/pipeline/run_log.parquet');
"
```

**Invariant Flag**
- **INV-37** — Atomic append: code review confirms `os.rename(TEMP_LOG_PATH, RUN_LOG_PATH)` — not `to_parquet(RUN_LOG_PATH)` directly.
- **INV-37b** — Temp path `data/pipeline/.tmp_run_log.parquet` — same directory as canonical. No `/tmp` usage.
- **INV-39** — Code review: FAILED/error_message validation raises before any DataFrame construction.
- **INV-40b** — `pipeline_type` and `layer` validated against frozensets before write.

---

### Task 5.3 — Watermark Control and `--reset-watermark`

**Description**
Creates `pipeline/control.py` with `read_watermark()` and `write_watermark()`. Wires `--reset-watermark YYYY-MM-DD --confirm` into `pipeline.py`. The command touches only `data/pipeline/control.parquet`. Implements RQ-1 and RQ-4 decisions.

**CC Prompt**
```
Create pipeline/control.py with two functions:

CONTROL_PATH      = 'data/pipeline/control.parquet'
TEMP_CONTROL_PATH = 'data/pipeline/.tmp_control.parquet'

def read_watermark() -> datetime.date | None:
  If CONTROL_PATH does not exist: return None
  df = pandas.read_parquet(CONTROL_PATH)
  If len(df) != 1: raise ValueError(f'control.parquet must have 1 row, has {len(df)}')
  return df['last_processed_date'].iloc[0].date()

def write_watermark(date: datetime.date, run_id: str) -> None:
  df = pandas.DataFrame([{
    'last_processed_date': date,
    'updated_at': datetime.now(timezone.utc),
    'updated_by_run_id': run_id,
  }])
  df.to_parquet(TEMP_CONTROL_PATH, index=False)
  os.rename(TEMP_CONTROL_PATH, CONTROL_PATH)

Then modify pipeline.py's --reset-watermark handler:
  When --reset-watermark DATE --confirm:
    from pipeline.control import read_watermark, write_watermark
    prior = read_watermark()
    print(f"Current watermark: {prior or 'none'}")
    try:
      target = datetime.strptime(DATE, '%Y-%m-%d').date()
    except ValueError:
      print(f"Error: invalid date '{DATE}'", file=sys.stderr)
      sys.exit(1)
    write_watermark(target, 'manual-reset')
    print(f"Watermark reset to {target}")
    sys.exit(0)
  When --reset-watermark without --confirm:
    print("Error: --confirm required for --reset-watermark", file=sys.stderr)
    sys.exit(1)

The --reset-watermark handler must not read, write, or delete any file
other than data/pipeline/control.parquet and its temp file.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-5.3.1 | `read_watermark` returns None when no file | No control.parquet | Returns None | INV-33 |
| TC-5.3.2 | `write_watermark` creates 1-row file | Any date | File has exactly 1 row | INV-33b |
| TC-5.3.3 | Second `write_watermark` replaces not appends | Call twice | File still has 1 row | INV-33b |
| TC-5.3.4 | `--reset-watermark` without `--confirm` exits 1 | Missing `--confirm` | Exit 1, "confirm required" message | INV-36b |
| TC-5.3.5 | `--reset-watermark` touches only control.parquet | Run with --confirm | No Bronze/Silver/Gold/quarantine files modified | INV-36b |
| TC-5.3.6 | Invalid date exits 1 | `--reset-watermark 2024-13-01 --confirm` | Exit 1, "invalid date" message | — |

**Verification Command**
```bash
docker compose run --rm pipeline python -c "
from pipeline.control import read_watermark, write_watermark
import datetime
assert read_watermark() is None, 'FAIL: expected None'
write_watermark(datetime.date(2024, 1, 15), 'test')
result = read_watermark()
assert result == datetime.date(2024, 1, 15), f'FAIL: {result}'
write_watermark(datetime.date(2024, 1, 16), 'test2')
print('PASS: read/write roundtrip')
" && \
docker compose run --rm pipeline duckdb :memory: -c "
SELECT COUNT(*) AS rows FROM read_parquet('data/pipeline/control.parquet');
" && \
docker compose run --rm pipeline python pipeline.py --reset-watermark 2024-01-10 \
  2>&1 | grep "confirm required" && echo "PASS: --confirm enforced"
```
Expected: `rows` = 1; grep matches "confirm required".

**Invariant Flag**
- **INV-33b** — `write_watermark`: always writes a fresh single-row DataFrame — never appends. Code review: no `pandas.concat` in this function.
- **INV-36b** — `--reset-watermark` handler: code review confirms no `os.remove`, `os.rmdir`, `to_parquet`, or `open(..., 'w')` for any path other than `CONTROL_PATH` and `TEMP_CONTROL_PATH`.

---

### Task 5.4 — Historical and Incremental Pipeline Orchestration

**Description**
Implements the full `run_historical(start_date, end_date, run_id)` and `run_incremental(run_id)` functions in `pipeline.py`. Historical: resumes from `max(start_date, watermark+1)` per RQ-1 and RQ-3. Incremental: no-op if source file absent per RQ-4. Watermark advances only after Gold completes per RQ-1. Both pipelines write SKIPPED for Silver TC per RQ-5.

**CC Prompt**
```
Add two functions to pipeline.py. Import from pipeline.bronze_loader,
pipeline.silver_runner, pipeline.control, pipeline.run_log.

def run_historical(start_date: str, end_date: str, run_id: str) -> None:
  from pipeline.control import read_watermark, write_watermark
  watermark = read_watermark()
  start = datetime.strptime(start_date, '%Y-%m-%d').date()
  end   = datetime.strptime(end_date,   '%Y-%m-%d').date()

  if watermark is not None and watermark >= end:
    print(f"nothing to do: all dates already processed (watermark={watermark})")
    return

  effective_start = max(start, watermark + timedelta(days=1)) \
                    if watermark else start

  # Bronze TC (once only)
  if not os.path.exists('data/bronze/transaction_codes/data.parquet'):
    from pipeline.bronze_loader import load_bronze_transaction_codes
    rows = load_bronze_transaction_codes(run_id)
    write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                      pipeline_type='HISTORICAL', status='SUCCESS',
                      records_written=rows, ...)
  else:
    write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                      pipeline_type='HISTORICAL', status='SKIPPED', ...)

  # Silver TC (once only — RQ-5)
  from pipeline.silver_runner import run_silver_transaction_codes
  run_silver_transaction_codes(run_id)  # writes its own SKIPPED/SUCCESS row

  # Per-date loop
  current = effective_start
  while current <= end:
    date_str = current.strftime('%Y-%m-%d')

    # Bronze (skip if partition exists — INV-06)
    for loader, entity in [('load_bronze_accounts', 'accounts'),
                            ('load_bronze_transactions', 'transactions')]:
      bronze_path = f'data/bronze/{entity}/date={date_str}/data.parquet'
      if not os.path.exists(bronze_path):
        from pipeline.bronze_loader import load_bronze_accounts, load_bronze_transactions
        rows = locals()[loader](date_str, run_id)
        write_run_log_row(run_id, f'bronze_{entity}', 'BRONZE',
                          pipeline_type='HISTORICAL', status='SUCCESS',
                          records_written=rows, ...)
      else:
        write_run_log_row(..., status='SKIPPED', ...)

    # Silver (skip if partition exists — INV-22, INV-49b)
    from pipeline.silver_runner import run_silver_accounts, run_silver_transactions
    run_silver_accounts(date_str, run_id)
    run_silver_transactions(date_str, run_id)

    # Gold (full recompute — RQ-6)
    run_gold(run_id)

    # Watermark advances ONLY after Gold succeeds (RQ-1)
    write_watermark(current, run_id)
    current += timedelta(days=1)


def run_incremental(run_id: str) -> None:
  from pipeline.control import read_watermark, write_watermark
  watermark = read_watermark()
  if watermark is None:
    print("Error: no watermark — run historical pipeline first", file=sys.stderr)
    sys.exit(1)

  target = watermark + timedelta(days=1)
  date_str = target.strftime('%Y-%m-%d')
  txn_path  = f'source/transactions_{date_str}.csv'
  acct_path = f'source/accounts_{date_str}.csv'

  if not os.path.exists(txn_path) or not os.path.exists(acct_path):
    print(f"no source file for {date_str} — pipeline is current")
    return   # RQ-4: no run log row, no watermark change, exit 0

  # Silver TC: always SKIPPED in incremental (RQ-5)
  write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                    pipeline_type='INCREMENTAL', status='SKIPPED', ...)

  # Bronze
  for loader, entity in [('load_bronze_accounts', 'accounts'),
                          ('load_bronze_transactions', 'transactions')]:
    bronze_path = f'data/bronze/{entity}/date={date_str}/data.parquet'
    if not os.path.exists(bronze_path):
      rows = locals()[loader](date_str, run_id)
      write_run_log_row(run_id, f'bronze_{entity}', 'BRONZE',
                        pipeline_type='INCREMENTAL', status='SUCCESS', ...)
    else:
      write_run_log_row(..., status='SKIPPED', ...)

  # Silver + Gold + watermark (RQ-1)
  run_silver_accounts(date_str, run_id)
  run_silver_transactions(date_str, run_id)
  run_gold(run_id)
  write_watermark(target, run_id)

Wire up: in main(), call run_historical or run_incremental or reset-watermark
based on args.
```

**Test Cases**

| TC | Scenario | Input | Expected | Invariant |
|----|----------|-------|----------|-----------|
| TC-5.4.1 | Historical: 7 dates processed | `--historical 2024-01-15 2024-01-21` | Watermark = 2024-01-21 | INV-33 |
| TC-5.4.2 | Crash-and-retry: resumes from watermark+1 | Kill after date 3; re-run | Resumes from 2024-01-18; no FM-4 | INV-35, RQ-1 |
| TC-5.4.3 | Re-run completed history: no-op message | Re-run same command | "nothing to do" printed; watermark unchanged | RQ-3, INV-48 |
| TC-5.4.4 | Watermark not advanced on Silver failure | Silver fails on date 3 | Watermark = last successfully completed date | RQ-1 |
| TC-5.4.5 | Incremental: no source file → no-op | Run with no new file | Exit 0; watermark unchanged; no run log row added | RQ-4, INV-36 |
| TC-5.4.6 | Silver TC SKIPPED in incremental run | Any incremental run | Run log has SKIPPED row for silver_transaction_codes | RQ-5, INV-24b |
| TC-5.4.7 | Historical: Bronze TC loaded once | First run | `bronze_transaction_codes` in run log exactly once (not 7 times) | INV-24d |

**Verification Command**
```bash
docker compose run --rm pipeline python pipeline.py \
  --historical --start-date 2024-01-15 --end-date 2024-01-21 && \
docker compose run --rm pipeline duckdb :memory: -c "
-- Watermark
SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');

-- Bronze TC loaded exactly once
SELECT status, COUNT(*) AS cnt
FROM read_parquet('data/pipeline/run_log.parquet')
WHERE model_name = 'bronze_transaction_codes'
GROUP BY 1;

-- Silver TC: should be 1 SUCCESS row (first run) or 1 SKIPPED (if already existed)
SELECT status, COUNT(*) AS cnt
FROM read_parquet('data/pipeline/run_log.parquet')
WHERE model_name = 'silver_transaction_codes'
GROUP BY 1;
" && \
# No-op re-run
docker compose run --rm pipeline python pipeline.py \
  --historical --start-date 2024-01-15 --end-date 2024-01-21 \
  | grep "nothing to do"
```
Expected: `last_processed_date` = `2024-01-21`; bronze_transaction_codes has exactly 1 SUCCESS; "nothing to do" printed on re-run.

**Invariant Flag**
- **INV-33** — `write_watermark(current, run_id)` is the last statement in the per-date loop body. Code review: no statement after `write_watermark` and before `current += timedelta(days=1)`.
- **INV-35** — `effective_start = max(start, watermark + 1)` — code review confirms `start` is not used directly when watermark is set.
- **INV-49** — Layer calls are sequential: `run_silver_accounts` returns before `run_silver_transactions` is called; `run_silver_transactions` returns before `run_gold` is called. No threading.
- **INV-49b** — Path-existence check in `run_silver_transactions` and `run_silver_accounts` occurs before `subprocess.run(['dbt', ...])`.

---

### Task 5.5 — Phase 8 Sign-Off Verification

**Description**
No new code. Runs the full pipeline from a clean `data/` directory, then runs it a second time to confirm idempotency. Records all Phase 8 sign-off DuckDB query results against the expected values from `VERIFICATION_CHECKLIST.md`. Signs off on all Brief §10 conditions.

**CC Prompt**
```
This is a verification-only task. Do not write any code.

1. Clean the data directory:
   find data/ -name '*.parquet' -delete
   find data/ -name 'pipeline.pid' -delete

2. Run:
   python pipeline.py --historical --start-date 2024-01-15 --end-date 2024-01-21

3. Run each of the following DuckDB queries. Record the result.
   Compare against VERIFICATION_CHECKLIST.md expected values.
   Record PASS or FAIL for each.

4. Run the pipeline a second time (no clean):
   python pipeline.py --historical --start-date 2024-01-15 --end-date 2024-01-21
   Verify all counts are identical to run 1 (idempotency).
```

**Verification Command (Phase 8 sign-off queries — exact DuckDB CLI commands)**

```bash
docker compose run --rm pipeline duckdb :memory: -c "

-- §10.1: Bronze row counts match source CSVs
SELECT 'bronze_txn' AS check_name,
  regexp_extract(filename,'date=([0-9-]+)',1) AS date,
  COUNT(*) AS bronze_rows
FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true)
GROUP BY 2 ORDER BY 2;

-- §10.2a: Silver + quarantine = Bronze for each date
SELECT
  b.date,
  b.bronze_rows,
  s.silver_rows,
  q.quarantine_rows,
  (b.bronze_rows = s.silver_rows + q.quarantine_rows) AS balanced
FROM (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS bronze_rows
  FROM read_parquet('data/bronze/transactions/**/*.parquet', filename=true)
  GROUP BY 1
) b
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS silver_rows
  FROM read_parquet('data/silver/transactions/**/*.parquet', filename=true)
  GROUP BY 1
) s ON b.date = s.date
JOIN (
  SELECT regexp_extract(filename,'date=([0-9-]+)',1) AS date, COUNT(*) AS quarantine_rows
  FROM read_parquet('data/silver/quarantine/**/*.parquet', filename=true)
  WHERE filename NOT LIKE '%rejected_accounts%'
  GROUP BY 1
) q ON b.date = q.date
ORDER BY 1;

-- §10.2b: No duplicate transaction_id across all Silver partitions
SELECT transaction_id, COUNT(*) AS cnt
FROM read_parquet('data/silver/transactions/**/*.parquet')
GROUP BY 1 HAVING COUNT(*) > 1;

-- §10.2c: Every Silver transaction has valid transaction_code in Silver TC
SELECT COUNT(*) AS orphan_codes
FROM read_parquet('data/silver/transactions/**/*.parquet') t
LEFT JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
  ON t.transaction_code = tc.transaction_code
WHERE tc.transaction_code IS NULL AND t._is_resolvable = true;

-- §10.2d: No null _signed_amount
SELECT COUNT(*) AS null_signed
FROM read_parquet('data/silver/transactions/**/*.parquet')
WHERE _signed_amount IS NULL;

-- §10.2e: Every quarantine record has a valid non-null rejection reason
SELECT
  COUNT(*) FILTER (WHERE _rejection_reason IS NULL) AS null_reason,
  COUNT(*) FILTER (WHERE _rejection_reason NOT IN (
    'NULL_REQUIRED_FIELD','INVALID_AMOUNT','DUPLICATE_TRANSACTION_ID',
    'INVALID_TRANSACTION_CODE','INVALID_CHANNEL',
    'INVALID_ACCOUNT_STATUS'
  )) AS invalid_reason
FROM read_parquet('data/silver/quarantine/**/*.parquet');

-- §10.3a: Gold daily_summary has exactly one row per resolvable date
SELECT
  (SELECT COUNT(DISTINCT transaction_date)
   FROM read_parquet('data/silver/transactions/**/*.parquet')
   WHERE _is_resolvable = true)               AS silver_resolvable_dates,
  (SELECT COUNT(*)
   FROM read_parquet('data/gold/daily_summary/data.parquet')) AS gold_daily_rows;

-- §10.3b: Gold weekly total_purchases matches Silver PURCHASE count
SELECT
  g.account_id, g.week_start_date,
  g.total_purchases AS gold_purchases,
  COUNT(t.transaction_id) AS silver_purchases,
  (g.total_purchases = COUNT(t.transaction_id)) AS match
FROM read_parquet('data/gold/weekly_account_summary/data.parquet') g
LEFT JOIN read_parquet('data/silver/transactions/**/*.parquet') t
  ON g.account_id = t.account_id
  AND t._is_resolvable = true
  AND t.transaction_type = 'PURCHASE'
  AND DATE_TRUNC('week', t.transaction_date)::DATE = g.week_start_date
GROUP BY g.account_id, g.week_start_date, g.total_purchases
HAVING NOT (g.total_purchases = COUNT(t.transaction_id));

-- §10.4: Idempotency — run second time, compare counts
-- (Run after second pipeline invocation)
SELECT 'bronze_txn' AS entity, COUNT(*) AS rows
FROM read_parquet('data/bronze/transactions/**/*.parquet')
UNION ALL
SELECT 'silver_txn', COUNT(*) FROM read_parquet('data/silver/transactions/**/*.parquet')
UNION ALL
SELECT 'quarantine', COUNT(*) FROM read_parquet('data/silver/quarantine/**/*.parquet')
UNION ALL
SELECT 'gold_daily', COUNT(*) FROM read_parquet('data/gold/daily_summary/data.parquet')
UNION ALL
SELECT 'gold_weekly', COUNT(*) FROM read_parquet('data/gold/weekly_account_summary/data.parquet')
ORDER BY 1;

-- §10.5a: Every Bronze/Silver/Gold record has non-null _pipeline_run_id
SELECT
  (SELECT COUNT(*) FROM read_parquet('data/bronze/transactions/**/*.parquet')
   WHERE _pipeline_run_id IS NULL) AS bronze_null_run_id,
  (SELECT COUNT(*) FROM read_parquet('data/silver/transactions/**/*.parquet')
   WHERE _pipeline_run_id IS NULL) AS silver_null_run_id,
  (SELECT COUNT(*) FROM read_parquet('data/gold/daily_summary/data.parquet')
   WHERE _pipeline_run_id IS NULL) AS gold_null_run_id;

-- §10.5b: Every Silver run_id has a SUCCESS row in the run log
SELECT s._pipeline_run_id
FROM (SELECT DISTINCT _pipeline_run_id
      FROM read_parquet('data/silver/transactions/**/*.parquet')) s
LEFT JOIN (SELECT DISTINCT run_id
           FROM read_parquet('data/pipeline/run_log.parquet')
           WHERE status = 'SUCCESS') l
  ON s._pipeline_run_id = l.run_id
WHERE l.run_id IS NULL;
"
```
**Expected for all sign-off conditions to PASS:**
- `balanced` = true for all 7 dates
- Duplicate `transaction_id` query returns 0 rows
- `orphan_codes` = 0
- `null_signed` = 0
- `null_reason` = 0; `invalid_reason` = 0
- `silver_resolvable_dates` = `gold_daily_rows`
- `total_purchases` mismatch query returns 0 rows
- Idempotency counts match `VERIFICATION_CHECKLIST.md` baseline
- All three `_null_run_id` columns = 0
- Orphaned Silver run_id query returns 0 rows

**Invariant Flag**
- **INV-48** — Idempotency query (§10.4): counts from second run must match `VERIFICATION_CHECKLIST.md` values recorded in Task 3.4. Any discrepancy is a regression.
- **INV-14, INV-15, INV-20, INV-26, INV-29, INV-31, INV-32, INV-40, INV-47** — All covered by the sign-off queries above. Each query maps to at least one invariant. Results must be recorded in `VERIFICATION_CHECKLIST.md` alongside pass/fail status.

---

## Appendix — Invariant Coverage Summary

All invariant IDs from INVARIANTS.md v2.0. Primary enforcement session listed.

| Invariant | Primary Task | Session |
|-----------|-------------|---------|
| INV-01 | 2.1 | S2 |
| INV-02 | 2.1, 2.2 | S2 |
| INV-03 | 2.1, 2.2, 2.3 | S2 |
| INV-04 | 1.1 | S1 |
| INV-05a | 2.1 | S2 |
| INV-05b | 1.1, 2.1 | S1, S2 |
| INV-05c | 2.1 | S2 |
| INV-06 | 2.1 | S2 |
| INV-07 | 2.1 (atomic guard) | S2 |
| INV-08 | 4.3 | S4 |
| INV-09 | 3.3 | S3 |
| INV-10 | 3.3 | S3 |
| INV-11a | 3.2 | S3 |
| INV-11b | 3.3 | S3 |
| INV-12a | 3.3 | S3 |
| INV-12b | 3.3 | S3 |
| INV-12c | 3.3 | S3 |
| INV-12d | 3.3 | S3 |
| INV-12e | 3.3 | S3 |
| INV-13 | 3.3 | S3 |
| INV-14 | 3.3, 5.5 | S3, S5 |
| INV-15 | 3.3, 5.5 | S3, S5 |
| INV-16a | 3.2 | S3 |
| INV-16b | 3.2 | S3 |
| INV-17 | 3.2 | S3 |
| INV-18 | 3.2, 3.3 | S3 |
| INV-18b | 3.2, 3.3 | S3 |
| INV-19 | 3.3 | S3 |
| INV-20 | 3.3, 5.5 | S3, S5 |
| INV-21 | 2.1, 3.3 | S2, S3 |
| INV-21b | 3.1 | S3 |
| INV-22 | 3.3 | S3 |
| INV-23 | 3.3 | S3 |
| INV-24 | 3.2 | S3 |
| INV-24b | 5.4 | S5 |
| INV-24c | 3.2 | S3 |
| INV-24d | 2.3 | S2 |
| INV-25 | 4.1, 4.2 | S4 |
| INV-26 | 4.1, 4.2, 5.5 | S4, S5 |
| INV-27 | 4.2 | S4 |
| INV-27b | 3.2 | S3 |
| INV-28 | 4.3 | S4 |
| INV-29 | 4.1, 5.5 | S4, S5 |
| INV-30 | 4.2 | S4 |
| INV-31 | 4.1, 5.5 | S4, S5 |
| INV-32 | 4.2, 5.5 | S4, S5 |
| INV-32b | 4.2 | S4 |
| INV-33 | 5.3, 5.4 | S5 |
| INV-33b | 5.3 | S5 |
| INV-34 | 5.4 | S5 |
| INV-35 | 5.4 | S5 |
| INV-36 | 5.4 | S5 |
| INV-36b | 5.3 | S5 |
| INV-37 | 5.2 | S5 |
| INV-37b | 5.2 | S5 |
| INV-38 | 3.1, 3.2, 3.3 | S3 |
| INV-39 | 5.2 | S5 |
| INV-39b | 5.2 | S5 |
| INV-40 | 2.2, 3.3, 4.1, 5.5 | S2, S3, S4, S5 |
| INV-40b | 5.2 | S5 |
| INV-41a | 5.1 | S5 |
| INV-41b | 5.1 | S5 |
| INV-41c | 5.1 | S5 |
| INV-41d | 5.1 | S5 |
| INV-41e | 5.1 | S5 |
| INV-42 | 3.3 (model header comments) | S3 |
| INV-43a | 1.3 | S1 |
| INV-43b | 1.3, 4.3 | S1, S4 |
| INV-44 | 2.1 | S2 |
| INV-44b | 2.1 | S2 |
| INV-45 | 3.1, 3.3 | S3 |
| INV-46 | 4.3 (derived from INV-28) | S4 |
| INV-47 | 4.3, 5.5 | S4, S5 |
| INV-48 | 3.4, 5.5 | S3, S5 |
| INV-49 | 5.4 | S5 |
| INV-49b | 3.3, 5.4 | S3, S5 |
| INV-50 | 3.3, 5.5 | S3, S5 |
| INV-51 | 4.1 | S4 |
| INV-52 | 3.1 | S3 |
